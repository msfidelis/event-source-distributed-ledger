package account

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"ledger/internal/models"
	"ledger/internal/utils"
	"ledger/pkg/config"
	"ledger/pkg/events"
	"ledger/pkg/kafka"
	"ledger/pkg/logger"
	"ledger/pkg/metrics"

	"github.com/google/uuid"
	"github.com/uptrace/bun"
)

// Listener processa eventos relacionados a contas
type Listener struct {
	db       *bun.DB
	config   *config.Config
	producer *kafka.Producer
}

// NewListener cria uma nova instância do AccountListener
func NewListener(db *bun.DB, cfg *config.Config) (*Listener, error) {
	producer, err := kafka.NewProducer(cfg.Kafka.Brokers)
	if err != nil {
		return nil, fmt.Errorf("erro ao criar produtor: %w", err)
	}

	return &Listener{
		db:       db,
		config:   cfg,
		producer: producer,
	}, nil
}

// StartConsuming inicia o consumidor Kafka para eventos de conta criada
func (l *Listener) StartConsuming(ctx context.Context) error {
	topic := l.config.Kafka.TopicContaCriada
	groupID := l.config.Kafka.GroupAccountListener
	log := logger.Instance()

	log.Info().
		Str("topic", topic).
		Str("group_id", groupID).
		Msg("Iniciando listener de conta criada")

	consumer := kafka.NewConsumer(l.config.Kafka.Brokers, topic, groupID)
	defer func() {
		log.Info().
			Str("topic", topic).
			Str("group_id", groupID).
			Msg("Fechando consumer do tópico")
		consumer.Close()
		l.producer.Close()
	}()

	return consumer.Consume(ctx, l.Handle)
}

// Handle processa o evento de conta criada.
//
// Fluxo transacional:
//  1. Deserializa envelope e payload (fora da tx — erros aqui são permanentes)
//  2. RunInTx:
//     a. Check de idempotência via processed_messages (ON CONFLICT DO NOTHING)
//     b. Cria registro na tabela accounts
//     c. Persiste evento no event store
//  3. Após commit: publica eventos Kafka (balance update + confirmação)
func (l *Listener) Handle(key, value []byte, correlationID string) error {
	startTime := time.Now()
	eventType := events.EventTypeContaCriada
	listenerName := "account"
	log := logger.Instance()

	log.Info().
		Str("correlation_id", correlationID).
		Msg("Processando evento de conta criada")

	var envelope events.EventEnvelope
	if err := json.Unmarshal(value, &envelope); err != nil {
		metrics.EventsFailedTotal.WithLabelValues(eventType, listenerName, "unmarshal_error").Inc()
		return fmt.Errorf("erro ao deserializar envelope: %w", err)
	}

	dataBytes, err := json.Marshal(envelope.Data)
	if err != nil {
		metrics.EventsFailedTotal.WithLabelValues(eventType, listenerName, "marshal_error").Inc()
		return fmt.Errorf("erro ao serializar data: %w", err)
	}

	var contaCriada events.ContaCriada
	if err := json.Unmarshal(dataBytes, &contaCriada); err != nil {
		metrics.EventsFailedTotal.WithLabelValues(eventType, listenerName, "unmarshal_data_error").Inc()
		return fmt.Errorf("erro ao deserializar ContaCriada: %w", err)
	}

	contaCriada.SaldoInicial = utils.RoundMoneyUp(contaCriada.SaldoInicial)

	ctx := context.Background()
	var eventID int64
	topic := l.config.Kafka.TopicContaCriada

	txErr := l.db.RunInTx(ctx, nil, func(ctx context.Context, tx bun.Tx) error {

		// Idempotência: ContaID é o identificador natural do evento de criação de conta.
		// Em reentrega Kafka, o INSERT retorna rowsAffected=0 e o handler retorna nil
		// sem reprocessar — evitando sobrescrever saldo e eventos já persistidos.
		result, err := tx.NewInsert().
			Model(&models.ProcessedMessage{
				EventID:     contaCriada.ContaID.String(),
				Topic:       topic,
				ProcessedAt: time.Now(),
			}).
			On("CONFLICT (event_id) DO NOTHING").
			Exec(ctx)
		if err != nil {
			return fmt.Errorf("erro ao verificar idempotência: %w", err)
		}
		rowsAffected, err := result.RowsAffected()
		if err != nil {
			return fmt.Errorf("erro ao ler rows affected: %w", err)
		}
		if rowsAffected == 0 {
			log.Info().
				Str("correlation_id", correlationID).
				Str("event_id", contaCriada.ContaID.String()).
				Str("topic", topic).
				Msg("Evento já processado — skip idempotente")
			return nil
		}

		if err := l.createAccountTx(ctx, tx, contaCriada, correlationID); err != nil {
			metrics.EventsFailedTotal.WithLabelValues(eventType, listenerName, "create_account_error").Inc()
			return fmt.Errorf("erro ao criar conta: %w", err)
		}

		eventID, err = l.saveEventTx(ctx, tx, contaCriada.ContaID, "Account",
			events.EventTypeContaCriada, dataBytes, envelope.Metadata, correlationID)
		if err != nil {
			metrics.EventsFailedTotal.WithLabelValues(eventType, listenerName, "save_event_error").Inc()
			return fmt.Errorf("erro ao salvar evento: %w", err)
		}

		log.Info().
			Str("correlation_id", correlationID).
			Int64("event_id", eventID).
			Str("account_id", contaCriada.ContaID.String()).
			Str("nome_proprietario", contaCriada.NomeProprietario).
			Float64("saldo_inicial", contaCriada.SaldoInicial).
			Msg("Evento persistido: ContaCriada")

		return nil
	})

	if txErr != nil {
		return txErr
	}

	// Publishes Kafka APÓS commit da transação — evita dual-write inconsistente.
	if err := l.publishBalanceUpdate(contaCriada.ContaID, contaCriada.SaldoInicial, 1, correlationID); err != nil {
		log.Error().
			Str("correlation_id", correlationID).
			Err(err).
			Msg("Erro ao publicar saldo atualizado")
	}

	if err := l.publishConfirmation(contaCriada, eventID, correlationID); err != nil {
		log.Error().
			Str("correlation_id", correlationID).
			Err(err).
			Msg("Erro ao publicar confirmação")
	}

	metrics.EventsProcessedTotal.WithLabelValues(eventType, listenerName).Inc()
	metrics.EventsProcessingDuration.WithLabelValues(eventType, listenerName).Observe(time.Since(startTime).Seconds())
	metrics.AccountsCreatedTotal.Inc()

	log.Info().
		Str("correlation_id", correlationID).
		Msg("Processamento concluído com sucesso")

	return nil
}

// createAccountTx persiste o registro na tabela accounts dentro da transação fornecida.
// Não realiza nenhum publish Kafka — isso é responsabilidade do chamador após o commit.
func (l *Listener) createAccountTx(ctx context.Context, tx bun.Tx, conta events.ContaCriada, correlationID string) error {
	log := logger.Instance()

	account := &models.Account{
		AggregateID: conta.ContaID,
		OwnerName:   conta.NomeProprietario,
		Balance:     utils.RoundMoneyUp(conta.SaldoInicial),
		Status:      "active",
		CreatedAt:   conta.CriadoEm,
		UpdatedAt:   time.Now(),
	}

	_, err := tx.NewInsert().
		Model(account).
		On("CONFLICT (aggregate_id) DO UPDATE").
		Set("owner_name = EXCLUDED.owner_name").
		Set("balance = EXCLUDED.balance").
		Set("updated_at = NOW()").
		Exec(ctx)
	if err != nil {
		return fmt.Errorf("erro ao inserir conta: %w", err)
	}

	log.Info().
		Str("correlation_id", correlationID).
		Str("conta_id", conta.ContaID.String()).
		Str("nome_proprietario", conta.NomeProprietario).
		Float64("saldo_inicial", conta.SaldoInicial).
		Msg("Conta criada na tabela accounts")

	return nil
}

// saveEventTx persiste um evento no event store dentro da transação fornecida.
func (l *Listener) saveEventTx(ctx context.Context, tx bun.Tx, aggregateID uuid.UUID, aggregateType, eventType string, eventData []byte, metadata map[string]string, correlationID string) (int64, error) {
	startTime := time.Now()
	log := logger.Instance()

	if metadata == nil {
		metadata = make(map[string]string)
	}
	metadata["correlationID"] = correlationID

	var version int
	err := tx.NewSelect().
		ColumnExpr("COALESCE(MAX(version), 0)").
		TableExpr("events").
		Where("aggregate_id = ?", aggregateID).
		Scan(ctx, &version)
	if err != nil {
		metrics.EventsAppendedTotal.WithLabelValues(aggregateType, eventType, "error").Inc()
		return 0, fmt.Errorf("erro ao obter versão: %w", err)
	}

	metadataJSON, err := json.Marshal(metadata)
	if err != nil {
		metrics.EventsAppendedTotal.WithLabelValues(aggregateType, eventType, "error").Inc()
		return 0, fmt.Errorf("erro ao serializar metadata: %w", err)
	}

	event := &models.Event{
		AggregateID:   aggregateID,
		AggregateType: aggregateType,
		EventType:     eventType,
		EventData:     eventData,
		Metadata:      metadataJSON,
		Version:       version + 1,
		OccurredAt:    time.Now(),
	}

	_, err = tx.NewInsert().
		Model(event).
		Returning("id").
		Exec(ctx)
	if err != nil {
		if isVersionConflict(err) {
			log.Warn().
				Str("correlation_id", correlationID).
				Str("aggregate_id", aggregateID.String()).
				Msg("Version conflict")
			metrics.EventsVersionConflictsTotal.WithLabelValues(aggregateType).Inc()
			metrics.EventsAppendedTotal.WithLabelValues(aggregateType, eventType, "version_conflict").Inc()
		} else {
			metrics.EventsAppendedTotal.WithLabelValues(aggregateType, eventType, "error").Inc()
		}
		return 0, err
	}

	metrics.EventsAppendedTotal.WithLabelValues(aggregateType, eventType, "success").Inc()
	metrics.EventsAppendDuration.WithLabelValues(aggregateType, eventType).Observe(time.Since(startTime).Seconds())

	return event.ID, nil
}

// isVersionConflict verifica se o erro é um conflito de versão
func isVersionConflict(err error) bool {
	return err != nil && (err.Error() == "duplicate key value violates unique constraint" ||
		err.Error() == "unique_violation")
}

func (l *Listener) publishConfirmation(conta events.ContaCriada, eventID int64, correlationID string) error {
	confirmation := map[string]interface{}{
		"event_id":          eventID,
		"conta_id":          conta.ContaID,
		"nome_proprietario": conta.NomeProprietario,
		"saldo_inicial":     conta.SaldoInicial,
		"balance":           conta.SaldoInicial,
		"moeda":             conta.Moeda,
		"criado_em":         conta.CriadoEm,
		"confirmed_at":      conta.CriadoEm,
	}

	confirmationJSON, err := json.Marshal(confirmation)
	if err != nil {
		return fmt.Errorf("erro ao serializar confirmação: %w", err)
	}

	topic := l.config.Kafka.TopicNovaContaRegistrada
	headers := map[string]string{"correlationID": correlationID}

	if err := l.producer.PublishWithHeaders(topic, []byte(conta.ContaID.String()), confirmationJSON, headers); err != nil {
		return fmt.Errorf("erro ao publicar no tópico %s: %w", topic, err)
	}

	log := logger.Instance()
	log.Info().
		Str("correlation_id", correlationID).
		Str("conta_id", conta.ContaID.String()).
		Int64("event_id", eventID).
		Str("topic", topic).
		Msg("Confirmação publicada no tópico")

	return nil
}

func (l *Listener) publishBalanceUpdate(contaID uuid.UUID, balance float64, version int, correlationID string) error {
	log := logger.Instance()

	balanceUpdate := map[string]interface{}{
		"conta_id":  contaID,
		"balance":   balance,
		"version":   version,
		"timestamp": time.Now(),
	}

	balanceJSON, err := json.Marshal(balanceUpdate)
	if err != nil {
		return fmt.Errorf("erro ao serializar saldo atualizado: %w", err)
	}

	topic := l.config.Kafka.TopicSaldoAtualizado
	headers := map[string]string{"correlationID": correlationID}

	if err := l.producer.PublishWithHeaders(topic, []byte(contaID.String()), balanceJSON, headers); err != nil {
		return fmt.Errorf("erro ao publicar no tópico %s: %w", topic, err)
	}

	log.Info().
		Str("correlation_id", correlationID).
		Str("conta_id", contaID.String()).
		Float64("balance", balance).
		Int("version", version).
		Str("topic", topic).
		Msg("Saldo atualizado publicado no tópico")

	return nil
}
