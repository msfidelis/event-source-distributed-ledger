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
	// Cria o produtor para publicar confirmações
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
	logger := logger.Instance()

	logger.Info().
		Str("topic", topic).
		Str("group_id", groupID).
		Msg("Iniciando listener de conta criada")

	consumer := kafka.NewConsumer(l.config.Kafka.Brokers, topic, groupID)
	defer func() {
		logger.Info().
			Str("topic", topic).
			Str("group_id", groupID).
			Msg("Fechando consumer do tópico")
		consumer.Close()
		l.producer.Close()
	}()

	return consumer.Consume(ctx, l.Handle)
}

// Handle processa o evento de conta criada
func (l *Listener) Handle(key, value []byte, correlationID string) error {
	startTime := time.Now()
	eventType := events.EventTypeContaCriada
	listener := "account"
	logger := logger.Instance()

	logger.Info().
		Str("correlation_id", correlationID).
		Msg("Processando evento de conta criada")

	var envelope events.EventEnvelope
	if err := json.Unmarshal(value, &envelope); err != nil {
		metrics.EventsFailedTotal.WithLabelValues(eventType, listener, "unmarshal_error").Inc()
		logger.Error().
			Str("correlation_id", correlationID).
			Err(err).
			Msg("Erro ao deserializar envelope")
		return fmt.Errorf("erro ao deserializar envelope: %w", err)
	}

	// Converte Data para ContaCriada
	dataBytes, err := json.Marshal(envelope.Data)
	if err != nil {
		metrics.EventsFailedTotal.WithLabelValues(eventType, listener, "marshal_error").Inc()
		logger.Error().
			Str("correlation_id", correlationID).
			Err(err).
			Msg("Erro ao serializar data")
		return fmt.Errorf("erro ao serializar data: %w", err)
	}

	var contaCriada events.ContaCriada
	if err := json.Unmarshal(dataBytes, &contaCriada); err != nil {
		metrics.EventsFailedTotal.WithLabelValues(eventType, listener, "unmarshal_data_error").Inc()
		logger.Error().
			Str("correlation_id", correlationID).
			Err(err).
			Msg("Erro ao deserializar ContaCriada")
		return fmt.Errorf("erro ao deserializar ContaCriada: %w", err)
	}

	// Arredonda o saldo inicial para 2 casas decimais
	contaCriada.SaldoInicial = utils.RoundMoneyUp(contaCriada.SaldoInicial)

	// PRIMEIRO: Cria registro na tabela accounts (para satisfazer FK da tabela events)
	if err := l.createAccount(contaCriada, correlationID); err != nil {
		logger.Error().
			Str("correlation_id", correlationID).
			Err(err).
			Msg("Erro ao criar conta")
		metrics.EventsFailedTotal.WithLabelValues(eventType, listener, "create_account_error").Inc()
		return fmt.Errorf("erro ao criar conta: %w", err)
	}

	// SEGUNDO: Persiste no event store (agora a FK para accounts vai funcionar)
	eventID, err := l.saveEvent(
		contaCriada.ContaID,
		"Account",
		events.EventTypeContaCriada,
		dataBytes,
		envelope.Metadata,
		correlationID,
	)
	if err != nil {
		metrics.EventsFailedTotal.WithLabelValues(eventType, listener, "save_event_error").Inc()
		logger.Error().
			Str("correlation_id", correlationID).
			Err(err).
			Msg("Erro ao salvar evento")
		return fmt.Errorf("erro ao salvar evento: %w", err)
	}

	logger.Info().
		Str("correlation_id", correlationID).
		Int64("event_id", eventID).
		Str("account_id", contaCriada.ContaID.String()).
		Str("nome_proprietario", contaCriada.NomeProprietario).
		Float64("saldo_inicial", contaCriada.SaldoInicial).
		Msg("Evento persistido: ContaCriada")

	// TERCEIRO: Publica confirmação no tópico de confirmações
	if err := l.publishConfirmation(contaCriada, eventID, correlationID); err != nil {
		logger.Error().
			Str("correlation_id", correlationID).
			Err(err).
			Msg("Erro ao publicar confirmação")
		// Não retorna erro para não bloquear o consumer
	}

	// Métricas de sucesso
	metrics.EventsProcessedTotal.WithLabelValues(eventType, listener).Inc()
	metrics.EventsProcessingDuration.WithLabelValues(eventType, listener).Observe(time.Since(startTime).Seconds())
	metrics.AccountsCreatedTotal.Inc()

	logger.Info().
		Str("correlation_id", correlationID).
		Msg("Processamento concluído com sucesso")

	return nil
}

func (l *Listener) saveEvent(aggregateID uuid.UUID, aggregateType, eventType string, eventData []byte, metadata map[string]string, correlationID string) (int64, error) {
	startTime := time.Now()
	ctx := context.Background()
	logger := logger.Instance()

	// Adiciona correlationID ao metadata
	if metadata == nil {
		metadata = make(map[string]string)
	}
	metadata["correlationID"] = correlationID

	// Obtém a versão atual do agregado
	var version int
	err := l.db.NewSelect().
		ColumnExpr("COALESCE(MAX(version), 0)").
		TableExpr("events").
		Where("aggregate_id = ?", aggregateID).
		Scan(ctx, &version)
	if err != nil {
		metrics.EventsAppendedTotal.WithLabelValues(aggregateType, eventType, "error").Inc()
		logger.Error().
			Str("correlation_id", correlationID).
			Err(err).
			Msg("Erro ao obter versão")
		return 0, fmt.Errorf("erro ao obter versão: %w", err)
	}

	// Converte metadata para JSONB
	metadataJSON, err := json.Marshal(metadata)
	if err != nil {
		metrics.EventsAppendedTotal.WithLabelValues(aggregateType, eventType, "error").Inc()
		logger.Error().
			Str("correlation_id", correlationID).
			Err(err).
			Msg("Erro ao serializar metadata")
		return 0, fmt.Errorf("erro ao serializar metadata: %w", err)
	}

	// Insere o evento
	event := &models.Event{
		AggregateID:   aggregateID,
		AggregateType: aggregateType,
		EventType:     eventType,
		EventData:     eventData,
		Metadata:      metadataJSON,
		Version:       version + 1,
		OccurredAt:    time.Now(),
	}

	_, err = l.db.NewInsert().
		Model(event).
		Returning("id").
		Exec(ctx)

	if err != nil {
		// Detecta conflitos de versão (optimistic locking)
		if isVersionConflict(err) {
			logger.Warn().
				Str("correlation_id", correlationID).
				Str("aggregate_id", aggregateID.String()).
				Msg("Version conflict")
			metrics.EventsVersionConflictsTotal.WithLabelValues(aggregateType).Inc()
			metrics.EventsAppendedTotal.WithLabelValues(aggregateType, eventType, "version_conflict").Inc()
		} else {
			logger.Error().
				Str("correlation_id", correlationID).
				Err(err).
				Msg("Erro ao inserir evento")
			metrics.EventsAppendedTotal.WithLabelValues(aggregateType, eventType, "error").Inc()
		}
		return 0, err
	}

	// Métricas de sucesso
	metrics.EventsAppendedTotal.WithLabelValues(aggregateType, eventType, "success").Inc()
	metrics.EventsAppendDuration.WithLabelValues(aggregateType, eventType).Observe(time.Since(startTime).Seconds())

	return event.ID, err
}

// isVersionConflict verifica se o erro é um conflito de versão
func isVersionConflict(err error) bool {
	// PostgreSQL unique violation error code é 23505
	return err != nil && (err.Error() == "duplicate key value violates unique constraint" ||
		err.Error() == "unique_violation")
}

func (l *Listener) createAccount(conta events.ContaCriada, correlationID string) error {
	ctx := context.Background()
	logger := logger.Instance()

	// Arredonda o saldo inicial para 2 casas decimais
	saldoInicial := utils.RoundMoneyUp(conta.SaldoInicial)

	account := &models.Account{
		AggregateID: conta.ContaID,
		OwnerName:   conta.NomeProprietario,
		Balance:     saldoInicial,
		Status:      "active",
		CreatedAt:   conta.CriadoEm,
		UpdatedAt:   time.Now(),
	}

	_, err := l.db.NewInsert().
		Model(account).
		On("CONFLICT (aggregate_id) DO UPDATE").
		Set("owner_name = EXCLUDED.owner_name").
		Set("balance = EXCLUDED.balance").
		Set("updated_at = NOW()").
		Exec(ctx)

	if err != nil {
		logger.Error().
			Str("correlation_id", correlationID).
			Err(err).
			Msg("Erro ao inserir conta")
		return fmt.Errorf("erro ao inserir conta: %w", err)
	}

	logger.Info().
		Str("correlation_id", correlationID).
		Str("conta_id", conta.ContaID.String()).
		Str("nome_proprietario", conta.NomeProprietario).
		Float64("saldo_inicial", saldoInicial).
		Msg("Conta criada na tabela accounts")

	// Publica evento de saldo atualizado
	if err := l.publishBalanceUpdate(conta.ContaID, saldoInicial, 1, correlationID); err != nil {
		logger.Error().
			Str("correlation_id", correlationID).
			Err(err).
			Msg("Erro ao publicar saldo atualizado")
	}

	return nil
}

func (l *Listener) publishConfirmation(conta events.ContaCriada, eventID int64, correlationID string) error {
	// Cria mensagem de confirmação
	confirmation := map[string]interface{}{
		"event_id":          eventID,
		"conta_id":          conta.ContaID,
		"nome_proprietario": conta.NomeProprietario,
		"saldo_inicial":     conta.SaldoInicial,
		"balance":           conta.SaldoInicial,
		"moeda":             conta.Moeda,
		"criado_em":         conta.CriadoEm,
		"confirmed_at":      conta.CriadoEm, // timestamp da confirmação
	}

	confirmationJSON, err := json.Marshal(confirmation)
	if err != nil {
		logger := logger.Instance()
		logger.Error().
			Str("correlation_id", correlationID).
			Err(err).
			Msg("Erro ao serializar confirmação")
		return fmt.Errorf("erro ao serializar confirmação: %w", err)
	}

	topic := l.config.Kafka.TopicNovaContaRegistrada
	headers := map[string]string{
		"correlationID": correlationID,
	}
	// Publica no tópico de confirmações com headers
	if err := l.producer.PublishWithHeaders(topic, []byte(conta.ContaID.String()), confirmationJSON, headers); err != nil {
		logger := logger.Instance()
		logger.Error().
			Str("correlation_id", correlationID).
			Err(err).
			Msgf("Erro ao publicar no tópico %s", topic)
		return fmt.Errorf("erro ao publicar no tópico %s: %w", topic, err)
	}

	logger := logger.Instance()
	logger.Info().
		Str("correlation_id", correlationID).
		Str("conta_id", conta.ContaID.String()).
		Int64("event_id", eventID).
		Str("topic", topic).
		Msg("Confirmação publicada no tópico")
	return nil
}

func (l *Listener) publishBalanceUpdate(contaID uuid.UUID, balance float64, version int, correlationID string) error {

	logger := logger.Instance()

	// Cria mensagem de saldo atualizado
	balanceUpdate := map[string]interface{}{
		"conta_id":  contaID,
		"balance":   balance,
		"version":   version,
		"timestamp": time.Now(),
	}

	balanceJSON, err := json.Marshal(balanceUpdate)
	if err != nil {
		logger.Error().
			Str("correlation_id", correlationID).
			Err(err).
			Msg("Erro ao serializar saldo atualizado")
		return fmt.Errorf("erro ao serializar saldo atualizado: %w", err)
	}

	topic := l.config.Kafka.TopicSaldoAtualizado
	headers := map[string]string{
		"correlationID": correlationID,
	}
	// Publica no tópico de saldo atualizado com headers
	if err := l.producer.PublishWithHeaders(topic, []byte(contaID.String()), balanceJSON, headers); err != nil {
		logger.Error().
			Str("correlation_id", correlationID).
			Err(err).
			Msgf("Erro ao publicar no tópico %s", topic)
		return fmt.Errorf("erro ao publicar no tópico %s: %w", topic, err)
	}

	logger.Info().
		Str("correlation_id", correlationID).
		Str("conta_id", contaID.String()).
		Float64("balance", balance).
		Int("version", version).
		Str("topic", topic).
		Msg("Saldo atualizado publicado no tópico")

	return nil
}
