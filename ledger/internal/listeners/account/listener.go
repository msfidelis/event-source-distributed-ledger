package account

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"ledger/internal/models"
	"ledger/pkg/config"
	"ledger/pkg/events"
	"ledger/pkg/kafka"
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

	log.Printf("[Account] Iniciando listener para tópico: %s (group: %s)", topic, groupID)

	consumer := kafka.NewConsumer(l.config.Kafka.Brokers, topic, groupID)
	defer func() {
		log.Printf("[Account] Fechando consumer do tópico: %s", topic)
		consumer.Close()
		l.producer.Close()
	}()

	return consumer.Consume(ctx, l.Handle)
}

// Handle processa o evento de conta criada
func (l *Listener) Handle(key, value []byte) error {
	startTime := time.Now()
	eventType := events.EventTypeContaCriada
	listener := "account"

	var envelope events.EventEnvelope
	if err := json.Unmarshal(value, &envelope); err != nil {
		metrics.EventsFailedTotal.WithLabelValues(eventType, listener, "unmarshal_error").Inc()
		return fmt.Errorf("erro ao deserializar envelope: %w", err)
	}

	// Converte Data para ContaCriada
	dataBytes, err := json.Marshal(envelope.Data)
	if err != nil {
		metrics.EventsFailedTotal.WithLabelValues(eventType, listener, "marshal_error").Inc()
		return fmt.Errorf("erro ao serializar data: %w", err)
	}

	var contaCriada events.ContaCriada
	if err := json.Unmarshal(dataBytes, &contaCriada); err != nil {
		metrics.EventsFailedTotal.WithLabelValues(eventType, listener, "unmarshal_data_error").Inc()
		return fmt.Errorf("erro ao deserializar ContaCriada: %w", err)
	}

	// PRIMEIRO: Cria registro na tabela accounts (para satisfazer FK da tabela events)
	if err := l.createAccount(contaCriada); err != nil {
		log.Printf("[Account] Erro ao criar conta: %v", err)
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
	)
	if err != nil {
		metrics.EventsFailedTotal.WithLabelValues(eventType, listener, "save_event_error").Inc()
		return fmt.Errorf("erro ao salvar evento: %w", err)
	}

	log.Printf("[Account] Evento persistido: ContaCriada - ID=%d, Conta=%s (%s) - R$ %.2f",
		eventID, contaCriada.ContaID, contaCriada.NomeProprietario, contaCriada.SaldoInicial)

	// TERCEIRO: Publica confirmação no tópico de confirmações
	if err := l.publishConfirmation(contaCriada, eventID); err != nil {
		log.Printf("[Account] Erro ao publicar confirmação: %v", err)
		// Não retorna erro para não bloquear o consumer
	}

	// Métricas de sucesso
	metrics.EventsProcessedTotal.WithLabelValues(eventType, listener).Inc()
	metrics.EventsProcessingDuration.WithLabelValues(eventType, listener).Observe(time.Since(startTime).Seconds())
	metrics.AccountsCreatedTotal.Inc()

	return nil
}

func (l *Listener) saveEvent(aggregateID uuid.UUID, aggregateType, eventType string, eventData []byte, metadata map[string]string) (int64, error) {
	startTime := time.Now()
	ctx := context.Background()

	// Obtém a versão atual do agregado
	var version int
	err := l.db.NewSelect().
		ColumnExpr("COALESCE(MAX(version), 0)").
		TableExpr("events").
		Where("aggregate_id = ?", aggregateID).
		Scan(ctx, &version)
	if err != nil {
		metrics.EventsAppendedTotal.WithLabelValues(aggregateType, eventType, "error").Inc()
		return 0, fmt.Errorf("erro ao obter versão: %w", err)
	}

	// Converte metadata para JSONB
	metadataJSON, err := json.Marshal(metadata)
	if err != nil {
		metrics.EventsAppendedTotal.WithLabelValues(aggregateType, eventType, "error").Inc()
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
			metrics.EventsVersionConflictsTotal.WithLabelValues(aggregateType).Inc()
			metrics.EventsAppendedTotal.WithLabelValues(aggregateType, eventType, "version_conflict").Inc()
		} else {
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

func (l *Listener) createAccount(conta events.ContaCriada) error {
	ctx := context.Background()

	account := &models.Account{
		AggregateID: conta.ContaID,
		OwnerName:   conta.NomeProprietario,
		Balance:     conta.SaldoInicial,
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
		return fmt.Errorf("erro ao inserir conta: %w", err)
	}

	log.Printf("[Account] Conta criada na tabela accounts: %s - %s - Saldo: R$ %.2f",
		conta.ContaID, conta.NomeProprietario, conta.SaldoInicial)

	// Publica evento de saldo atualizado
	if err := l.publishBalanceUpdate(conta.ContaID, conta.SaldoInicial, 1); err != nil {
		log.Printf("[Account] Erro ao publicar saldo atualizado: %v", err)
	}

	return nil
}

func (l *Listener) publishConfirmation(conta events.ContaCriada, eventID int64) error {
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
		return fmt.Errorf("erro ao serializar confirmação: %w", err)
	}

	topic := l.config.Kafka.TopicNovaContaRegistrada
	// Publica no tópico de confirmações
	if err := l.producer.Publish(topic, []byte(conta.ContaID.String()), confirmationJSON); err != nil {
		return fmt.Errorf("erro ao publicar no tópico %s: %w", topic, err)
	}

	log.Printf("[Account] Confirmação publicada no tópico %s: conta_id=%s, event_id=%d",
		topic, conta.ContaID, eventID)
	return nil
}

func (l *Listener) publishBalanceUpdate(contaID uuid.UUID, balance float64, version int) error {
	// Cria mensagem de saldo atualizado
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
	// Publica no tópico de saldo atualizado
	if err := l.producer.Publish(topic, []byte(contaID.String()), balanceJSON); err != nil {
		return fmt.Errorf("erro ao publicar no tópico %s: %w", topic, err)
	}

	log.Printf("[Account] Saldo atualizado publicado: conta_id=%s, balance=%.2f, version=%d",
		contaID, balance, version)

	return nil
}
