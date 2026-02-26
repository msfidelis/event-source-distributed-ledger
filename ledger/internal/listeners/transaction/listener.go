package transaction

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"ledger/internal/models"
	"ledger/pkg/config"
	"ledger/pkg/envoyratelimit"
	"ledger/pkg/events"
	"ledger/pkg/kafka"
	"ledger/pkg/metrics"

	"github.com/google/uuid"
	"github.com/uptrace/bun"
)

// Listener processa eventos relacionados a transações
type Listener struct {
	db          *bun.DB
	config      *config.Config
	producer    *kafka.Producer
	rateLimiter *envoyratelimit.Client
	topic       string
}

// NewListener cria uma nova instância do TransactionListener
func NewListener(db *bun.DB, topic string, cfg *config.Config) (*Listener, error) {
	// Cria o produtor para publicar confirmações
	producer, err := kafka.NewProducer(cfg.Kafka.Brokers)
	if err != nil {
		return nil, fmt.Errorf("erro ao criar produtor: %w", err)
	}

	// Inicializa o cliente Rate Limiter (singleton)
	rateLimiter, err := envoyratelimit.GetInstance(cfg.RateLimit.Host)
	if err != nil {
		log.Printf("[Transaction] Aviso: Rate Limiter não disponível: %v", err)
		// Não retorna erro, permite continuar sem rate limiting
	}

	return &Listener{
		db:          db,
		config:      cfg,
		producer:    producer,
		rateLimiter: rateLimiter,
		topic:       topic,
	}, nil
}

// StartConsuming inicia o consumidor Kafka para eventos de movimentação
func (l *Listener) StartConsuming(ctx context.Context) error {
	topic := l.topic
	groupID := l.config.Kafka.GroupTransactionListener

	log.Printf("[Transaction] Iniciando listener para tópico: %s (group: %s)", topic, groupID)

	consumer := kafka.NewConsumer(l.config.Kafka.Brokers, topic, groupID)
	defer func() {
		log.Printf("[Transaction] Fechando consumer do tópico: %s", topic)
		consumer.Close()
		l.producer.Close()
	}()

	return consumer.Consume(ctx, l.Handle)
}

// Handle processa o evento de movimentação de conta
func (l *Listener) Handle(key, value []byte, correlationID string) error {
	startTime := time.Now()
	eventType := events.EventTypeContaMovimentacao
	listener := "transaction"

	log.Printf("[Transaction] [CorrelationID: %s] Processando evento de movimentação", correlationID)

	var envelope events.EventEnvelope
	if err := json.Unmarshal(value, &envelope); err != nil {
		metrics.EventsFailedTotal.WithLabelValues(eventType, listener, "unmarshal_error").Inc()
		log.Printf("[Transaction] [CorrelationID: %s] Erro ao deserializar envelope: %v", correlationID, err)
		return fmt.Errorf("erro ao deserializar envelope: %w", err)
	}

	// Converte Data para ContaMovimentacao
	dataBytes, err := json.Marshal(envelope.Data)
	if err != nil {
		metrics.EventsFailedTotal.WithLabelValues(eventType, listener, "marshal_error").Inc()
		log.Printf("[Transaction] [CorrelationID: %s] Erro ao serializar data: %v", correlationID, err)
		return fmt.Errorf("erro ao serializar data: %w", err)
	}

	var movimentacao events.ContaMovimentacao
	if err := json.Unmarshal(dataBytes, &movimentacao); err != nil {
		metrics.EventsFailedTotal.WithLabelValues(eventType, listener, "unmarshal_data_error").Inc()
		log.Printf("[Transaction] [CorrelationID: %s] Erro ao deserializar ContaMovimentacao: %v", correlationID, err)
		return fmt.Errorf("erro ao deserializar ContaMovimentacao: %w", err)
	}

	// Verifica rate limit se o cliente estiver disponível
	if l.rateLimiter != nil {
		ctxRL := context.Background()
		allowed, err := l.rateLimiter.ShouldRateLimit(ctxRL, "ledger-transactions", "account", movimentacao.ContaID.String())
		if err != nil {
			log.Printf("[Transaction] [CorrelationID: %s] Erro ao verificar rate limit: %v", correlationID, err)
			// Continua processamento em caso de erro no rate limiter
		} else if !allowed {
			metrics.EventsFailedTotal.WithLabelValues(eventType, listener, "rate_limited").Inc()

			// Publica evento no tópico de rate limited
			if err := l.publishRateLimitedEvent(movimentacao, key, value, correlationID); err != nil {
				log.Printf("[Transaction] [CorrelationID: %s] Erro ao publicar evento rate limited: %v", correlationID, err)
			}

			return fmt.Errorf("requisição bloqueada por rate limit para conta: %s", movimentacao.ContaID)
		}
	}

	ctx := context.Background()
	var eventID int64
	var balanceAfter float64

	// Executa as operações de event sourcing e balance dentro de uma transaction
	txErr := l.db.RunInTx(ctx, nil, func(ctx context.Context, tx bun.Tx) error {
		// Persiste no event store
		var err error
		eventID, err = l.saveEventTx(ctx, tx, movimentacao.ContaID, "Account",
			events.EventTypeContaMovimentacao, dataBytes, envelope.Metadata, correlationID)
		if err != nil {
			tx.Rollback()
			metrics.EventsFailedTotal.WithLabelValues(eventType, listener, "save_event_error").Inc()
			log.Printf("[Transaction] [CorrelationID: %s] Erro ao salvar evento: %v", correlationID, err)
			return fmt.Errorf("erro ao salvar evento: %w", err)
		}

		log.Printf("[Transaction] [CorrelationID: %s] Evento persistido: ContaMovimentacao - ID=%d, Conta=%s (%s) - R$ %.2f",
			correlationID, eventID, movimentacao.ContaID, movimentacao.Tipo, movimentacao.Valor)

		// Atualiza saldo da conta e registra transação
		balanceAfter, err = l.processTransactionTx(ctx, tx, movimentacao, correlationID)
		if err != nil {
			log.Printf("[Transaction] [CorrelationID: %s] Erro ao processar transação: %v", correlationID, err)
			tx.Rollback()
			metrics.TransactionsRollbackTotal.WithLabelValues("processing_error").Inc()
			metrics.EventsFailedTotal.WithLabelValues(eventType, listener, "process_transaction_error").Inc()
			return fmt.Errorf("erro ao processar transação: %w", err)
		}
		// Commit
		// tx.Commit()
		return nil
	})

	if txErr != nil {
		return txErr
	}

	// Publica confirmação após commit da transação
	if err := l.publishConfirmation(movimentacao, eventID, balanceAfter, correlationID); err != nil {
		log.Printf("[Transaction] [CorrelationID: %s] Erro ao publicar confirmação: %v", correlationID, err)
	}

	// Métricas de sucesso
	metrics.EventsProcessedTotal.WithLabelValues(eventType, listener).Inc()
	metrics.EventsProcessingDuration.WithLabelValues(eventType, listener).Observe(time.Since(startTime).Seconds())
	metrics.TransactionsProcessedTotal.WithLabelValues(string(movimentacao.Tipo)).Inc()
	metrics.TransactionsPerAccount.WithLabelValues(movimentacao.ContaID.String()).Inc()

	log.Printf("[Transaction] [CorrelationID: %s] Processamento concluído com sucesso", correlationID)

	return nil
}

func (l *Listener) saveEventTx(ctx context.Context, tx bun.Tx, aggregateID uuid.UUID, aggregateType, eventType string, eventData []byte, metadata map[string]string, correlationID string) (int64, error) {
	startTime := time.Now()

	// Adiciona correlationID ao metadata
	if metadata == nil {
		metadata = make(map[string]string)
	}
	metadata["correlationID"] = correlationID

	// Obtém a versão atual do agregado
	var version int
	err := tx.NewSelect().
		ColumnExpr("COALESCE(MAX(version), 0)").
		TableExpr("events").
		Where("aggregate_id = ?", aggregateID).
		Scan(ctx, &version)
	if err != nil {
		metrics.EventsAppendedTotal.WithLabelValues(aggregateType, eventType, "error").Inc()
		log.Printf("[Transaction] [CorrelationID: %s] Erro ao obter versão: %v", correlationID, err)
		return 0, fmt.Errorf("erro ao obter versão: %w", err)
	}

	// Converte metadata para JSONB
	metadataJSON, err := json.Marshal(metadata)
	if err != nil {
		metrics.EventsAppendedTotal.WithLabelValues(aggregateType, eventType, "error").Inc()
		log.Printf("[Transaction] [CorrelationID: %s] Erro ao serializar metadata: %v", correlationID, err)
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

	_, err = tx.NewInsert().
		Model(event).
		Returning("id").
		Exec(ctx)

	if err != nil {
		// Detecta conflitos de versão (optimistic locking)
		if isVersionConflict(err) {
			log.Printf("[Transaction] [CorrelationID: %s] Version conflict: %v", correlationID, aggregateID)
			metrics.EventsVersionConflictsTotal.WithLabelValues(aggregateType).Inc()
			metrics.EventsAppendedTotal.WithLabelValues(aggregateType, eventType, "version_conflict").Inc()
		} else {
			log.Printf("[Transaction] [CorrelationID: %s] Erro ao inserir evento: %v", correlationID, err)
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

func (l *Listener) processTransactionTx(ctx context.Context, tx bun.Tx, mov events.ContaMovimentacao, correlationID string) (float64, error) {
	// Obtém saldo atual da conta
	var account models.Account
	err := tx.NewSelect().
		Model(&account).
		Where("aggregate_id = ?", mov.ContaID).
		Scan(ctx)
	if err != nil {
		log.Printf("[Transaction] [CorrelationID: %s] Erro ao obter saldo: %v", correlationID, err)
		return 0, fmt.Errorf("erro ao obter saldo: %w", err)
	}

	currentBalance := account.Balance

	// Calcula novo saldo
	var newBalance float64
	if mov.Tipo == "credito" {
		newBalance = currentBalance + mov.Valor
	} else {
		newBalance = currentBalance - mov.Valor
	}

	// Valida saldo negativo
	if newBalance < 0 {
		metrics.TransactionsRollbackTotal.WithLabelValues("negative_balance").Inc()
		log.Printf("[Transaction] [CorrelationID: %s] Saldo insuficiente: saldo atual=%.2f, valor=%.2f", correlationID, currentBalance, mov.Valor)
		return 0, fmt.Errorf("saldo insuficiente: saldo atual=%.2f, valor=%.2f", currentBalance, mov.Valor)
	}

	// Atualiza saldo da conta
	_, err = tx.NewUpdate().
		Model(&models.Account{}).
		Set("balance = ?", newBalance).
		Set("updated_at = NOW()").
		Where("aggregate_id = ?", mov.ContaID).
		Exec(ctx)
	if err != nil {
		log.Printf("[Transaction] [CorrelationID: %s] Erro ao atualizar saldo: %v", correlationID, err)
		return 0, fmt.Errorf("erro ao atualizar saldo: %w", err)
	}

	// Registra transação com saldo resultante
	transaction := &models.Transaction{
		ID:              mov.MovimentacaoID,
		AccountID:       mov.ContaID,
		TransactionType: string(mov.Tipo),
		Amount:          mov.Valor,
		BalanceAfter:    newBalance,
		Description:     mov.Descricao,
		OccurredAt:      mov.OcorridoEm,
		CreatedAt:       time.Now(),
	}

	_, err = tx.NewInsert().
		Model(transaction).
		Exec(ctx)
	if err != nil {
		log.Printf("[Transaction] [CorrelationID: %s] Erro ao registrar transação: %v", correlationID, err)
		return 0, fmt.Errorf("erro ao registrar transação: %w", err)
	}

	log.Printf("[Transaction] [CorrelationID: %s] Transação processada: %s - Saldo: R$ %.2f → R$ %.2f",
		correlationID, mov.ContaID, currentBalance, newBalance)

	// Obtém versão atual da conta para publicar com saldo
	var version int
	err = tx.NewSelect().
		ColumnExpr("COALESCE(MAX(version), 0)").
		TableExpr("events").
		Where("aggregate_id = ?", mov.ContaID).
		Scan(ctx, &version)
	if err != nil {
		log.Printf("[Transaction] [CorrelationID: %s] Erro ao obter versão: %v", correlationID, err)
	}

	// Publica evento de saldo atualizado
	if err := l.publishBalanceUpdate(mov.ContaID, newBalance, version, correlationID); err != nil {
		log.Printf("[Transaction] [CorrelationID: %s] Erro ao publicar saldo atualizado: %v", correlationID, err)
	}

	return newBalance, nil
}

func (l *Listener) publishConfirmation(mov events.ContaMovimentacao, eventID int64, balanceAfter float64, correlationID string) error {
	// Cria mensagem de confirmação
	confirmation := map[string]interface{}{
		"event_id":        eventID,
		"movimentacao_id": mov.MovimentacaoID,
		"conta_id":        mov.ContaID,
		"tipo":            mov.Tipo,
		"valor":           mov.Valor,
		"balance_after":   balanceAfter,
		"descricao":       mov.Descricao,
		"ocorrido_em":     mov.OcorridoEm,
		"confirmed_at":    mov.OcorridoEm, // timestamp da confirmação
	}

	confirmationJSON, err := json.Marshal(confirmation)
	if err != nil {
		log.Printf("[Transaction] [CorrelationID: %s] Erro ao serializar confirmação: %v", correlationID, err)
		return fmt.Errorf("erro ao serializar confirmação: %w", err)
	}

	topic := l.config.Kafka.TopicNovaTransacaoConfirmada
	headers := map[string]string{
		"correlationID": correlationID,
	}
	// Publica no tópico de confirmações com headers
	if err := l.producer.PublishWithHeaders(topic, []byte(mov.ContaID.String()), confirmationJSON, headers); err != nil {
		log.Printf("[Transaction] [CorrelationID: %s] Erro ao publicar no tópico %s: %v", correlationID, topic, err)
		return fmt.Errorf("erro ao publicar no tópico %s: %w", topic, err)
	}

	log.Printf("[Transaction] [CorrelationID: %s] Confirmação publicada no tópico %s: movimentacao_id=%s, event_id=%d",
		correlationID, topic, mov.MovimentacaoID, eventID)
	return nil
}

func (l *Listener) publishBalanceUpdate(contaID uuid.UUID, balance float64, version int, correlationID string) error {
	// Cria mensagem de saldo atualizado
	balanceUpdate := map[string]interface{}{
		"conta_id":  contaID,
		"balance":   balance,
		"version":   version,
		"timestamp": time.Now(),
	}

	balanceJSON, err := json.Marshal(balanceUpdate)
	if err != nil {
		log.Printf("[Transaction] [CorrelationID: %s] Erro ao serializar saldo atualizado: %v", correlationID, err)
		return fmt.Errorf("erro ao serializar saldo atualizado: %w", err)
	}

	topic := l.config.Kafka.TopicSaldoAtualizado
	headers := map[string]string{
		"correlationID": correlationID,
	}
	// Publica no tópico de saldo atualizado com headers
	if err := l.producer.PublishWithHeaders(topic, []byte(contaID.String()), balanceJSON, headers); err != nil {
		log.Printf("[Transaction] [CorrelationID: %s] Erro ao publicar no tópico %s: %v", correlationID, topic, err)
		return fmt.Errorf("erro ao publicar no tópico %s: %w", topic, err)
	}

	log.Printf("[Transaction] [CorrelationID: %s] Saldo atualizado publicado: conta_id=%s, balance=%.2f, version=%d",
		correlationID, contaID, balance, version)

	return nil
}

func (l *Listener) publishRateLimitedEvent(mov events.ContaMovimentacao, key []byte, value []byte, correlationID string) error {
	topic := l.config.Kafka.TopicTransacaoRateLimited
	headers := map[string]string{
		"correlationID": correlationID,
	}
	// Publica no tópico de transações bloqueadas por rate limit com headers
	if err := l.producer.PublishWithHeaders(topic, key, value, headers); err != nil {
		log.Printf("[Transaction] [CorrelationID: %s] Erro ao publicar no tópico %s: %v", correlationID, topic, err)
		return fmt.Errorf("erro ao publicar no tópico %s: %w", topic, err)
	}

	log.Printf("[Transaction] [CorrelationID: %s] Evento rate limited publicado no tópico %s: conta_id=%s, movimentacao_id=%s",
		correlationID, topic, mov.ContaID, mov.MovimentacaoID)

	return nil
}
