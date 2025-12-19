package transaction

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

// Listener processa eventos relacionados a transações
type Listener struct {
	db       *bun.DB
	config   *config.Config
	producer *kafka.Producer
}

// NewListener cria uma nova instância do TransactionListener
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

// StartConsuming inicia o consumidor Kafka para eventos de movimentação
func (l *Listener) StartConsuming(ctx context.Context) error {
	topic := l.config.Kafka.TopicContaMovimentacao
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
func (l *Listener) Handle(key, value []byte) error {
	startTime := time.Now()
	eventType := events.EventTypeContaMovimentacao
	listener := "transaction"

	var envelope events.EventEnvelope
	if err := json.Unmarshal(value, &envelope); err != nil {
		metrics.EventsFailedTotal.WithLabelValues(eventType, listener, "unmarshal_error").Inc()
		return fmt.Errorf("erro ao deserializar envelope: %w", err)
	}

	// Converte Data para ContaMovimentacao
	dataBytes, err := json.Marshal(envelope.Data)
	if err != nil {
		metrics.EventsFailedTotal.WithLabelValues(eventType, listener, "marshal_error").Inc()
		return fmt.Errorf("erro ao serializar data: %w", err)
	}

	var movimentacao events.ContaMovimentacao
	if err := json.Unmarshal(dataBytes, &movimentacao); err != nil {
		metrics.EventsFailedTotal.WithLabelValues(eventType, listener, "unmarshal_data_error").Inc()
		return fmt.Errorf("erro ao deserializar ContaMovimentacao: %w", err)
	}

	ctx := context.Background()
	var eventID int64
	var balanceAfter float64

	// Executa as operações de event sourcing e balance dentro de uma transaction
	txErr := l.db.RunInTx(ctx, nil, func(ctx context.Context, tx bun.Tx) error {
		// Persiste no event store
		var err error
		eventID, err = l.saveEventTx(ctx, tx, movimentacao.ContaID, "Account",
			events.EventTypeContaMovimentacao, dataBytes, envelope.Metadata)
		if err != nil {
			tx.Rollback()
			metrics.EventsFailedTotal.WithLabelValues(eventType, listener, "save_event_error").Inc()
			return fmt.Errorf("erro ao salvar evento: %w", err)
		}

		log.Printf("[Transaction] Evento persistido: ContaMovimentacao - ID=%d, Conta=%s (%s) - R$ %.2f",
			eventID, movimentacao.ContaID, movimentacao.Tipo, movimentacao.Valor)

		// Atualiza saldo da conta e registra transação
		balanceAfter, err = l.processTransactionTx(ctx, tx, movimentacao)
		if err != nil {
			log.Printf("[Transaction] Erro ao processar transação: %v", err)
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
	if err := l.publishConfirmation(movimentacao, eventID, balanceAfter); err != nil {
		log.Printf("[Transaction] Erro ao publicar confirmação: %v", err)
	}

	// Métricas de sucesso
	metrics.EventsProcessedTotal.WithLabelValues(eventType, listener).Inc()
	metrics.EventsProcessingDuration.WithLabelValues(eventType, listener).Observe(time.Since(startTime).Seconds())
	metrics.TransactionsProcessedTotal.WithLabelValues(string(movimentacao.Tipo)).Inc()
	metrics.TransactionsPerAccount.WithLabelValues(movimentacao.ContaID.String()).Inc()

	return nil
}

func (l *Listener) saveEventTx(ctx context.Context, tx bun.Tx, aggregateID uuid.UUID, aggregateType, eventType string, eventData []byte, metadata map[string]string) (int64, error) {
	startTime := time.Now()

	// Obtém a versão atual do agregado
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

	_, err = tx.NewInsert().
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

func (l *Listener) processTransactionTx(ctx context.Context, tx bun.Tx, mov events.ContaMovimentacao) (float64, error) {
	// Obtém saldo atual da conta
	var account models.Account
	err := tx.NewSelect().
		Model(&account).
		Where("aggregate_id = ?", mov.ContaID).
		Scan(ctx)
	if err != nil {
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
		return 0, fmt.Errorf("erro ao registrar transação: %w", err)
	}

	log.Printf("[Transaction] Transação processada: %s - Saldo: R$ %.2f → R$ %.2f",
		mov.ContaID, currentBalance, newBalance)

	// Obtém versão atual da conta para publicar com saldo
	var version int
	err = tx.NewSelect().
		ColumnExpr("COALESCE(MAX(version), 0)").
		TableExpr("events").
		Where("aggregate_id = ?", mov.ContaID).
		Scan(ctx, &version)
	if err != nil {
		log.Printf("[Transaction] Erro ao obter versão: %v", err)
	}

	// Publica evento de saldo atualizado
	if err := l.publishBalanceUpdate(mov.ContaID, newBalance, version); err != nil {
		log.Printf("[Transaction] Erro ao publicar saldo atualizado: %v", err)
	}

	return newBalance, nil
}

func (l *Listener) publishConfirmation(mov events.ContaMovimentacao, eventID int64, balanceAfter float64) error {
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
		return fmt.Errorf("erro ao serializar confirmação: %w", err)
	}

	topic := l.config.Kafka.TopicNovaTransacaoConfirmada
	// Publica no tópico de confirmações
	if err := l.producer.Publish(topic, []byte(mov.ContaID.String()), confirmationJSON); err != nil {
		return fmt.Errorf("erro ao publicar no tópico %s: %w", topic, err)
	}

	log.Printf("[Transaction] Confirmação publicada no tópico %s: movimentacao_id=%s, event_id=%d",
		topic, mov.MovimentacaoID, eventID)
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

	log.Printf("[Transaction] Saldo atualizado publicado: conta_id=%s, balance=%.2f, version=%d",
		contaID, balance, version)

	return nil
}
