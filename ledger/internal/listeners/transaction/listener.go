package transaction

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"ledger/internal/models"
	"ledger/internal/utils"
	"ledger/pkg/config"
	"ledger/pkg/envoyratelimit"
	"ledger/pkg/events"
	"ledger/pkg/kafka"
	"ledger/pkg/logger"
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
	logger := logger.Instance()
	producer, err := kafka.NewProducer(cfg.Kafka.Brokers)
	if err != nil {
		return nil, fmt.Errorf("erro ao criar produtor: %w", err)
	}

	// Inicializa o cliente Rate Limiter (singleton)
	rateLimiter, err := envoyratelimit.GetInstance(cfg.RateLimit.Host)
	if err != nil {
		logger.Warn().
			Err(err).
			Msg("Aviso: Rate Limiter não disponível")
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
	logger := logger.Instance()

	logger.Info().
		Str("topic", topic).
		Str("group_id", groupID).
		Msg("Iniciando listener de transação")

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

// Handle processa o evento de movimentação de conta
func (l *Listener) Handle(key, value []byte, correlationID string) error {
	startTime := time.Now()
	eventType := events.EventTypeContaMovimentacao
	listener := "transaction"
	logger := logger.Instance()

	logger.Info().
		Str("correlation_id", correlationID).
		Msg("Processando evento de movimentação")

	var envelope events.EventEnvelope
	if err := json.Unmarshal(value, &envelope); err != nil {
		metrics.EventsFailedTotal.WithLabelValues(eventType, listener, "unmarshal_error").Inc()
		logger.Error().
			Str("correlation_id", correlationID).
			Err(err).
			Msg("Erro ao deserializar envelope")
		return fmt.Errorf("erro ao deserializar envelope: %w", err)
	}

	// Converte Data para ContaMovimentacao
	dataBytes, err := json.Marshal(envelope.Data)
	if err != nil {
		metrics.EventsFailedTotal.WithLabelValues(eventType, listener, "marshal_error").Inc()
		logger.Error().
			Str("correlation_id", correlationID).
			Err(err).
			Msg("Erro ao serializar data")
		return fmt.Errorf("erro ao serializar data: %w", err)
	}

	var movimentacao events.ContaMovimentacao
	if err := json.Unmarshal(dataBytes, &movimentacao); err != nil {
		metrics.EventsFailedTotal.WithLabelValues(eventType, listener, "unmarshal_data_error").Inc()
		logger.Error().
			Str("correlation_id", correlationID).
			Err(err).
			Msg("Erro ao deserializar ContaMovimentacao")
		return fmt.Errorf("erro ao deserializar ContaMovimentacao: %w", err)
	}

	// Arredonda o valor da movimentação para 2 casas decimais
	movimentacao.Valor = utils.RoundMoneyUp(movimentacao.Valor)

	// Verifica rate limit se o cliente estiver disponível
	if l.rateLimiter != nil {
		ctxRL := context.Background()
		allowed, err := l.rateLimiter.ShouldRateLimit(ctxRL, "ledger-transactions", "account", movimentacao.ContaID.String())
		if err != nil {
			logger.Error().
				Str("correlation_id", correlationID).
				Err(err).
				Msg("Erro ao verificar rate limit")
			// Continua processamento em caso de erro no rate limiter
		} else if !allowed {
			metrics.EventsFailedTotal.WithLabelValues(eventType, listener, "rate_limited").Inc()

			// Publica evento no tópico de rate limited
			if err := l.publishRateLimitedEvent(movimentacao, key, value, correlationID); err != nil {
				logger.Error().
					Str("correlation_id", correlationID).
					Err(err).
					Msg("Erro ao publicar evento rate limited")
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
			logger.Error().
				Str("correlation_id", correlationID).
				Int64("event_id", eventID).
				Str("account_id", movimentacao.ContaID.String()).
				Str("type", string(movimentacao.Tipo)).
				Float64("transaction_amount", movimentacao.Valor).
				Err(err).
				Msg("Erro ao salvar evento")
			return fmt.Errorf("erro ao salvar evento: %w", err)
		}

		logger.Info().
			Str("correlation_id", correlationID).
			Int64("event_id", eventID).
			Str("account_id", movimentacao.ContaID.String()).
			Str("type", string(movimentacao.Tipo)).
			Float64("transaction_amount", movimentacao.Valor).
			Msg("Evento persistido: ContaMovimentacao")

		// Atualiza saldo da conta e registra transação
		balanceAfter, err = l.processTransactionTx(ctx, tx, movimentacao, correlationID)
		if err != nil {
			logger.Error().
				Str("correlation_id", correlationID).
				Int64("event_id", eventID).
				Str("account_id", movimentacao.ContaID.String()).
				Str("type", string(movimentacao.Tipo)).
				Float64("current_balance", balanceAfter).
				Float64("transaction_amount", movimentacao.Valor).
				Err(err).
				Msg("Erro ao processar transação")
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
		logger.Error().
			Str("correlation_id", correlationID).
			Int64("event_id", eventID).
			Str("account_id", movimentacao.ContaID.String()).
			Str("type", string(movimentacao.Tipo)).
			Float64("transaction_amount", movimentacao.Valor).
			Err(err).
			Msg("Erro ao publicar confirmação")
	}

	// Métricas de sucesso
	metrics.EventsProcessedTotal.WithLabelValues(eventType, listener).Inc()
	metrics.EventsProcessingDuration.WithLabelValues(eventType, listener).Observe(time.Since(startTime).Seconds())
	metrics.TransactionsProcessedTotal.WithLabelValues(string(movimentacao.Tipo)).Inc()
	metrics.TransactionsPerAccount.WithLabelValues(movimentacao.ContaID.String()).Inc()

	logger.Info().
		Str("correlation_id", correlationID).
		Int64("event_id", eventID).
		Str("account_id", movimentacao.ContaID.String()).
		Str("type", string(movimentacao.Tipo)).
		Float64("transaction_amount", movimentacao.Valor).
		Msg("Processamento concluído com sucesso")

	return nil
}

func (l *Listener) saveEventTx(ctx context.Context, tx bun.Tx, aggregateID uuid.UUID, aggregateType, eventType string, eventData []byte, metadata map[string]string, correlationID string) (int64, error) {
	startTime := time.Now()
	logger := logger.Instance()

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

	_, err = tx.NewInsert().
		Model(event).
		Returning("id").
		Exec(ctx)

	if err != nil {
		// Detecta conflitos de versão (optimistic locking)
		if isVersionConflict(err) {
			logger.Error().
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

func (l *Listener) processTransactionTx(ctx context.Context, tx bun.Tx, mov events.ContaMovimentacao, correlationID string) (float64, error) {

	logger := logger.Instance()

	var account models.Account
	err := tx.NewSelect().
		Model(&account).
		Where("aggregate_id = ?", mov.ContaID).
		Scan(ctx)
	if err != nil {
		logger.Error().
			Str("correlation_id", correlationID).
			Err(err).
			Msg("Erro ao obter saldo")
		return 0, fmt.Errorf("erro ao obter saldo: %w", err)
	}

	currentBalance := account.Balance

	// Calcula novo saldo
	var newBalance float64
	if mov.Tipo == "credito" {
		newBalance = utils.RoundMoneyUp(currentBalance + mov.Valor)
	} else {
		newBalance = utils.RoundMoneyUp(currentBalance - mov.Valor)
	}

	// Valida saldo negativo
	if newBalance < 0 {
		metrics.TransactionsRollbackTotal.WithLabelValues("negative_balance").Inc()
		logger.Error().
			Str("correlation_id", correlationID).
			Str("account_id", mov.ContaID.String()).
			Float64("transaction_amount", mov.Valor).
			Float64("current_balance", currentBalance).
			Float64("new_balance", newBalance).
			Msg("Saldo insuficiente")
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
		logger.Error().
			Str("correlation_id", correlationID).
			Err(err).
			Msg("Erro ao atualizar saldo")
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
		logger.Error().
			Str("correlation_id", correlationID).
			Err(err).
			Msg("Erro ao registrar transação")
		return 0, fmt.Errorf("erro ao registrar transação: %w", err)
	}

	logger.Info().
		Str("correlation_id", correlationID).
		Str("account_id", mov.ContaID.String()).
		Float64("transaction_amount", mov.Valor).
		Float64("current_balance", currentBalance).
		Float64("new_balance", newBalance).
		Msg("Transação processada")

	// Obtém versão atual da conta para publicar com saldo
	var version int
	err = tx.NewSelect().
		ColumnExpr("COALESCE(MAX(version), 0)").
		TableExpr("events").
		Where("aggregate_id = ?", mov.ContaID).
		Scan(ctx, &version)
	if err != nil {
		logger.Error().
			Str("correlation_id", correlationID).
			Err(err).
			Msg("Erro ao obter versão")
	}

	// Publica evento de saldo atualizado
	if err := l.publishBalanceUpdate(mov.ContaID, newBalance, version, correlationID); err != nil {
		logger.Error().
			Str("correlation_id", correlationID).
			Err(err).
			Msg("Erro ao publicar saldo atualizado")
	}

	return newBalance, nil
}

func (l *Listener) publishConfirmation(mov events.ContaMovimentacao, eventID int64, balanceAfter float64, correlationID string) error {

	logger := logger.Instance()

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
		logger.Error().
			Str("correlation_id", correlationID).
			Err(err).
			Msg("Erro ao serializar confirmação")
		return fmt.Errorf("erro ao serializar confirmação: %w", err)
	}

	topic := l.config.Kafka.TopicNovaTransacaoConfirmada
	headers := map[string]string{
		"correlationID": correlationID,
	}
	// Publica no tópico de confirmações com headers
	if err := l.producer.PublishWithHeaders(topic, []byte(mov.ContaID.String()), confirmationJSON, headers); err != nil {
		logger.Error().
			Str("correlation_id", correlationID).
			Err(err).
			Msgf("Erro ao publicar no tópico %s", topic)
		return fmt.Errorf("erro ao publicar no tópico %s: %w", topic, err)
	}

	logger.Info().
		Str("correlation_id", correlationID).
		Str("topic", topic).
		Str("movimentacao_id", mov.MovimentacaoID.String()).
		Int64("event_id", eventID).
		Msg("Confirmação publicada")
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
		Str("topic", topic).
		Str("conta_id", contaID.String()).
		Float64("balance", balance).
		Int("version", version).
		Msg("Saldo atualizado publicado")

	return nil
}

func (l *Listener) publishRateLimitedEvent(mov events.ContaMovimentacao, key []byte, value []byte, correlationID string) error {
	logger := logger.Instance()
	topic := l.config.Kafka.TopicTransacaoRateLimited
	headers := map[string]string{
		"correlationID": correlationID,
	}
	// Publica no tópico de transações bloqueadas por rate limit com headers
	if err := l.producer.PublishWithHeaders(topic, key, value, headers); err != nil {
		logger.Error().
			Str("correlation_id", correlationID).
			Err(err).
			Msgf("Erro ao publicar no tópico %s", topic)
		return fmt.Errorf("erro ao publicar no tópico %s: %w", topic, err)
	}

	logger.Debug().
		Str("correlation_id", correlationID).
		Str("topic", topic).
		Str("conta_id", mov.ContaID.String()).
		Str("movimentacao_id", mov.MovimentacaoID.String()).
		Msg("Evento rate limited publicado no tópico")

	return nil
}
