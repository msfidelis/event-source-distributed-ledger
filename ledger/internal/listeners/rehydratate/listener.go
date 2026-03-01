package rehydratate

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"ledger/internal/models"
	"ledger/internal/utils"
	"ledger/pkg/config"
	"ledger/pkg/events"
	"ledger/pkg/kafka"

	"github.com/google/uuid"
	"github.com/uptrace/bun"
)

// RehydrateRequest é a mensagem recebida no tópico para triggerar rehydratation
type RehydrateRequest struct {
	AggregateID uuid.UUID  `json:"aggregate_id"`
	StartDate   *time.Time `json:"start_date,omitempty"`
	EndDate     *time.Time `json:"end_date,omitempty"`
}

// Listener processa solicitações de rehydratation
type Listener struct {
	db       *bun.DB
	config   *config.Config
	producer *kafka.Producer
}

// NewListener cria uma nova instância do RehydrateListener
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

// StartConsuming inicia o consumidor Kafka para eventos de rehydratation
func (l *Listener) StartConsuming(ctx context.Context) error {
	topic := l.config.Kafka.TopicRehydratate
	groupID := l.config.Kafka.GroupRehydrateListener

	log.Printf("[Rehydrate] Iniciando listener para tópico: %s (group: %s)", topic, groupID)

	consumer := kafka.NewConsumer(l.config.Kafka.Brokers, topic, groupID)
	defer func() {
		log.Printf("[Rehydrate] Fechando consumer do tópico: %s", topic)
		consumer.Close()
		l.producer.Close()
	}()

	return consumer.Consume(ctx, l.Handle)
}

// Handle processa solicitação de rehydratation
func (l *Listener) Handle(key, value []byte, correlationID string) error {
	log.Printf("[Rehydrate] [CorrelationID: %s] Processando solicitação de rehydratation", correlationID)

	var request RehydrateRequest
	if err := json.Unmarshal(value, &request); err != nil {
		log.Printf("[Rehydrate] [CorrelationID: %s] Erro ao deserializar request: %v", correlationID, err)
		return fmt.Errorf("erro ao deserializar request: %w", err)
	}

	log.Printf("[Rehydrate] [CorrelationID: %s] Iniciando rehydratation para aggregate_id=%s", correlationID, request.AggregateID)

	ctx := context.Background()

	// Busca todos os eventos do agregado
	events, err := l.fetchEvents(ctx, request.AggregateID, request.StartDate, request.EndDate)
	if err != nil {
		log.Printf("[Rehydrate] [CorrelationID: %s] Erro ao buscar eventos: %v", correlationID, err)
		return fmt.Errorf("erro ao buscar eventos: %w", err)
	}

	if len(events) == 0 {
		log.Printf("[Rehydrate] [CorrelationID: %s] Nenhum evento encontrado para aggregate_id=%s", correlationID, request.AggregateID)
		return nil
	}

	log.Printf("[Rehydrate] [CorrelationID: %s] Encontrados %d eventos para reprocessar", correlationID, len(events))

	// Reprocessa eventos e republica
	return l.reprocessEvents(ctx, events, correlationID)
}

// fetchEvents busca eventos do Event Store com filtros opcionais de data
func (l *Listener) fetchEvents(ctx context.Context, aggregateID uuid.UUID, startDate, endDate *time.Time) ([]models.Event, error) {
	query := l.db.NewSelect().
		Model((*models.Event)(nil)).
		Where("aggregate_id = ?", aggregateID).
		Order("version ASC")

	if startDate != nil {
		query = query.Where("occurred_at >= ?", *startDate)
	}

	if endDate != nil {
		query = query.Where("occurred_at <= ?", *endDate)
	}

	var events []models.Event
	if err := query.Scan(ctx, &events); err != nil {
		return nil, err
	}

	return events, nil
}

// reprocessEvents reconstrói o estado e republica eventos
func (l *Listener) reprocessEvents(ctx context.Context, eventList []models.Event, correlationID string) error {
	var balance float64
	var ownerName string
	var accountCreated bool

	for _, event := range eventList {
		log.Printf("[Rehydrate] [CorrelationID: %s] Processando evento: id=%d, type=%s, version=%d",
			correlationID, event.ID, event.EventType, event.Version)

		switch event.EventType {
		case events.EventTypeContaCriada:
			if err := l.reprocessAccountCreated(event, &balance, &ownerName, correlationID); err != nil {
				log.Printf("[Rehydrate] [CorrelationID: %s] Erro ao reprocessar AccountCreated: %v", correlationID, err)
				continue
			}
			accountCreated = true

		case events.EventTypeContaMovimentacao:
			if !accountCreated {
				log.Printf("[Rehydrate] [CorrelationID: %s] Pulando movimentação - conta não foi criada ainda", correlationID)
				continue
			}

			newBalance, err := l.reprocessTransaction(event, balance, correlationID)
			if err != nil {
				log.Printf("[Rehydrate] [CorrelationID: %s] Erro ao reprocessar Transaction: %v", correlationID, err)
				continue
			}
			balance = newBalance

			// Republica evento de transação confirmada
			if err := l.republishTransactionConfirmed(event, balance, correlationID); err != nil {
				log.Printf("[Rehydrate] [CorrelationID: %s] Erro ao republicar transação: %v", correlationID, err)
			}
		}

		// Republica saldo atualizado após cada evento
		if err := l.republishBalanceUpdate(event.AggregateID, balance, event.Version, correlationID); err != nil {
			log.Printf("[Rehydrate] [CorrelationID: %s] Erro ao republicar saldo: %v", correlationID, err)
		}
	}

	log.Printf("[Rehydrate] [CorrelationID: %s] Rehydratation concluída: aggregate_id=%s, saldo_final=%.2f",
		correlationID, eventList[0].AggregateID, balance)

	return nil
}

// reprocessAccountCreated reconstrói o estado inicial da conta
func (l *Listener) reprocessAccountCreated(event models.Event, balance *float64, ownerName *string, correlationID string) error {
	var contaCriada events.ContaCriada
	if err := json.Unmarshal(event.EventData, &contaCriada); err != nil {
		log.Printf("[Rehydrate] [CorrelationID: %s] Erro ao unmarshal ContaCriada: %v", correlationID, err)
		return err
	}

	*balance = contaCriada.SaldoInicial
	*ownerName = contaCriada.NomeProprietario

	log.Printf("[Rehydrate] [CorrelationID: %s] Conta criada: owner=%s, saldo_inicial=%.2f", correlationID, *ownerName, *balance)
	return nil
}

// reprocessTransaction reconstrói transação e calcula novo saldo
func (l *Listener) reprocessTransaction(event models.Event, currentBalance float64, correlationID string) (float64, error) {
	var movimentacao events.ContaMovimentacao
	if err := json.Unmarshal(event.EventData, &movimentacao); err != nil {
		log.Printf("[Rehydrate] [CorrelationID: %s] Erro ao unmarshal ContaMovimentacao: %v", correlationID, err)
		return currentBalance, err
	}

	var newBalance float64
	if movimentacao.Tipo == events.TipoCredito {
		newBalance = utils.RoundMoneyUp(currentBalance + movimentacao.Valor)
	} else {
		newBalance = utils.RoundMoneyUp(currentBalance - movimentacao.Valor)
	}

	log.Printf("[Rehydrate] [CorrelationID: %s] Transação: tipo=%s, valor=%.2f, saldo: %.2f → %.2f",
		correlationID, movimentacao.Tipo, movimentacao.Valor, currentBalance, newBalance)

	return newBalance, nil
}

// republishTransactionConfirmed republica evento de transação confirmada
func (l *Listener) republishTransactionConfirmed(event models.Event, balanceAfter float64, correlationID string) error {
	var movimentacao events.ContaMovimentacao
	if err := json.Unmarshal(event.EventData, &movimentacao); err != nil {
		log.Printf("[Rehydrate] [CorrelationID: %s] Erro ao unmarshal ContaMovimentacao: %v", correlationID, err)
		return err
	}

	confirmation := map[string]interface{}{
		"event_id":        event.ID,
		"movimentacao_id": movimentacao.MovimentacaoID,
		"conta_id":        movimentacao.ContaID,
		"tipo":            movimentacao.Tipo,
		"valor":           movimentacao.Valor,
		"balance_after":   balanceAfter,
		"descricao":       movimentacao.Descricao,
		"ocorrido_em":     movimentacao.OcorridoEm,
		"confirmed_at":    time.Now(),
		"rehydrated":      true,
	}

	confirmationJSON, err := json.Marshal(confirmation)
	if err != nil {
		log.Printf("[Rehydrate] [CorrelationID: %s] Erro ao serializar confirmação: %v", correlationID, err)
		return err
	}

	topic := l.config.Kafka.TopicNovaTransacaoConfirmada
	headers := map[string]string{
		"correlationID": correlationID,
	}
	if err := l.producer.PublishWithHeaders(topic, []byte(movimentacao.ContaID.String()), confirmationJSON, headers); err != nil {
		log.Printf("[Rehydrate] [CorrelationID: %s] Erro ao publicar no tópico %s: %v", correlationID, topic, err)
		return err
	}

	log.Printf("[Rehydrate] [CorrelationID: %s] Transação republicada: movimentacao_id=%s", correlationID, movimentacao.MovimentacaoID)
	return nil
}

// republishBalanceUpdate republica evento de saldo atualizado
func (l *Listener) republishBalanceUpdate(aggregateID uuid.UUID, balance float64, version int, correlationID string) error {
	balanceUpdate := map[string]interface{}{
		"conta_id":   aggregateID,
		"balance":    balance,
		"version":    version,
		"timestamp":  time.Now(),
		"rehydrated": true,
	}

	balanceJSON, err := json.Marshal(balanceUpdate)
	if err != nil {
		log.Printf("[Rehydrate] [CorrelationID: %s] Erro ao serializar saldo atualizado: %v", correlationID, err)
		return err
	}

	topic := l.config.Kafka.TopicSaldoAtualizado
	headers := map[string]string{
		"correlationID": correlationID,
	}
	if err := l.producer.PublishWithHeaders(topic, []byte(aggregateID.String()), balanceJSON, headers); err != nil {
		log.Printf("[Rehydrate] [CorrelationID: %s] Erro ao publicar no tópico %s: %v", correlationID, topic, err)
		return err
	}

	log.Printf("[Rehydrate] [CorrelationID: %s] Saldo republicado: conta_id=%s, balance=%.2f, version=%d",
		correlationID, aggregateID, balance, version)

	return nil
}
