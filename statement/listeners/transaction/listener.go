package transaction

import (
	"encoding/json"

	"statement/internal/models"
	"statement/pkg/config"
	"statement/pkg/logger"
	"statement/pkg/mongodb"
)

type Listener struct {
	mongoClient *mongodb.Client
	config      *config.Config
}

func NewListener(mongoClient *mongodb.Client, cfg *config.Config) *Listener {
	return &Listener{
		mongoClient: mongoClient,
		config:      cfg,
	}
}

func (l *Listener) HandleMessage(key, value []byte, correlationID string) error {
	log := logger.Instance()
	log.Info().Str("correlation_id", correlationID).Msg("Processando transação confirmada")

	var transactionEvent models.TransactionConfirmed

	if err := json.Unmarshal(value, &transactionEvent); err != nil {
		log.Error().Err(err).Str("correlation_id", correlationID).Msg("Erro ao fazer unmarshal da mensagem")
		return err
	}

	log.Info().
		Str("correlation_id", correlationID).
		Str("transaction_id", transactionEvent.MovimentacaoID).
		Str("account_id", transactionEvent.ContaID).
		Str("type", transactionEvent.Tipo).
		Float64("amount", transactionEvent.Valor).
		Float64("balance_after", transactionEvent.BalanceAfter).
		Msg("Processando transação")

	// Mapeia para o modelo MongoDB usando movimentacao_id como _id
	transaction := models.Transaction{
		ID:           transactionEvent.MovimentacaoID,
		ContaID:      transactionEvent.ContaID,
		EventID:      transactionEvent.EventID,
		Tipo:         transactionEvent.Tipo,
		Valor:        transactionEvent.Valor,
		Descricao:    transactionEvent.Descricao,
		BalanceAfter: transactionEvent.BalanceAfter,
		OcorridoEm:   transactionEvent.OcorridoEm,
		ConfirmedAt:  transactionEvent.ConfirmedAt,
	}

	if err := l.mongoClient.InsertTransaction(transaction); err != nil {
		log.Error().
			Err(err).
			Str("correlation_id", correlationID).
			Str("transaction_id", transactionEvent.MovimentacaoID).
			Str("account_id", transactionEvent.ContaID).
			Str("type", transactionEvent.Tipo).
			Float64("amount", transactionEvent.Valor).
			Msg("Erro ao inserir transação no MongoDB")
		return err
	}

	log.Info().
		Str("correlation_id", correlationID).
		Str("transaction_id", transactionEvent.MovimentacaoID).
		Str("account_id", transactionEvent.ContaID).
		Str("type", transactionEvent.Tipo).
		Float64("amount", transactionEvent.Valor).
		Float64("balance_after", transactionEvent.BalanceAfter).
		Msg("Transação inserida com sucesso")

	return nil
}
