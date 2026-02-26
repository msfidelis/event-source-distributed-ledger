package transaction

import (
	"encoding/json"
	"log"

	"statement/internal/models"
	"statement/pkg/config"
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
	log.Printf("[TransactionListener] [CorrelationID: %s] Processando transação confirmada", correlationID)

	var transactionEvent models.TransactionConfirmed

	if err := json.Unmarshal(value, &transactionEvent); err != nil {
		log.Printf("[TransactionListener] [CorrelationID: %s] Erro ao fazer unmarshal da mensagem: %v", correlationID, err)
		return err
	}

	log.Printf("[TransactionListener] [CorrelationID: %s] Processando transação %s para conta %s: tipo=%s, valor=%.2f",
		correlationID, transactionEvent.MovimentacaoID, transactionEvent.ContaID, transactionEvent.Tipo, transactionEvent.Valor)

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
		log.Printf("[TransactionListener] [CorrelationID: %s] Erro ao inserir transação no MongoDB: %v", correlationID, err)
		return err
	}

	log.Printf("[TransactionListener] [CorrelationID: %s] Transação %s inserida com sucesso", correlationID, transactionEvent.MovimentacaoID)

	return nil
}
