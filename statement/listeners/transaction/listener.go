package transaction

import (
	"encoding/json"
	"log"

	"statement/internal/models"
	"statement/pkg/mongodb"
)

type Listener struct {
	mongoClient *mongodb.Client
}

func NewListener(mongoClient *mongodb.Client) *Listener {
	return &Listener{
		mongoClient: mongoClient,
	}
}

func (l *Listener) HandleMessage(key, value []byte) error {
	var transactionEvent models.TransactionConfirmed

	if err := json.Unmarshal(value, &transactionEvent); err != nil {
		log.Printf("[TransactionListener] Erro ao fazer unmarshal da mensagem: %v", err)
		return err
	}

	log.Printf("[TransactionListener] Processando transação %s para conta %s: tipo=%s, valor=%.2f",
		transactionEvent.MovimentacaoID, transactionEvent.ContaID, transactionEvent.Tipo, transactionEvent.Valor)

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
		log.Printf("[TransactionListener] Erro ao inserir transação no MongoDB: %v", err)
		return err
	}

	log.Printf("[TransactionListener] Transação %s inserida com sucesso", transactionEvent.MovimentacaoID)

	return nil
}
