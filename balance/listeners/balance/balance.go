package balance

import (
	"encoding/json"
	"log"

	"balance/internal/models"
	"balance/pkg/config"
	"balance/pkg/scylla"
)

type Listener struct {
	scyllaClient *scylla.Client
	config       *config.Config
}

func NewListener(scyllaClient *scylla.Client, cfg *config.Config) *Listener {
	return &Listener{
		scyllaClient: scyllaClient,
		config:       cfg,
	}
}

func (l *Listener) HandleMessage(key, value []byte, correlationID string) error {
	log.Printf("[BalanceListener] [CorrelationID: %s] Processando atualização de saldo", correlationID)

	var balanceUpdate models.BalanceUpdated

	if err := json.Unmarshal(value, &balanceUpdate); err != nil {
		log.Printf("[BalanceListener] [CorrelationID: %s] Erro ao fazer unmarshal da mensagem: %v", correlationID, err)
		return err
	}

	log.Printf("[BalanceListener] [CorrelationID: %s] Processando atualização de saldo para conta %s: balance=%.2f, version=%d",
		correlationID, balanceUpdate.ContaID, balanceUpdate.Balance, balanceUpdate.Version)

	if err := l.scyllaClient.InsertBalance(balanceUpdate.ContaID, balanceUpdate.Balance, balanceUpdate.Version); err != nil {
		log.Printf("[BalanceListener] [CorrelationID: %s] Erro ao inserir saldo no ScyllaDB: %v", correlationID, err)
		return err
	}

	log.Printf("[BalanceListener] [CorrelationID: %s] Saldo atualizado com sucesso para conta %s", correlationID, balanceUpdate.ContaID)

	return nil
}
