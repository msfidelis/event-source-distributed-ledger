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

func (l *Listener) HandleMessage(key, value []byte) error {
	var balanceUpdate models.BalanceUpdated

	if err := json.Unmarshal(value, &balanceUpdate); err != nil {
		log.Printf("[BalanceListener] Erro ao fazer unmarshal da mensagem: %v", err)
		return err
	}

	log.Printf("[BalanceListener] Processando atualização de saldo para conta %s: balance=%.2f, version=%d",
		balanceUpdate.ContaID, balanceUpdate.Balance, balanceUpdate.Version)

	if err := l.scyllaClient.InsertBalance(balanceUpdate.ContaID, balanceUpdate.Balance, balanceUpdate.Version); err != nil {
		log.Printf("[BalanceListener] Erro ao inserir saldo no ScyllaDB: %v", err)
		return err
	}

	log.Printf("[BalanceListener] Saldo atualizado com sucesso para conta %s", balanceUpdate.ContaID)

	return nil
}
