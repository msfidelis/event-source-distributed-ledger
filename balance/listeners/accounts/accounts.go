package accounts

import (
	"balance/internal/models"
	"balance/pkg/config"
	"balance/pkg/scylla"
	"encoding/json"
	"log"
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

	var newAccount models.NewAccount

	if err := json.Unmarshal(value, &newAccount); err != nil {
		log.Printf("[AccountsListener] Erro ao fazer unmarshal da mensagem: %v", err)
		return err
	}

	newAccount.Version = 0 // Versão inicial

	log.Printf("[AccountsListener] Registrando nova conta %s: saldo_inicial=%.2f, version=%d",
		newAccount.ContaID, newAccount.SaldoInicial, newAccount.Version)

	if err := l.scyllaClient.InsertInitialBalance(newAccount.ContaID, newAccount.SaldoInicial, newAccount.Version); err != nil {
		log.Printf("[AccountsListener] Erro ao inserir conta no ScyllaDB: %v", err)
		return err
	}

	log.Printf("[AccountsListener] Conta registrada com sucesso para conta %s", newAccount.ContaID)
	return nil
}
