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

func (l *Listener) HandleMessage(key, value []byte, correlationID string) error {
	log.Printf("[AccountsListener] [CorrelationID: %s] Processando nova conta", correlationID)

	var newAccount models.NewAccount

	if err := json.Unmarshal(value, &newAccount); err != nil {
		log.Printf("[AccountsListener] [CorrelationID: %s] Erro ao fazer unmarshal da mensagem: %v", correlationID, err)
		return err
	}

	newAccount.Version = 0 // Versão inicial

	log.Printf("[AccountsListener] [CorrelationID: %s] Registrando nova conta %s: saldo_inicial=%.2f, version=%d",
		correlationID, newAccount.ContaID, newAccount.SaldoInicial, newAccount.Version)

	if err := l.scyllaClient.InsertInitialBalance(newAccount.ContaID, newAccount.SaldoInicial, newAccount.Version); err != nil {
		log.Printf("[AccountsListener] [CorrelationID: %s] Erro ao inserir conta no ScyllaDB: %v", correlationID, err)
		return err
	}

	log.Printf("[AccountsListener] [CorrelationID: %s] Conta registrada com sucesso para conta %s", correlationID, newAccount.ContaID)
	return nil
}
