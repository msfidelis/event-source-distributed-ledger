package accounts

import (
	"balance/internal/models"
	"balance/pkg/config"
	"balance/pkg/logger"
	"balance/pkg/scylla"
	"encoding/json"
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

	log := logger.Instance()
	log.Info().Str("correlation_id", correlationID).Msg("Processando nova conta")

	var newAccount models.NewAccount

	if err := json.Unmarshal(value, &newAccount); err != nil {
		log.Error().Str("correlation_id", correlationID).Err(err).Msg("Erro ao fazer unmarshal da mensagem")
		return err
	}

	newAccount.Version = 0 // Versão inicial

	log.Info().
		Str("correlation_id", correlationID).
		Str("conta_id", newAccount.ContaID.String()).
		Float64("saldo_inicial", newAccount.SaldoInicial).
		Int("version", newAccount.Version).
		Msg("Registrando nova conta")

	if err := l.scyllaClient.InsertInitialBalance(newAccount.ContaID, newAccount.SaldoInicial, newAccount.Version); err != nil {
		log.Error().
			Str("correlation_id", correlationID).
			Str("conta_id", newAccount.ContaID.String()).
			Err(err).
			Msg("Erro ao inserir conta no ScyllaDB")
		return err
	}

	log.Info().
		Str("correlation_id", correlationID).
		Str("conta_id", newAccount.ContaID.String()).
		Msg("Conta registrada com sucesso")
	return nil
}
