package balance

import (
	"encoding/json"

	"balance/internal/models"
	"balance/pkg/config"
	"balance/pkg/logger"
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

	log := logger.Instance()

	log.Info().Str("correlation_id", correlationID).Msg("Processando atualização de saldo")

	var balanceUpdate models.BalanceUpdated

	if err := json.Unmarshal(value, &balanceUpdate); err != nil {
		log.Error().Str("correlation_id", correlationID).Err(err).Msg("Erro ao fazer unmarshal da mensagem")
		return err
	}

	log.Info().
		Str("correlation_id", correlationID).
		Str("conta_id", balanceUpdate.ContaID.String()).
		Float64("balance", balanceUpdate.Balance).
		Int("version", balanceUpdate.Version).
		Msg("Processando atualização de saldo")

	if err := l.scyllaClient.InsertBalance(balanceUpdate.ContaID, balanceUpdate.Balance, balanceUpdate.Version); err != nil {
		log.Error().
			Str("correlation_id", correlationID).
			Str("conta_id", balanceUpdate.ContaID.String()).
			Err(err).
			Msg("Erro ao inserir saldo no ScyllaDB")
		return err
	}

	log.Info().
		Str("correlation_id", correlationID).
		Str("conta_id", balanceUpdate.ContaID.String()).
		Msg("Saldo atualizado com sucesso")

	return nil
}
