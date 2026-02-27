package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"balance/listeners/accounts"
	"balance/listeners/balance"
	"balance/pkg/config"
	"balance/pkg/kafka"
	"balance/pkg/logger"
	"balance/pkg/scylla"
)

func main() {
	log := logger.Instance()
	log.Info().Msg("Iniciando Balance Ingestion Service...")

	// Carrega configurações
	cfg := config.Load()

	// Conecta ao ScyllaDB
	scyllaClient, err := scylla.NewClient(cfg.Scylla.Hosts)
	if err != nil {
		log.Fatal().Err(err).Msg("Erro ao conectar ao ScyllaDB")
	}
	defer scyllaClient.Close()

	// Inicializa o schema do ScyllaDB
	if err := scyllaClient.InitSchema(); err != nil {
		log.Fatal().Err(err).Msg("Erro ao inicializar schema do ScyllaDB")
	}

	// Context com cancelamento via sinal
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Goroutine para capturar sinais de interrupção
	go func() {
		sigCh := make(chan os.Signal, 1)
		signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
		<-sigCh
		log.Info().Msg("Sinal de interrupção recebido, encerrando...")
		cancel()
	}()

	// Cria listeners
	balanceListener := balance.NewListener(scyllaClient, cfg)
	accountsListener := accounts.NewListener(scyllaClient, cfg)

	// Cria consumers usando configurações
	balanceConsumer := kafka.NewConsumer(
		cfg.Kafka.Brokers,
		cfg.Kafka.TopicSaldoAtualizado,
		cfg.Kafka.GroupBalanceIngestion,
	)
	defer balanceConsumer.Close()

	accountsConsumer := kafka.NewConsumer(
		cfg.Kafka.Brokers,
		cfg.Kafka.TopicNovaContaRegistrada,
		cfg.Kafka.GroupAccountsIngestion,
	)
	defer accountsConsumer.Close()

	// Canal para capturar erros das goroutines
	errChan := make(chan error, 2)

	// Inicia consumer de saldos em goroutine
	go func() {
		log.Info().Str("topic", cfg.Kafka.TopicSaldoAtualizado).Msg("Iniciando consumo de mensagens")
		if err := balanceConsumer.Consume(ctx, balanceListener.HandleMessage); err != nil {
			if err != context.Canceled {
				errChan <- err
			}
		}
	}()

	// Inicia consumer de contas em goroutine
	go func() {
		log.Info().Str("topic", cfg.Kafka.TopicNovaContaRegistrada).Msg("Iniciando consumo de mensagens")
		if err := accountsConsumer.Consume(ctx, accountsListener.HandleMessage); err != nil {
			if err != context.Canceled {
				errChan <- err
			}
		}
	}()

	// Aguarda erro ou cancelamento
	select {
	case err := <-errChan:
		log.Fatal().Err(err).Msg("Consumer error")
	case <-ctx.Done():
		log.Info().Msg("Balance Ingestion Service encerrado")
	}
}
