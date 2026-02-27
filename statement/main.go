package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"statement/listeners/transaction"
	"statement/pkg/config"
	"statement/pkg/kafka"
	"statement/pkg/logger"
	"statement/pkg/mongodb"
)

func main() {
	log := logger.Instance()
	log.Info().Msg("Iniciando Statement Ingestion Service...")

	// Carrega configurações
	cfg := config.Load()

	mongoClient, err := mongodb.NewClient(cfg)
	if err != nil {
		log.Fatal().Err(err).Msg("Erro ao conectar ao MongoDB")
	}
	defer mongoClient.Close()

	if err := mongoClient.InitIndexes(); err != nil {
		log.Fatal().Err(err).Msg("Erro ao inicializar índices do MongoDB")
	}

	transactionListener := transaction.NewListener(mongoClient, cfg)

	consumer := kafka.NewConsumer(
		cfg.Kafka.Brokers,
		cfg.Kafka.TopicTransactionConfirmed,
		cfg.Kafka.GroupStatementIngestion,
	)
	defer consumer.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		sigCh := make(chan os.Signal, 1)
		signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
		<-sigCh
		log.Info().Msg("Sinal de interrupção recebido, encerrando...")
		cancel()
	}()

	log.Info().Msgf("Iniciando consumo de mensagens do tópico %s...", cfg.Kafka.TopicTransactionConfirmed)
	if err := consumer.Consume(ctx, transactionListener.HandleMessage); err != nil {
		if err != context.Canceled {
			log.Fatal().Err(err).Msg("Erro ao consumir mensagens")
		}
	}

	log.Info().Msg("Statement Ingestion Service encerrado")
}
