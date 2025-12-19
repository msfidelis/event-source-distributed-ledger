package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"statement/listeners/transaction"
	"statement/pkg/config"
	"statement/pkg/kafka"
	"statement/pkg/mongodb"
)

func main() {
	log.Println("Iniciando Statement Ingestion Service...")

	// Carrega configurações
	cfg := config.Load()

	mongoClient, err := mongodb.NewClient(cfg)
	if err != nil {
		log.Fatalf("Erro ao conectar ao MongoDB: %v", err)
	}
	defer mongoClient.Close()

	if err := mongoClient.InitIndexes(); err != nil {
		log.Fatalf("Erro ao inicializar índices do MongoDB: %v", err)
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
		log.Println("Sinal de interrupção recebido, encerrando...")
		cancel()
	}()

	log.Printf("Iniciando consumo de mensagens do tópico %s...", cfg.Kafka.TopicTransactionConfirmed)
	if err := consumer.Consume(ctx, transactionListener.HandleMessage); err != nil {
		if err != context.Canceled {
			log.Fatalf("Erro ao consumir mensagens: %v", err)
		}
	}

	log.Println("Statement Ingestion Service encerrado")
}
