package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"statement/listeners/transaction"
	"statement/pkg/kafka"
	"statement/pkg/mongodb"
)

func main() {
	log.Println("Iniciando Statement Ingestion Service...")

	kafkaBrokers := getEnv("KAFKA_BROKERS", "localhost:9092")
	mongodbHosts := getEnv("MONGODB_HOSTS", "localhost:27017")
	kafkaTopic := getEnv("KAFKA_STATEMENTS_TOPIC", "ledger_nova_transacao_confirmada")

	mongoURI := "mongodb://" + mongodbHosts

	mongoClient, err := mongodb.NewClient(mongoURI, "extrato")
	if err != nil {
		log.Fatalf("Erro ao conectar ao MongoDB: %v", err)
	}
	defer mongoClient.Close()

	if err := mongoClient.InitIndexes(); err != nil {
		log.Fatalf("Erro ao inicializar índices do MongoDB: %v", err)
	}

	transactionListener := transaction.NewListener(mongoClient)

	consumer := kafka.NewConsumer(
		kafka.ParseBrokers(kafkaBrokers),
		kafkaTopic,
		"statement-ingestion-group",
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

	log.Printf("Iniciando consumo de mensagens do tópico %s...", kafkaTopic)
	if err := consumer.Consume(ctx, transactionListener.HandleMessage); err != nil {
		if err != context.Canceled {
			log.Fatalf("Erro ao consumir mensagens: %v", err)
		}
	}

	log.Println("Statement Ingestion Service encerrado")
}

func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}
