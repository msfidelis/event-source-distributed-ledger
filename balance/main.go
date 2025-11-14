package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"balance/listeners/accounts"
	"balance/listeners/balance"
	"balance/pkg/kafka"
	"balance/pkg/scylla"
)

func main() {
	log.Println("Iniciando Balance Ingestion Service...")

	// Lê configurações das variáveis de ambiente
	kafkaBrokers := getEnv("KAFKA_BROKERS", "localhost:9092")
	scyllaHosts := getEnv("SCYLLA_HOSTS", "localhost")

	// Conecta ao ScyllaDB
	scyllaClient, err := scylla.NewClient(strings.Split(scyllaHosts, ","))
	if err != nil {
		log.Fatalf("Erro ao conectar ao ScyllaDB: %v", err)
	}
	defer scyllaClient.Close()

	// Inicializa o schema do ScyllaDB
	if err := scyllaClient.InitSchema(); err != nil {
		log.Fatalf("Erro ao inicializar schema do ScyllaDB: %v", err)
	}

	// Context com cancelamento via sinal
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Goroutine para capturar sinais de interrupção
	go func() {
		sigCh := make(chan os.Signal, 1)
		signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
		<-sigCh
		log.Println("Sinal de interrupção recebido, encerrando...")
		cancel()
	}()

	// Cria listeners
	balanceListener := balance.NewListener(scyllaClient)
	accountsListener := accounts.NewListener(scyllaClient)

	// Cria consumers
	balanceConsumer := kafka.NewConsumer(
		kafka.ParseBrokers(kafkaBrokers),
		"ledger_saldo_atualizado",
		"balance-ingestion-group",
	)
	defer balanceConsumer.Close()

	accountsConsumer := kafka.NewConsumer(
		kafka.ParseBrokers(kafkaBrokers),
		"ledger_nova_conta_registrada",
		"balance-accounts-group",
	)
	defer accountsConsumer.Close()

	// Canal para capturar erros das goroutines
	errChan := make(chan error, 2)

	// Inicia consumer de saldos em goroutine
	go func() {
		log.Println("Iniciando consumo de mensagens do tópico ledger_saldo_atualizado...")
		if err := balanceConsumer.Consume(ctx, balanceListener.HandleMessage); err != nil {
			if err != context.Canceled {
				errChan <- err
			}
		}
	}()

	// Inicia consumer de contas em goroutine
	go func() {
		log.Println("Iniciando consumo de mensagens do tópico ledger_nova_conta_registrada...")
		if err := accountsConsumer.Consume(ctx, accountsListener.HandleMessage); err != nil {
			if err != context.Canceled {
				errChan <- err
			}
		}
	}()

	// Aguarda erro ou cancelamento
	select {
	case err := <-errChan:
		log.Fatalf("Erro em um dos consumers: %v", err)
	case <-ctx.Done():
		log.Println("Balance Ingestion Service encerrado")
	}
}

func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}
