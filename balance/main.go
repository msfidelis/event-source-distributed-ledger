package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"balance/listeners/accounts"
	"balance/listeners/balance"
	"balance/pkg/config"
	"balance/pkg/kafka"
	"balance/pkg/scylla"
)

func main() {
	log.Println("Iniciando Balance Ingestion Service...")

	// Carrega configurações
	cfg := config.Load()

	// Conecta ao ScyllaDB
	scyllaClient, err := scylla.NewClient(cfg.Scylla.Hosts)
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
		log.Printf("Iniciando consumo de mensagens do tópico %s...", cfg.Kafka.TopicSaldoAtualizado)
		if err := balanceConsumer.Consume(ctx, balanceListener.HandleMessage); err != nil {
			if err != context.Canceled {
				errChan <- err
			}
		}
	}()

	// Inicia consumer de contas em goroutine
	go func() {
		log.Printf("Iniciando consumo de mensagens do tópico %s...", cfg.Kafka.TopicNovaContaRegistrada)
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
