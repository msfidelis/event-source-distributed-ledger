package main

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"syscall"
	"time"

	"simulador/pkg/events"
	"simulador/pkg/kafka"

	"github.com/google/uuid"
)

var nomesPessoas = []string{
	"João Silva", "Maria Santos", "Pedro Oliveira", "Ana Costa",
	"Carlos Souza", "Juliana Lima", "Fernando Alves", "Patricia Rocha",
	"Roberto Martins", "Camila Fernandes",
}

// 10 UUIDs fixos para enriquecer os aggregates
var contasFixas = []uuid.UUID{
	uuid.MustParse("9d28521f-73eb-4b52-b2c9-4a877951725e"),
	uuid.MustParse("a57eb2c3-dc19-4380-8982-a3968a3c0991"),
	uuid.MustParse("e424ed00-134e-4e92-92c1-40d57a7586c5"),
	uuid.MustParse("b07e027d-c076-4507-b0a3-c66fd6512f1f"),
	uuid.MustParse("2526e98e-02fe-4a3f-8211-f5c99302557b"),
	uuid.MustParse("26a8c777-2a54-4533-a685-de2c8d221bb9"),
	uuid.MustParse("0ab0d853-ca2d-45cd-a66d-3fa4a094c7b8"),
	uuid.MustParse("79253f7e-27a2-4132-9167-889efa9f6e3c"),
	uuid.MustParse("4b3f225f-743c-49cd-a89e-c77ff2b13a3a"),
	uuid.MustParse("2d3792b9-7fb6-496c-be95-df5354787e71"),
}

var descricoesCredito = []string{
	"Salário", "Transferência recebida", "Depósito", "PIX recebido",
	"Cashback", "Reembolso", "Rendimento", "Bonificação",
}

var descricoesDebito = []string{
	"Compra supermercado", "Conta de luz", "Aluguel", "PIX enviado",
	"Restaurante", "Transferência enviada", "Farmácia", "Shopping",
}

func main() {
	log.Println("Iniciando Simulador de Transações Bancárias...")

	// Configuração
	brokers := os.Getenv("KAFKA_BROKERS")
	if brokers == "" {
		brokers = "localhost:9092"
	}

	brokersList := kafka.ParseBrokers(brokers)
	log.Printf("Conectando ao Kafka: %v", brokersList)

	// Producers
	producerContas := kafka.NewProducer(brokersList, "conta_criada")
	producerMovimentacoes := kafka.NewProducer(brokersList, "conta_movimentacao")
	defer producerContas.Close()
	defer producerMovimentacoes.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Captura sinais para graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	go func() {
		<-sigChan
		log.Println("\nSinal de interrupção recebido. Finalizando...")
		cancel()
	}()

	// Fase 1: Criar 10 contas
	log.Println("\nFase 1: Criando 10 contas bancárias...")
	contas := criarContas(ctx, producerContas, 10)

	log.Println("\nAguardando contas serem processadas...")
	time.Sleep(2 * time.Second)

	// Fase 2: Simular movimentações
	log.Println("\nFase 2: Simulando movimentações em alta performance...")
	inicio := time.Now()
	simularMovimentacoes(ctx, producerMovimentacoes, contas)
	duracao := time.Since(inicio)

	log.Printf("\nSimulação concluída em %v!", duracao)
	log.Printf("Taxa: %.0f eventos/segundo", float64(50000)/duracao.Seconds())
}

func criarContas(ctx context.Context, producer *kafka.Producer, quantidade int) []uuid.UUID {
	rand.Seed(time.Now().UnixNano())

	for i := 0; i < quantidade; i++ {
		contaID := contasFixas[i] // Usa UUID fixo

		evento := events.ContaCriada{
			ContaID:          contaID,
			NomeProprietario: nomesPessoas[i],
			SaldoInicial:     randomFloat(1000, 10000),
			Moeda:            "BRL",
			CriadoEm:         time.Now(),
		}

		envelope := events.EventEnvelope{
			EventID:   uuid.New(),
			EventType: events.EventTypeContaCriada,
			Data:      evento,
			Timestamp: time.Now(),
			Metadata: map[string]string{
				"source":  "producer-simulator",
				"version": "1.0",
			},
		}

		if err := producer.Publish(ctx, contaID.String(), envelope); err != nil {
			log.Printf("Erro ao criar conta: %v", err)
			continue
		}

		log.Printf("✅ Conta criada: %s - %s (Saldo: R$ %.2f)",
			contaID, evento.NomeProprietario, evento.SaldoInicial)
	}

	return contasFixas[:quantidade] // Retorna as 10 contas fixas
}

func simularMovimentacoes(ctx context.Context, producer *kafka.Producer, contas []uuid.UUID) {
	rand.Seed(time.Now().UnixNano())

	totalMovimentacoes := 100
	numWorkers := 10 // 10 goroutines paralelas

	// Canal para distribuir trabalho
	jobs := make(chan int, totalMovimentacoes)
	results := make(chan error, totalMovimentacoes)

	// Inicia workers
	for w := 1; w <= numWorkers; w++ {
		go worker(ctx, w, jobs, results, producer, contas)
	}

	// Envia jobs
	for i := 0; i < totalMovimentacoes; i++ {
		jobs <- i
	}
	close(jobs)

	// Aguarda resultados e conta progresso
	erros := 0
	for i := 0; i < totalMovimentacoes; i++ {
		if err := <-results; err != nil {
			erros++
		}

		// Log de progresso a cada 5000 eventos
		if (i+1)%5000 == 0 {
			log.Printf("Progresso: %d/%d eventos publicados", i+1, totalMovimentacoes)
		}
	}

	if erros > 0 {
		log.Printf("Total de erros: %d", erros)
	}
}

func worker(ctx context.Context, id int, jobs <-chan int, results chan<- error, producer *kafka.Producer, contas []uuid.UUID) {
	for range jobs {
		select {
		case <-ctx.Done():
			results <- nil
			return
		default:
		}

		contaID := contas[rand.Intn(len(contas))]

		// 70% chance de crédito, 30% de débito
		isCredito := rand.Float32() < 0.7

		var tipo events.TipoMovimentacao
		var descricao string
		var valor float64

		if isCredito {
			tipo = events.TipoCredito
			descricao = descricoesCredito[rand.Intn(len(descricoesCredito))]
			valor = randomFloat(50, 2000)
		} else {
			tipo = events.TipoDebito
			descricao = descricoesDebito[rand.Intn(len(descricoesDebito))]
			valor = randomFloat(20, 500)
		}

		evento := events.ContaMovimentacao{
			MovimentacaoID: uuid.New(),
			ContaID:        contaID,
			Tipo:           tipo,
			Valor:          valor,
			Descricao:      descricao,
			OcorridoEm:     time.Now(),
		}

		// 20% de chance de ser uma transferência
		if rand.Float32() < 0.2 {
			contaDestinoID := contas[rand.Intn(len(contas))]
			for contaDestinoID == contaID {
				contaDestinoID = contas[rand.Intn(len(contas))]
			}
			evento.ContaDestinoID = &contaDestinoID
			evento.Descricao = "Transferência"
		}

		envelope := events.EventEnvelope{
			EventID:   uuid.New(),
			EventType: events.EventTypeContaMovimentacao,
			Data:      evento,
			Timestamp: time.Now(),
			Metadata: map[string]string{
				"source":  "producer-simulator",
				"version": "1.0",
				"worker":  fmt.Sprintf("worker-%d", id),
			},
		}

		err := producer.Publish(ctx, contaID.String(), envelope)
		results <- err
	}
}

func randomFloat(min, max float64) float64 {
	return min + rand.Float64()*(max-min)
}
