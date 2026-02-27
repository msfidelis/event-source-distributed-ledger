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

	"simulador/pkg/config"
	"simulador/pkg/events"
	"simulador/pkg/kafka"

	"github.com/google/uuid"
)

var nomesPessoas = []string{
	"João Silva", "Maria Santos", "Pedro Oliveira", "Ana Costa",
	"Carlos Souza", "Juliana Lima", "Fernando Alves", "Patricia Rocha",
	"Roberto Martins", "Camila Fernandes",
}

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
	uuid.MustParse("35e44dbc-3214-4ce2-939d-34e77063e3e0"),
	uuid.MustParse("620d43fc-0889-4d17-bd39-6323086be228"),
	uuid.MustParse("2cc7ca7f-226f-4e0b-864b-6c24a13f6a88"),
	uuid.MustParse("49c7d332-6172-4187-b86c-85e2429a6554"),
	uuid.MustParse("d75cb45b-5b6d-4c29-a5ea-b0685af83233"),
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

	// Carrega configurações
	cfg := config.Load()

	// Producers
	producerContas := kafka.NewProducer(cfg, cfg.Kafka.TopicContaCriada)
	producerMovimentacoes := kafka.NewProducer(cfg, cfg.Kafka.TopicContaMovimentacao)
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

	// Fase 1: Criar contas
	log.Printf("\nFase 1: Criando %d contas bancárias...", cfg.Simulation.NumContas)
	contas := criarContas(ctx, producerContas, cfg)

	log.Printf("\nAguardando contas serem processadas (%v)...", cfg.Simulation.WaitAfterCreate)
	time.Sleep(cfg.Simulation.WaitAfterCreate)

	// Fase 2: Simular movimentações
	if cfg.Simulation.ContinuousMode {
		log.Printf("\nFase 2: Iniciando %d workers em MODO CONTÍNUO...", cfg.Simulation.NumWorkers)
		log.Printf("Cada worker gerará %d evento(s) e dormirá por %v", cfg.Simulation.EventsPerWorker, cfg.Simulation.SleepBetweenEvents)
		log.Println("Pressione Ctrl+C para finalizar...")
		simularMovimentacoesContinuas(ctx, producerMovimentacoes, contas, cfg)
	} else {
		log.Printf("\nFase 2: Simulando %d movimentações com %d workers (MODO BATCH)...", cfg.Simulation.NumMovimentacoes, cfg.Simulation.NumWorkers)
		inicio := time.Now()
		simularMovimentacoesBatch(ctx, producerMovimentacoes, contas, cfg)
		duracao := time.Since(inicio)
		log.Printf("\nSimulação concluída em %v!", duracao)
		log.Printf("Taxa: %.0f eventos/segundo", float64(cfg.Simulation.NumMovimentacoes)/duracao.Seconds())
	}
}

func criarContas(ctx context.Context, producer *kafka.Producer, cfg *config.Config) []uuid.UUID {
	rand.Seed(time.Now().UnixNano())

	// Determina quantas contas criar (usa o mínimo entre configuração e contas fixas disponíveis)
	quantidade := cfg.Simulation.NumContas
	if quantidade > len(contasFixas) {
		quantidade = len(contasFixas)
		log.Printf("Aviso: Limitando a %d contas (máximo de UUIDs fixos disponíveis)", quantidade)
	}

	for i := 0; i < quantidade; i++ {
		contaID := contasFixas[i]            // Usa UUID fixo
		correlationID := uuid.New().String() // Gera correlationID para esta operação

		evento := events.ContaCriada{
			ContaID:          contaID,
			NomeProprietario: nomesPessoas[i%len(nomesPessoas)],
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
				"source":        "producer-simulator",
				"version":       "1.0",
				"correlationID": correlationID,
			},
		}

		if err := producer.PublishWithCorrelationID(ctx, contaID.String(), envelope, correlationID); err != nil {
			log.Printf("[CorrelationID: %s] Erro ao criar conta: %v", correlationID, err)
			continue
		}

		log.Printf("[CorrelationID: %s] Conta criada: %s - %s (Saldo: R$ %.2f)",
			correlationID, contaID, evento.NomeProprietario, evento.SaldoInicial)
	}

	return contasFixas[:quantidade] // Retorna as contas criadas
}

func simularMovimentacoesBatch(ctx context.Context, producer *kafka.Producer, contas []uuid.UUID, cfg *config.Config) {
	rand.Seed(time.Now().UnixNano())

	// Canal para distribuir trabalho
	jobs := make(chan int, cfg.Simulation.NumMovimentacoes)
	results := make(chan error, cfg.Simulation.NumMovimentacoes)

	// Inicia workers
	for w := 1; w <= cfg.Simulation.NumWorkers; w++ {
		go batchWorker(ctx, w, jobs, results, producer, contas, cfg)
	}

	// Envia jobs
	for i := 0; i < cfg.Simulation.NumMovimentacoes; i++ {
		jobs <- i
	}
	close(jobs)

	// Aguarda resultados e conta progresso
	erros := 0
	for i := 0; i < cfg.Simulation.NumMovimentacoes; i++ {
		if err := <-results; err != nil {
			erros++
		}

		// Log de progresso a cada 5000 eventos
		if (i+1)%5000 == 0 {
			log.Printf("Progresso: %d/%d eventos publicados", i+1, cfg.Simulation.NumMovimentacoes)
		}
	}

	if erros > 0 {
		log.Printf("Total de erros: %d", erros)
	}
}

func simularMovimentacoesContinuas(ctx context.Context, producer *kafka.Producer, contas []uuid.UUID, cfg *config.Config) {
	rand.Seed(time.Now().UnixNano())

	// Inicia workers contínuos
	for w := 1; w <= cfg.Simulation.NumWorkers; w++ {
		go continuousWorker(ctx, w, producer, contas, cfg)
	}

	// Aguarda cancelamento
	<-ctx.Done()
	log.Println("\nParando workers contínuos...")
}

func batchWorker(ctx context.Context, id int, jobs <-chan int, results chan<- error, producer *kafka.Producer, contas []uuid.UUID, cfg *config.Config) {
	for range jobs {
		select {
		case <-ctx.Done():
			results <- nil
			return
		default:
		}

		err := gerarMovimentacao(ctx, id, producer, contas, cfg)
		results <- err
	}
}

func continuousWorker(ctx context.Context, id int, producer *kafka.Producer, contas []uuid.UUID, cfg *config.Config) {
	log.Printf("[Worker-%d] Iniciado em modo contínuo", id)
	contador := 0

	for {
		select {
		case <-ctx.Done():
			log.Printf("[Worker-%d] Finalizado. Total de eventos gerados: %d", id, contador)
			return
		default:
			// Gera N eventos configurados
			for i := 0; i < cfg.Simulation.EventsPerWorker; i++ {
				if err := gerarMovimentacao(ctx, id, producer, contas, cfg); err != nil {
					log.Printf("[Worker-%d] Erro ao gerar movimentação: %v", id, err)
				} else {
					contador++
				}
			}

			// Log periódico
			if contador%100 == 0 {
				log.Printf("[Worker-%d] %d eventos gerados", id, contador)
			}

			// Dorme entre batches
			time.Sleep(cfg.Simulation.SleepBetweenEvents)
		}
	}
}

func gerarMovimentacao(ctx context.Context, workerID int, producer *kafka.Producer, contas []uuid.UUID, cfg *config.Config) error {
	contaID := contas[rand.Intn(len(contas))]
	correlationID := uuid.New().String() // Gera correlationID para esta transação

	// Usa probabilidade configurável para crédito
	isCredito := rand.Float64() < cfg.Simulation.CreditoProbability

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

	// Usa probabilidade configurável para transferências
	if rand.Float64() < cfg.Simulation.TransferProbability {
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
			"source":        "producer-simulator",
			"version":       "1.0",
			"worker":        fmt.Sprintf("worker-%d", workerID),
			"correlationID": correlationID,
		},
	}

	return producer.PublishWithCorrelationID(ctx, contaID.String(), envelope, correlationID)
}

func randomFloat(min, max float64) float64 {
	return min + rand.Float64()*(max-min)
}
