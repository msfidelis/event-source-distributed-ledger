package main

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"ledger/internal/listeners/account"
	"ledger/internal/listeners/rehydratate"
	"ledger/internal/listeners/transaction"
	"ledger/pkg/config"
	database "ledger/pkg/db"
	"ledger/pkg/migrations"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/uptrace/bun"
)

var bunDB *bun.DB
var cfg *config.Config

func main() {
	log.Println("Iniciando Ledger - Event Store")

	// Carrega configurações
	cfg = config.Load()

	// Executa migrations antes de conectar ao banco
	if err := migrations.RunMigrations(cfg.GetDatabaseURL(), cfg.App.MigrationsPath); err != nil {
		log.Printf("[Migrations] AVISO: Erro ao executar migrations: %v", err)
		log.Println("[Migrations] Continuando inicialização...")
	}

	// Conecta ao PostgreSQL usando Bun
	bunDB = database.GetDB()
	log.Println("Conectado ao PostgreSQL via Bun")

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

	// API HTTP em goroutine separada
	go startHTTPServer()

	// Listeners

	// Listeners de Novas Contas
	accountListener, err := account.NewListener(bunDB, cfg)
	if err != nil {
		log.Fatal("Erro ao criar account listener:", err)
	}

	// Listeners de Transações
	transactionListener, err := transaction.NewListener(bunDB, cfg.Kafka.TopicContaMovimentacao, cfg)
	if err != nil {
		log.Fatal("Erro ao criar transaction listener:", err)
	}

	// Listener de Transações que toparam o rate limit
	transactionRateLimitListener, err := transaction.NewListener(bunDB, cfg.Kafka.TopicTransacaoRateLimited, cfg)
	if err != nil {
		log.Fatal("Erro ao criar transaction listener:", err)
	}

	// Listener de Rehydratation
	rehydrateListener, err := rehydratate.NewListener(bunDB, cfg)
	if err != nil {
		log.Fatal("Erro ao criar rehydrate listener:", err)
	}

	// Inicia os listeners em goroutines separadas
	go func() {
		if err := accountListener.StartConsuming(ctx); err != nil {
			log.Printf("[Account] Erro no consumer: %v", err)
		}
	}()

	go func() {
		if err := transactionListener.StartConsuming(ctx); err != nil {
			log.Printf("[Transaction] Erro no consumer: %v", err)
		}
	}()

	go func() {
		if err := transactionRateLimitListener.StartConsuming(ctx); err != nil {
			log.Printf("[Transaction] Erro no consumer: %v", err)
		}
	}()

	go func() {
		if err := rehydrateListener.StartConsuming(ctx); err != nil {
			log.Printf("[Rehydrate] Erro no consumer: %v", err)
		}
	}()

	<-ctx.Done()
	log.Println("Ledger finalizado")
}

func startHTTPServer() {
	port := cfg.App.Port

	http.HandleFunc("/", handleHome)
	http.HandleFunc("/health", handleHealth)
	http.HandleFunc("/events", handleEvents)
	http.Handle("/metrics", promhttp.Handler())

	log.Printf("API rodando na porta %s", port)
	log.Printf("Endpoints disponíveis em http://localhost:%s", port)
	log.Printf("Prometheus metrics: http://localhost:%s/metrics", port)

	if err := http.ListenAndServe(":"+port, nil); err != nil {
		log.Fatal("Erro ao iniciar servidor HTTP:", err)
	}
}

func handleHome(w http.ResponseWriter, r *http.Request) {
	response := map[string]interface{}{
		"service": "Event Sourcing Ledger",
		"status":  "running",
		"time":    time.Now(),
		"endpoints": map[string]string{
			"GET /health":  "Health check",
			"GET /events":  "Lista todos os eventos do event store",
			"GET /metrics": "Prometheus metrics",
		},
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

func handleHealth(w http.ResponseWriter, r *http.Request) {
	ctx := context.Background()
	if err := bunDB.PingContext(ctx); err != nil {
		w.WriteHeader(http.StatusServiceUnavailable)
		json.NewEncoder(w).Encode(map[string]string{
			"status": "unhealthy",
			"error":  err.Error(),
		})
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{
		"status":   "healthy",
		"database": "connected",
	})
}

func handleEvents(w http.ResponseWriter, r *http.Request) {
	ctx := context.Background()

	type Event struct {
		ID            int64           `json:"id"`
		AggregateID   uuid.UUID       `json:"aggregate_id"`
		AggregateType string          `json:"aggregate_type"`
		EventType     string          `json:"event_type"`
		EventData     json.RawMessage `json:"event_data"`
		Version       int             `json:"version"`
		OccurredAt    time.Time       `json:"occurred_at"`
	}

	var eventsList []Event
	err := bunDB.NewSelect().
		Column("id", "aggregate_id", "aggregate_type", "event_type", "event_data", "version", "occurred_at").
		TableExpr("events").
		Order("occurred_at DESC").
		Limit(100).
		Scan(ctx, &eventsList)

	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"count":  len(eventsList),
		"events": eventsList,
	})
}
