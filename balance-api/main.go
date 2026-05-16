package main

import (
	"context"
	"log"
	"net/http"
	"os/signal"
	"syscall"
	"time"

	"balance-api/pkg/config"
	"balance-api/pkg/scylla"
	"balance-api/routes"

	"github.com/gin-gonic/gin"
)

func main() {
	log.Println("Iniciando Balance API")

	// Carrega configurações
	cfg := config.Load()

	// Set Gin mode based on environment
	if cfg.App.Environment == "production" {
		gin.SetMode(gin.ReleaseMode)
	}

	scyllaClient, err := scylla.NewClient(cfg)
	if err != nil {
		log.Fatalf("Erro ao conectar ao ScyllaDB: %v", err)
	}

	router := gin.Default()

	balanceHandler := routes.NewBalanceHandler(scyllaClient)
	probeHandler := routes.NewProbeHandler(scyllaClient)

	router.GET("/balance/:account_id", balanceHandler.GetBalance)
	router.GET("/health", probeHandler.Health)
	router.GET("/livez", probeHandler.Live)
	router.GET("/readyz", probeHandler.Ready)

	// Configure HTTP server with timeouts
	srv := &http.Server{
		Addr:              ":" + cfg.Server.Port,
		Handler:           router,
		ReadTimeout:       cfg.Server.ReadTimeout,
		WriteTimeout:      cfg.Server.WriteTimeout,
		ReadHeaderTimeout: 5 * time.Second,
		IdleTimeout:       120 * time.Second,
		MaxHeaderBytes:    1 << 14, // 16KB
	}

	go func() {
		log.Printf("Balance API rodando na porta %s (environment: %s)", cfg.Server.Port, cfg.App.Environment)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("Erro ao iniciar servidor: %v", err)
		}
	}()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	<-ctx.Done()

	log.Println("Sinal recebido, iniciando graceful shutdown...")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()

	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Printf("Erro no shutdown do servidor: %v", err)
	}

	scyllaClient.Close()
	log.Println("Servidor encerrado com sucesso")
}
