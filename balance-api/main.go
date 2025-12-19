package main

import (
	"log"
	"net/http"

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
	defer scyllaClient.Close()

	router := gin.Default()

	balanceHandler := routes.NewBalanceHandler(scyllaClient)

	// Health check endpoint
	router.GET("/health", func(c *gin.Context) {
		c.JSON(200, gin.H{
			"status":      "ok",
			"service":     "balance-api",
			"environment": cfg.App.Environment,
		})
	})

	// Balance endpoints
	router.GET("/balance/:account_id", balanceHandler.GetBalance)

	// Configure HTTP server with timeouts
	srv := &http.Server{
		Addr:         ":" + cfg.Server.Port,
		Handler:      router,
		ReadTimeout:  cfg.Server.ReadTimeout,
		WriteTimeout: cfg.Server.WriteTimeout,
	}

	log.Printf("Balance API rodando na porta %s (environment: %s)", cfg.Server.Port, cfg.App.Environment)
	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatalf("Erro ao iniciar servidor: %v", err)
	}
}
