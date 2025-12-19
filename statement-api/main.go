package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"statement-api/pkg/config"
	"statement-api/pkg/mongodb"
	"statement-api/routes"

	"github.com/gin-gonic/gin"
)

func main() {
	log.Println("Iniciando Statement API Service...")

	// Carrega configurações
	cfg := config.Load()

	// Configura modo do Gin baseado no ambiente
	if cfg.App.Environment == "production" {
		gin.SetMode(gin.ReleaseMode)
	}

	mongoClient, err := mongodb.NewClient(cfg)
	if err != nil {
		log.Fatalf("Erro ao conectar ao MongoDB: %v", err)
	}
	defer mongoClient.Close()

	router := gin.Default()

	statementHandler := routes.NewStatementHandler(mongoClient)

	router.GET("/health", func(c *gin.Context) {
		c.JSON(200, gin.H{
			"status":      "ok",
			"environment": cfg.App.Environment,
		})
	})
	router.GET("/statements/:conta_id", statementHandler.GetStatements)

	// Configura servidor HTTP com timeouts
	srv := &http.Server{
		Addr:         ":" + cfg.Server.Port,
		Handler:      router,
		ReadTimeout:  cfg.Server.ReadTimeout,
		WriteTimeout: cfg.Server.WriteTimeout,
	}

	// Inicia servidor em goroutine
	go func() {
		log.Printf("Statement API rodando na porta %s (ambiente: %s)", cfg.Server.Port, cfg.App.Environment)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("Erro ao iniciar servidor: %v", err)
		}
	}()

	// Aguarda sinal de interrupção para graceful shutdown
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	log.Println("Desligando servidor...")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := srv.Shutdown(ctx); err != nil {
		log.Fatal("Erro ao desligar servidor:", err)
	}

	log.Println("Servidor desligado com sucesso")
}
