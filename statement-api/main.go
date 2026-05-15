package main

import (
	"context"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"statement-api/pkg/config"
	"statement-api/pkg/logger"
	"statement-api/pkg/mongodb"
	"statement-api/routes"

	"github.com/gin-gonic/gin"
)


func main() {
	appLog := logger.New()
	appLog.Info().Msg("Starting Statement API Service")

	// Load configurations
	cfg := config.Load()

	// Set Gin mode based on environment
	if cfg.App.Environment == "production" {
		gin.SetMode(gin.ReleaseMode)
	}

	mongoClient, err := mongodb.NewClient(cfg)
	if err != nil {
		appLog.Fatal().Err(err).Msg("Error connecting to MongoDB")
	}
	defer mongoClient.Close()

	router := gin.New()
	router.Use(gin.Recovery())
	router.Use(func(c *gin.Context) {
		start := time.Now()
		c.Next()
		appLog.Info().
			Str("method", c.Request.Method).
			Str("path", c.FullPath()).
			Str("client_ip", c.ClientIP()).
			Int("status", c.Writer.Status()).
			Int("size", c.Writer.Size()).
			Dur("latency_ms", time.Since(start)).
			Str("request_id", c.GetHeader("X-Request-ID")).
			Msg("request")
	})

	statementHandler := routes.NewStatementHandler(mongoClient, appLog)
	probeHandler := routes.NewProbeHandler(mongoClient, cfg.App.Environment)

	router.GET("/health", probeHandler.Health)
	router.GET("/livez", probeHandler.Livez)
	router.GET("/readyz", probeHandler.Readyz)
	router.GET("/statements/:account_id", statementHandler.GetStatements)

	// Configura servidor HTTP com timeouts
	srv := &http.Server{
		Addr:         ":" + cfg.Server.Port,
		Handler:      router,
		ReadTimeout:  cfg.Server.ReadTimeout,
		WriteTimeout: cfg.Server.WriteTimeout,
	}

	// Start server in a goroutine
	go func() {
		appLog.Info().Str("port", cfg.Server.Port).Str("environment", cfg.App.Environment).Msg("Servidor iniciado")
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			appLog.Fatal().Err(err).Msg("Erro ao iniciar servidor")
		}
	}()

	// Aguarda sinal de interrupção para graceful shutdown
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	appLog.Info().Msg("Shutdown signal received, shutting down server...")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := srv.Shutdown(ctx); err != nil {
		appLog.Fatal().Err(err).Msg("Error shutting down server")
	}

	appLog.Info().Msg("Server shut down successfully")
}
