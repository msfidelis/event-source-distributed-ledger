package main

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"statement-api/pkg/config"
	"statement-api/pkg/logger"
	"statement-api/pkg/middleware"
	"statement-api/pkg/mongodb"
	"statement-api/pkg/observability"
	"statement-api/routes"

	"github.com/gin-gonic/gin"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
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
	// mongoClient.Close() is called explicitly in ordered shutdown below — no defer to avoid double-close

	reg := prometheus.NewRegistry()
	reg.MustRegister(
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
	)
	observability.RegisterMetrics(reg)

	router := gin.New()
	router.Use(gin.Recovery())
	router.Use(middleware.Prometheus())
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
			Str("correlation_id", c.GetHeader("X-Correlation-ID")).
			Msg("request")
	})

	statementHandler := routes.NewStatementHandler(mongoClient, appLog)
	probeHandler := routes.NewProbeHandler(mongoClient, cfg.App.Environment)

	router.GET("/metrics", gin.WrapH(promhttp.HandlerFor(reg, promhttp.HandlerOpts{})))
	router.GET("/health", probeHandler.Health)
	router.GET("/livez", probeHandler.Livez)
	router.GET("/readyz", probeHandler.Readyz)
	router.GET("/statements/:account_id", statementHandler.GetStatements)

	srv := &http.Server{
		Addr:              ":" + cfg.Server.Port,
		Handler:           router,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       cfg.Server.ReadTimeout,
		WriteTimeout:      cfg.Server.WriteTimeout,
		IdleTimeout:       60 * time.Second,
		MaxHeaderBytes:    1 << 16,
	}

	appLog.Debug().
		Str("addr", srv.Addr).
		Dur("read_header_timeout", srv.ReadHeaderTimeout).
		Dur("read_timeout", srv.ReadTimeout).
		Dur("write_timeout", srv.WriteTimeout).
		Dur("idle_timeout", srv.IdleTimeout).
		Int("max_header_bytes", srv.MaxHeaderBytes).
		Msg("http server config")

	errCh := make(chan error, 1)
	go func() {
		defer func() {
			if r := recover(); r != nil {
				appLog.Error().Interface("panic", r).Msg("panic na goroutine do servidor")
				errCh <- fmt.Errorf("server panic: %v", r)
			}
		}()
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			errCh <- err
		}
	}()

	appLog.Info().Str("port", cfg.Server.Port).Str("environment", cfg.App.Environment).Msg("Servidor iniciado")

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	select {
	case err := <-errCh:
		appLog.Error().Err(err).Msg("erro no servidor HTTP")
	case sig := <-quit:
		appLog.Info().Str("signal", sig.String()).Msg("sinal de shutdown recebido")
	}

	appLog.Info().Msg("iniciando shutdown gracioso")

	// 1. Parar de aceitar novas conexões e drenar as in-flight
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer shutdownCancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		appLog.Error().Err(err).Msg("erro no shutdown do servidor HTTP")
	}

	// 2. Fechar MongoDB apenas APÓS o servidor ter drenado
	if err := mongoClient.Close(); err != nil {
		appLog.Error().Err(err).Msg("erro ao fechar conexão MongoDB")
	} else {
		appLog.Info().Msg("conexão MongoDB fechada com sucesso")
	}

	appLog.Info().Msg("servidor desligado com sucesso")
}
