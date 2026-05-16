package main

import (
	"context"
	"net/http"
	"os/signal"
	"syscall"
	"time"

	"balance-api/pkg/config"
	"balance-api/pkg/logger"
	"balance-api/pkg/metrics"
	"balance-api/pkg/middleware"
	"balance-api/pkg/scylla"
	"balance-api/routes"

	"github.com/gin-gonic/gin"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/rs/zerolog"
)

func main() {
	cfg := config.Load()

	level, err := zerolog.ParseLevel(cfg.App.LogLevel)
	if err != nil {
		level = zerolog.InfoLevel
	}
	zerolog.SetGlobalLevel(level)

	log := logger.Instance()
	log.Info().Str("service", "balance-api").Msg("iniciando")

	if cfg.App.Environment == "production" {
		gin.SetMode(gin.ReleaseMode)
	}

	scyllaClient, err := scylla.NewClient(cfg)
	if err != nil {
		log.Fatal().Err(err).Msg("erro ao conectar ao scylladb")
	}

	m := metrics.New()

	router := gin.New()
	router.Use(gin.Recovery())
	router.Use(middleware.RequestLogger())

	balanceHandler := routes.NewBalanceHandler(scyllaClient)
	probeHandler := routes.NewProbeHandler(scyllaClient)

	router.GET("/metrics", gin.WrapH(promhttp.HandlerFor(m.Registry, promhttp.HandlerOpts{})))
	router.GET("/balance/:account_id", balanceHandler.GetBalance)
	router.GET("/health", probeHandler.Health)
	router.GET("/livez", probeHandler.Live)
	router.GET("/readyz", probeHandler.Ready)

	srv := &http.Server{
		Addr:              ":" + cfg.Server.Port,
		Handler:           router,
		ReadTimeout:       cfg.Server.ReadTimeout,
		WriteTimeout:      cfg.Server.WriteTimeout,
		ReadHeaderTimeout: 5 * time.Second,
		IdleTimeout:       120 * time.Second,
		MaxHeaderBytes:    1 << 14,
	}

	go func() {
		log.Info().
			Str("port", cfg.Server.Port).
			Str("environment", cfg.App.Environment).
			Msg("servidor iniciado")
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatal().Err(err).Msg("erro no servidor http")
		}
	}()

	sigCtx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	<-sigCtx.Done()

	log.Info().Msg("sinal recebido, iniciando graceful shutdown")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()

	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Error().Err(err).Msg("erro no shutdown do servidor")
	}

	scyllaClient.Close()
	log.Info().Msg("servidor encerrado com sucesso")
}
