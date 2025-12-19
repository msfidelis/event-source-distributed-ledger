package config

import (
	"log"
	"os"
	"strconv"
	"strings"
	"time"
)

// Config centraliza todas as configurações do simulador
type Config struct {
	Kafka      KafkaConfig
	Simulation SimulationConfig
	App        AppConfig
}

// KafkaConfig contém configurações do Kafka
type KafkaConfig struct {
	Brokers                []string
	TopicContaCriada       string
	TopicContaMovimentacao string
	RequiredAcks           int
	Async                  bool
	BatchSize              int
	BatchTimeout           time.Duration
	Compression            string // snappy, gzip, lz4, zstd, none
}

// SimulationConfig contém configurações da simulação
type SimulationConfig struct {
	NumContas           int
	NumMovimentacoes    int // Usado apenas em modo batch (ContinuousMode=false)
	NumWorkers          int
	CreditoProbability  float64 // Probabilidade de crédito (0.0 a 1.0)
	TransferProbability float64 // Probabilidade de transferência (0.0 a 1.0)
	WaitAfterCreate     time.Duration
	ContinuousMode      bool          // Se true, workers rodam continuamente
	SleepBetweenEvents  time.Duration // Intervalo entre eventos em modo contínuo
	EventsPerWorker     int           // Quantos eventos cada worker gera antes de dormir (em modo contínuo)
}

// AppConfig contém configurações gerais da aplicação
type AppConfig struct {
	Environment string
	LogLevel    string
}

// Load carrega as configurações de variáveis de ambiente
func Load() *Config {
	cfg := &Config{
		Kafka: KafkaConfig{
			Brokers:                parseBrokers(getEnv("KAFKA_BROKERS", "localhost:9092")),
			TopicContaCriada:       getEnv("KAFKA_TOPIC_CONTA_CRIADA", "conta_criada"),
			TopicContaMovimentacao: getEnv("KAFKA_TOPIC_CONTA_MOVIMENTACAO", "conta_movimentacao"),
			RequiredAcks:           getEnvAsInt("KAFKA_REQUIRED_ACKS", 1), // 0=NoResponse, 1=WaitForLeader, -1=WaitForAll
			Async:                  getEnvAsBool("KAFKA_ASYNC", true),
			BatchSize:              getEnvAsInt("KAFKA_BATCH_SIZE", 100),
			BatchTimeout:           getEnvAsDuration("KAFKA_BATCH_TIMEOUT", 10*time.Millisecond),
			Compression:            getEnv("KAFKA_COMPRESSION", "snappy"), // snappy, gzip, lz4, zstd, none
		},
		Simulation: SimulationConfig{
			NumContas:           getEnvAsInt("SIM_NUM_CONTAS", 10),
			NumMovimentacoes:    getEnvAsInt("SIM_NUM_MOVIMENTACOES", 100),
			NumWorkers:          getEnvAsInt("SIM_NUM_WORKERS", 10),
			CreditoProbability:  getEnvAsFloat("SIM_CREDITO_PROBABILITY", 0.7),
			TransferProbability: getEnvAsFloat("SIM_TRANSFER_PROBABILITY", 0.2),
			WaitAfterCreate:     getEnvAsDuration("SIM_WAIT_AFTER_CREATE", 10*time.Second),
			ContinuousMode:      getEnvAsBool("SIM_CONTINUOUS_MODE", false),
			SleepBetweenEvents:  getEnvAsDuration("SIM_SLEEP_BETWEEN_EVENTS", 1*time.Second),
			EventsPerWorker:     getEnvAsInt("SIM_EVENTS_PER_WORKER", 1),
		},
		App: AppConfig{
			Environment: getEnv("ENVIRONMENT", "development"),
			LogLevel:    getEnv("LOG_LEVEL", "info"),
		},
	}

	log.Println("=== Configuração do Simulador ===")
	log.Printf("Kafka Brokers: %v", cfg.Kafka.Brokers)
	log.Printf("Tópico Conta Criada: %s", cfg.Kafka.TopicContaCriada)
	log.Printf("Tópico Movimentação: %s", cfg.Kafka.TopicContaMovimentacao)
	log.Printf("Kafka Async: %v, Batch Size: %d, Compression: %s", cfg.Kafka.Async, cfg.Kafka.BatchSize, cfg.Kafka.Compression)
	log.Printf("Contas a criar: %d", cfg.Simulation.NumContas)
	log.Printf("Modo: %s", map[bool]string{true: "CONTÍNUO", false: "BATCH"}[cfg.Simulation.ContinuousMode])
	if cfg.Simulation.ContinuousMode {
		log.Printf("Workers paralelos: %d (contínuos)", cfg.Simulation.NumWorkers)
		log.Printf("Eventos por worker: %d, Sleep: %v", cfg.Simulation.EventsPerWorker, cfg.Simulation.SleepBetweenEvents)
	} else {
		log.Printf("Movimentações a simular: %d", cfg.Simulation.NumMovimentacoes)
		log.Printf("Workers paralelos: %d", cfg.Simulation.NumWorkers)
	}
	log.Printf("Probabilidade Crédito: %.0f%%, Transferência: %.0f%%", cfg.Simulation.CreditoProbability*100, cfg.Simulation.TransferProbability*100)
	log.Printf("Ambiente: %s, Log Level: %s", cfg.App.Environment, cfg.App.LogLevel)
	log.Println("=================================")

	return cfg
}

// Helper functions

func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

func getEnvAsInt(key string, defaultValue int) int {
	valueStr := os.Getenv(key)
	if valueStr == "" {
		return defaultValue
	}
	value, err := strconv.Atoi(valueStr)
	if err != nil {
		log.Printf("Aviso: valor inválido para %s, usando padrão %d", key, defaultValue)
		return defaultValue
	}
	return value
}

func getEnvAsFloat(key string, defaultValue float64) float64 {
	valueStr := os.Getenv(key)
	if valueStr == "" {
		return defaultValue
	}
	value, err := strconv.ParseFloat(valueStr, 64)
	if err != nil {
		log.Printf("Aviso: valor inválido para %s, usando padrão %.2f", key, defaultValue)
		return defaultValue
	}
	return value
}

func getEnvAsBool(key string, defaultValue bool) bool {
	valueStr := os.Getenv(key)
	if valueStr == "" {
		return defaultValue
	}
	value, err := strconv.ParseBool(valueStr)
	if err != nil {
		log.Printf("Aviso: valor inválido para %s, usando padrão %v", key, defaultValue)
		return defaultValue
	}
	return value
}

func getEnvAsDuration(key string, defaultValue time.Duration) time.Duration {
	valueStr := os.Getenv(key)
	if valueStr == "" {
		return defaultValue
	}
	value, err := time.ParseDuration(valueStr)
	if err != nil {
		log.Printf("Aviso: valor inválido para %s, usando padrão %v", key, defaultValue)
		return defaultValue
	}
	return value
}

func parseBrokers(brokersStr string) []string {
	brokers := strings.Split(brokersStr, ",")
	for i, broker := range brokers {
		brokers[i] = strings.TrimSpace(broker)
	}
	return brokers
}
