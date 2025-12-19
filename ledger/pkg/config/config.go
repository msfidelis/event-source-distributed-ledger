package config

import (
	"log"
	"os"
	"strconv"
	"strings"
)

// Config holds all application configuration
type Config struct {
	// Database Configuration
	Database DatabaseConfig

	// Kafka Configuration
	Kafka KafkaConfig

	// Application Configuration
	App AppConfig
}

// DatabaseConfig holds database connection settings
type DatabaseConfig struct {
	Host     string
	Port     string
	User     string
	Password string
	Database string
	SSLMode  string
	URL      string // Full connection URL (optional, overrides individual fields)
}

// KafkaConfig holds Kafka broker and topic settings
type KafkaConfig struct {
	Brokers []string

	// Consumer Topics
	TopicContaCriada       string
	TopicContaMovimentacao string
	TopicRehydratate       string

	// Producer Topics
	TopicNovaContaRegistrada     string
	TopicNovaTransacaoConfirmada string
	TopicSaldoAtualizado         string

	// Consumer Groups
	GroupAccountListener     string
	GroupTransactionListener string
	GroupRehydrateListener   string
}

// AppConfig holds application-level settings
type AppConfig struct {
	Port        string
	Environment string
	LogLevel    string
}

// Load reads configuration from environment variables
func Load() *Config {
	config := &Config{
		Database: DatabaseConfig{
			Host:     getEnv("DATABASE_HOST", "localhost"),
			Port:     getEnv("DATABASE_PORT", "5432"),
			User:     getEnv("DATABASE_USER", "postgres"),
			Password: getEnv("DATABASE_PASSWORD", "postgres"),
			Database: getEnv("DATABASE_DB", "eventsourcing"),
			SSLMode:  getEnv("DATABASE_SSL_MODE", "disable"),
			URL:      getEnv("DATABASE_URL", ""),
		},
		Kafka: KafkaConfig{
			Brokers: parseBrokers(getEnv("KAFKA_BROKERS", "localhost:9092")),

			// Consumer Topics
			TopicContaCriada:       getEnv("KAFKA_TOPIC_CONTA_CRIADA", "conta_criada"),
			TopicContaMovimentacao: getEnv("KAFKA_TOPIC_CONTA_MOVIMENTACAO", "conta_movimentacao"),
			TopicRehydratate:       getEnv("KAFKA_TOPIC_REHYDRATATE", "ledger_rehydratate_transactions"),

			// Producer Topics
			TopicNovaContaRegistrada:     getEnv("KAFKA_TOPIC_NOVA_CONTA_REGISTRADA", "ledger_nova_conta_registrada"),
			TopicNovaTransacaoConfirmada: getEnv("KAFKA_TOPIC_NOVA_TRANSACAO_CONFIRMADA", "ledger_nova_transacao_confirmada"),
			TopicSaldoAtualizado:         getEnv("KAFKA_TOPIC_SALDO_ATUALIZADO", "ledger_saldo_atualizado"),

			// Consumer Groups
			GroupAccountListener:     getEnv("KAFKA_GROUP_ACCOUNT", "ledger-account-group"),
			GroupTransactionListener: getEnv("KAFKA_GROUP_TRANSACTION", "ledger-transaction-group"),
			GroupRehydrateListener:   getEnv("KAFKA_GROUP_REHYDRATE", "ledger-rehydrate-group"),
		},
		App: AppConfig{
			Port:        getEnv("PORT", "8081"),
			Environment: getEnv("ENVIRONMENT", "development"),
			LogLevel:    getEnv("LOG_LEVEL", "info"),
		},
	}

	log.Printf("[Config] Loaded configuration:")
	log.Printf("  - Database: %s:%s/%s", config.Database.Host, config.Database.Port, config.Database.Database)
	log.Printf("  - Kafka Brokers: %v", config.Kafka.Brokers)
	log.Printf("  - Environment: %s", config.App.Environment)
	log.Printf("  - Port: %s", config.App.Port)

	return config
}

// GetDatabaseURL returns the full database connection URL
func (c *Config) GetDatabaseURL() string {
	if c.Database.URL != "" {
		return c.Database.URL
	}

	return "postgres://" +
		c.Database.User + ":" +
		c.Database.Password + "@" +
		c.Database.Host + ":" +
		c.Database.Port + "/" +
		c.Database.Database +
		"?sslmode=" + c.Database.SSLMode
}

// Validate checks if all required configuration is present
func (c *Config) Validate() error {
	// Add validation logic here if needed
	return nil
}

// getEnv retrieves an environment variable or returns a default value
func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

// getEnvAsInt retrieves an environment variable as an integer
func getEnvAsInt(key string, defaultValue int) int {
	valueStr := getEnv(key, "")
	if value, err := strconv.Atoi(valueStr); err == nil {
		return value
	}
	return defaultValue
}

// getEnvAsBool retrieves an environment variable as a boolean
func getEnvAsBool(key string, defaultValue bool) bool {
	valueStr := getEnv(key, "")
	if value, err := strconv.ParseBool(valueStr); err == nil {
		return value
	}
	return defaultValue
}

// parseBrokers parses comma-separated broker list
func parseBrokers(brokers string) []string {
	brokerList := strings.Split(brokers, ",")
	result := make([]string, 0, len(brokerList))

	for _, broker := range brokerList {
		trimmed := strings.TrimSpace(broker)
		if trimmed != "" {
			result = append(result, trimmed)
		}
	}

	return result
}
