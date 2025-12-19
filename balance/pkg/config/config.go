package config

import (
	"log"
	"os"
	"strconv"
	"strings"
)

// Config holds all application configuration
type Config struct {
	// ScyllaDB Configuration
	Scylla ScyllaConfig

	// Kafka Configuration
	Kafka KafkaConfig

	// Application Configuration
	App AppConfig
}

// ScyllaConfig holds ScyllaDB connection settings
type ScyllaConfig struct {
	Hosts    []string
	Keyspace string
	Username string
	Password string
	Port     int
}

// KafkaConfig holds Kafka broker and topic settings
type KafkaConfig struct {
	Brokers []string

	// Consumer Topics
	TopicSaldoAtualizado     string
	TopicNovaContaRegistrada string

	// Consumer Groups
	GroupBalanceIngestion  string
	GroupAccountsIngestion string
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
		Scylla: ScyllaConfig{
			Hosts:    parseHosts(getEnv("SCYLLA_HOSTS", "localhost")),
			Keyspace: getEnv("SCYLLA_KEYSPACE", "balance"),
			Username: getEnv("SCYLLA_USERNAME", ""),
			Password: getEnv("SCYLLA_PASSWORD", ""),
			Port:     getEnvAsInt("SCYLLA_PORT", 9042),
		},
		Kafka: KafkaConfig{
			Brokers: parseBrokers(getEnv("KAFKA_BROKERS", "localhost:9092")),

			// Consumer Topics
			TopicSaldoAtualizado:     getEnv("KAFKA_TOPIC_SALDO_ATUALIZADO", "ledger_saldo_atualizado"),
			TopicNovaContaRegistrada: getEnv("KAFKA_TOPIC_NOVA_CONTA_REGISTRADA", "ledger_nova_conta_registrada"),

			// Consumer Groups
			GroupBalanceIngestion:  getEnv("KAFKA_GROUP_BALANCE", "balance-ingestion-group"),
			GroupAccountsIngestion: getEnv("KAFKA_GROUP_ACCOUNTS", "balance-accounts-group"),
		},
		App: AppConfig{
			Port:        getEnv("PORT", "8082"),
			Environment: getEnv("ENVIRONMENT", "development"),
			LogLevel:    getEnv("LOG_LEVEL", "info"),
		},
	}

	log.Printf("[Config] Loaded configuration:")
	log.Printf("  - ScyllaDB Hosts: %v", config.Scylla.Hosts)
	log.Printf("  - ScyllaDB Keyspace: %s", config.Scylla.Keyspace)
	log.Printf("  - Kafka Brokers: %v", config.Kafka.Brokers)
	log.Printf("  - Environment: %s", config.App.Environment)
	log.Printf("  - Port: %s", config.App.Port)

	return config
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

// parseHosts parses comma-separated host list
func parseHosts(hosts string) []string {
	hostList := strings.Split(hosts, ",")
	result := make([]string, 0, len(hostList))

	for _, host := range hostList {
		trimmed := strings.TrimSpace(host)
		if trimmed != "" {
			result = append(result, trimmed)
		}
	}

	return result
}
