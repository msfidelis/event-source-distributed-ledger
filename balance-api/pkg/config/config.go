package config

import (
	"log"
	"os"
	"strconv"
	"strings"
	"time"
)

// Config holds all application configuration
type Config struct {
	// ScyllaDB Configuration
	Scylla ScyllaConfig

	// HTTP Server Configuration
	Server ServerConfig

	// Application Configuration
	App AppConfig
}

// ScyllaConfig holds ScyllaDB connection settings
type ScyllaConfig struct {
	Hosts          []string
	Keyspace       string
	Username       string
	Password       string
	Port           int
	Consistency    string
	Timeout        time.Duration
	ConnectTimeout time.Duration
	ProtoVersion   int
	RetryAttempts  int
}

// ServerConfig holds HTTP server settings
type ServerConfig struct {
	Port         string
	ReadTimeout  time.Duration
	WriteTimeout time.Duration
}

// AppConfig holds application-level settings
type AppConfig struct {
	Environment string
	LogLevel    string
}

// Load reads configuration from environment variables
func Load() *Config {
	config := &Config{
		Scylla: ScyllaConfig{
			Hosts:          parseHosts(getEnv("SCYLLA_HOSTS", "localhost")),
			Keyspace:       getEnv("SCYLLA_KEYSPACE", "balance_ks"),
			Username:       getEnv("SCYLLA_USERNAME", ""),
			Password:       getEnv("SCYLLA_PASSWORD", ""),
			Port:           getEnvAsInt("SCYLLA_PORT", 9042),
			Consistency:    getEnv("SCYLLA_CONSISTENCY", "QUORUM"),
			Timeout:        getEnvAsDuration("SCYLLA_TIMEOUT", 10*time.Second),
			ConnectTimeout: getEnvAsDuration("SCYLLA_CONNECT_TIMEOUT", 10*time.Second),
			ProtoVersion:   getEnvAsInt("SCYLLA_PROTO_VERSION", 4),
			RetryAttempts:  getEnvAsInt("SCYLLA_RETRY_ATTEMPTS", 3),
		},
		Server: ServerConfig{
			Port:         getEnv("PORT", "8083"),
			ReadTimeout:  getEnvAsDuration("SERVER_READ_TIMEOUT", 30*time.Second),
			WriteTimeout: getEnvAsDuration("SERVER_WRITE_TIMEOUT", 30*time.Second),
		},
		App: AppConfig{
			Environment: getEnv("ENVIRONMENT", "development"),
			LogLevel:    getEnv("LOG_LEVEL", "info"),
		},
	}

	log.Printf("[Config] Loaded configuration:")
	log.Printf("  - ScyllaDB Hosts: %v", config.Scylla.Hosts)
	log.Printf("  - ScyllaDB Keyspace: %s", config.Scylla.Keyspace)
	log.Printf("  - ScyllaDB Consistency: %s", config.Scylla.Consistency)
	log.Printf("  - Server Port: %s", config.Server.Port)
	log.Printf("  - Environment: %s", config.App.Environment)

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

// getEnvAsDuration retrieves an environment variable as a duration
func getEnvAsDuration(key string, defaultValue time.Duration) time.Duration {
	valueStr := getEnv(key, "")
	if value, err := time.ParseDuration(valueStr); err == nil {
		return value
	}
	return defaultValue
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
