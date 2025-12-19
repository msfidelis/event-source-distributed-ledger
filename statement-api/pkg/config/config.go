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
	// MongoDB Configuration
	MongoDB MongoDBConfig

	// HTTP Server Configuration
	Server ServerConfig

	// Application Configuration
	App AppConfig
}

// MongoDBConfig holds MongoDB connection settings
type MongoDBConfig struct {
	Hosts          []string
	Database       string
	Username       string
	Password       string
	URI            string // Full URI (optional, overrides other fields)
	ConnectTimeout time.Duration
	QueryTimeout   time.Duration
	MaxPoolSize    uint64
	MinPoolSize    uint64
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
		MongoDB: MongoDBConfig{
			Hosts:          parseHosts(getEnv("MONGODB_HOSTS", "localhost:27017")),
			Database:       getEnv("MONGODB_DATABASE", "extrato"),
			Username:       getEnv("MONGODB_USERNAME", ""),
			Password:       getEnv("MONGODB_PASSWORD", ""),
			URI:            getEnv("MONGODB_URI", ""),
			ConnectTimeout: getEnvAsDuration("MONGODB_CONNECT_TIMEOUT", 10*time.Second),
			QueryTimeout:   getEnvAsDuration("MONGODB_QUERY_TIMEOUT", 10*time.Second),
			MaxPoolSize:    uint64(getEnvAsInt("MONGODB_MAX_POOL_SIZE", 100)),
			MinPoolSize:    uint64(getEnvAsInt("MONGODB_MIN_POOL_SIZE", 10)),
		},
		Server: ServerConfig{
			Port:         getEnv("PORT", "8085"),
			ReadTimeout:  getEnvAsDuration("SERVER_READ_TIMEOUT", 30*time.Second),
			WriteTimeout: getEnvAsDuration("SERVER_WRITE_TIMEOUT", 30*time.Second),
		},
		App: AppConfig{
			Environment: getEnv("ENVIRONMENT", "development"),
			LogLevel:    getEnv("LOG_LEVEL", "info"),
		},
	}

	log.Printf("[Config] Loaded configuration:")
	log.Printf("  - MongoDB Hosts: %v", config.MongoDB.Hosts)
	log.Printf("  - MongoDB Database: %s", config.MongoDB.Database)
	log.Printf("  - Server Port: %s", config.Server.Port)
	log.Printf("  - Environment: %s", config.App.Environment)

	return config
}

// GetMongoURI returns the full MongoDB connection URI
func (c *Config) GetMongoURI() string {
	if c.MongoDB.URI != "" {
		return c.MongoDB.URI
	}

	hosts := strings.Join(c.MongoDB.Hosts, ",")

	if c.MongoDB.Username != "" && c.MongoDB.Password != "" {
		return "mongodb://" + c.MongoDB.Username + ":" + c.MongoDB.Password + "@" + hosts
	}

	return "mongodb://" + hosts
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
