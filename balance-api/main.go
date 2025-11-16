package main

import (
	"log"
	"os"
	"strings"

	"balance-api/pkg/scylla"
	"balance-api/routes"

	"github.com/gin-gonic/gin"
)

func main() {
	log.Println("Iniciando Balance API")

	scyllaHosts := getEnv("SCYLLA_HOSTS", "localhost")
	port := getEnv("PORT", "8083")

	scyllaClient, err := scylla.NewClient(strings.Split(scyllaHosts, ","))
	if err != nil {
		log.Fatalf("Erro ao conectar ao ScyllaDB: %v", err)
	}
	defer scyllaClient.Close()

	router := gin.Default()

	balanceHandler := routes.NewBalanceHandler(scyllaClient)

	router.GET("/health", func(c *gin.Context) {
		c.JSON(200, gin.H{"status": "ok"})
	})
	router.GET("/balance/:account_id", balanceHandler.GetBalance)

	log.Printf("Balance API rodando na porta %s", port)
	if err := router.Run(":" + port); err != nil {
		log.Fatalf("Erro ao iniciar servidor: %v", err)
	}
}

func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}
