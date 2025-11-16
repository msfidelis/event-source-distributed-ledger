package main

import (
	"log"
	"os"

	"statement-api/pkg/mongodb"
	"statement-api/routes"

	"github.com/gin-gonic/gin"
)

func main() {
	log.Println("Iniciando Statement API Service...")

	mongodbHosts := getEnv("MONGODB_HOSTS", "localhost:27017")
	port := getEnv("PORT", "8085")

	mongoURI := "mongodb://" + mongodbHosts

	mongoClient, err := mongodb.NewClient(mongoURI, "extrato")
	if err != nil {
		log.Fatalf("Erro ao conectar ao MongoDB: %v", err)
	}
	defer mongoClient.Close()

	router := gin.Default()

	statementHandler := routes.NewStatementHandler(mongoClient)

	router.GET("/health", func(c *gin.Context) {
		c.JSON(200, gin.H{"status": "ok"})
	})
	router.GET("/statements/:conta_id", statementHandler.GetStatements)

	log.Printf("Statement API rodando na porta %s", port)
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
