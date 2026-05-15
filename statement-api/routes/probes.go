package routes

import (
	"context"
	"net/http"
	"time"

	"statement-api/pkg/mongodb"

	"github.com/gin-gonic/gin"
)

type ProbeHandler struct {
	mongoClient *mongodb.Client
	environment string
}

func NewProbeHandler(mongoClient *mongodb.Client, environment string) *ProbeHandler {
	return &ProbeHandler{mongoClient: mongoClient, environment: environment}
}

func (h *ProbeHandler) Health(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{
		"status":      "ok",
		"environment": h.environment,
	})
}

func (h *ProbeHandler) Livez(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{
		"status":  "alive",
		"service": "statement-api",
	})
}

func (h *ProbeHandler) Readyz(c *gin.Context) {
	ctx, cancel := context.WithTimeout(c.Request.Context(), 2*time.Second)
	defer cancel()

	checks := gin.H{"status": "ready", "service": "statement-api"}
	statusCode := http.StatusOK

	if err := h.mongoClient.Ping(ctx); err != nil {
		checks["status"] = "unready"
		checks["mongodb"] = err.Error()
		statusCode = http.StatusServiceUnavailable
	} else {
		checks["mongodb"] = "ok"
	}

	c.JSON(statusCode, checks)
}
