package routes

import (
	"context"
	"net/http"
	"time"

	"balance-api/pkg/scylla"

	"github.com/gin-gonic/gin"
)

type ProbeHandler struct {
	scyllaClient *scylla.Client
}

func NewProbeHandler(scyllaClient *scylla.Client) *ProbeHandler {
	return &ProbeHandler{scyllaClient: scyllaClient}
}

func (h *ProbeHandler) Live(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{"status": "ok", "service": "balance-api"})
}

func (h *ProbeHandler) Health(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{"status": "ok", "service": "balance-api"})
}

func (h *ProbeHandler) Ready(c *gin.Context) {
	ctx, cancel := context.WithTimeout(c.Request.Context(), 1*time.Second)
	defer cancel()
	if err := h.scyllaClient.Ping(ctx); err != nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{
			"status":     "unavailable",
			"dependency": "scylla",
		})
		return
	}
	c.JSON(http.StatusOK, gin.H{"status": "ok", "service": "balance-api", "scylla": "ok"})
}
