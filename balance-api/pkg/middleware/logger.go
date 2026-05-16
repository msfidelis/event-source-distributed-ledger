package middleware

import (
	"time"

	"balance-api/pkg/logger"

	"github.com/gin-gonic/gin"
)

// RequestLogger retorna um middleware Gin que emite um log estruturado JSON
// por requisição, usando zerolog. O nível é info para status < 500 e error para >= 500.
func RequestLogger() gin.HandlerFunc {
	log := logger.Instance()

	return func(c *gin.Context) {
		start := time.Now()

		c.Next()

		latencyMs := float64(time.Since(start).Nanoseconds()) / 1e6
		status := c.Writer.Status()

		correlationID := c.GetHeader("X-Correlation-ID")
		if correlationID == "" {
			correlationID = c.GetHeader("X-Request-ID")
		}

		event := log.Info()
		if status >= 500 {
			event = log.Error()
		}

		event.
			Str("method", c.Request.Method).
			Str("path", c.FullPath()).
			Str("client_ip", c.ClientIP()).
			Int("status", status).
			Int("size", c.Writer.Size()).
			Float64("latency_ms", latencyMs).
			Str("correlation_id", correlationID).
			Msg("request")
	}
}
