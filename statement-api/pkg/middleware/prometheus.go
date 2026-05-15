package middleware

import (
	"strconv"
	"time"

	"statement-api/pkg/observability"

	"github.com/gin-gonic/gin"
)

func Prometheus() gin.HandlerFunc {
	return func(c *gin.Context) {
		start := time.Now()
		path := c.FullPath() // e.g. "/statements/:account_id" — cardinalidade controlada
		c.Next()
		duration := time.Since(start).Seconds()
		status := strconv.Itoa(c.Writer.Status())
		observability.HTTPRequestsTotal.WithLabelValues(c.Request.Method, path, status).Inc()
		observability.HTTPRequestDurationSeconds.WithLabelValues(c.Request.Method, path).Observe(duration)
	}
}
