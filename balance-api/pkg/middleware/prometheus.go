package middleware

import (
	"strconv"
	"time"

	"balance-api/pkg/observability"

	"github.com/gin-gonic/gin"
)

func Prometheus() gin.HandlerFunc {
	return func(c *gin.Context) {
		start := time.Now()
		route := c.FullPath()
		c.Next()
		duration := time.Since(start).Seconds()
		status := strconv.Itoa(c.Writer.Status())
		observability.HTTPRequestsTotal.WithLabelValues(c.Request.Method, route, status).Inc()
		observability.HTTPRequestDurationSeconds.WithLabelValues(c.Request.Method, route).Observe(duration)
	}
}
