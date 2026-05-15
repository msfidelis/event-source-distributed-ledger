package routes

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"statement-api/pkg/mongodb"

	"github.com/gin-gonic/gin"
)

type StatementHandler struct {
	mongoClient *mongodb.Client
}

func NewStatementHandler(mongoClient *mongodb.Client) *StatementHandler {
	return &StatementHandler{
		mongoClient: mongoClient,
	}
}

func (h *StatementHandler) GetStatements(c *gin.Context) {
	contaID := c.Param("conta_id")

	// Valida se conta_id foi fornecido
	if contaID == "" {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": "conta_id is required",
		})
		return
	}

	// Parse de datas (formato: 2006-01-02 ou 2006-01-02T15:04:05Z)
	initialDateStr := c.DefaultQuery("initial_date", "")
	endDateStr := c.DefaultQuery("end_date", "")

	var startDate, endDate time.Time
	var err error

	if initialDateStr != "" {
		startDate, err = parseDate(initialDateStr)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{
				"error": fmt.Sprintf("formato de data inválido para initial_date: %s", err.Error()),
			})
			return
		}
	} else {
		// Últimos 30 dias
		startDate = time.Now().AddDate(0, 0, -30)
	}

	if endDateStr != "" {
		endDate, err = parseDate(endDateStr)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{
				"error": fmt.Sprintf("formato de data inválido para end_date: %s", err.Error()),
			})
			return
		}
	} else {
		endDate = time.Now()
	}

	// Valida range de datas
	if endDate.Before(startDate) {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": "end_date must not be before initial_date",
		})
		return
	}

	if endDate.Sub(startDate) > 366*24*time.Hour {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": "date range must not exceed 366 days",
		})
		return
	}

	// Parse de paginação
	page := parseIntQuery(c, "page", 1)
	itemsPerPage := parseIntQuery(c, "items_per_page", 100)

	// Valida paginação
	if page < 1 {
		page = 1
	}
	if itemsPerPage < 1 || itemsPerPage > 1000 {
		itemsPerPage = 100
	}

	// Busca statements
	result, err := h.mongoClient.GetStatements(c.Request.Context(), contaID, startDate, endDate, page, itemsPerPage)
	if err != nil {
		if errors.Is(err, context.Canceled) {
			// Cliente desconectou intencionalmente — não logar como erro e não escrever resposta HTTP
			return
		}
		c.JSON(http.StatusInternalServerError, gin.H{
			"error": "Error fetching statements",
		})
		return
	}

	c.JSON(http.StatusOK, result)
}

func parseDate(dateStr string) (time.Time, error) {
	switch {
	case len(dateStr) == 10:
		t, err := time.Parse("2006-01-02", dateStr)
		if err != nil {
			return time.Time{}, fmt.Errorf("formato de data inválido %q: esperado YYYY-MM-DD ou RFC3339: %w", dateStr, err)
		}
		return t, nil
	case len(dateStr) >= 20:
		t, err := time.Parse(time.RFC3339, dateStr)
		if err != nil {
			return time.Time{}, fmt.Errorf("formato de data inválido %q: esperado YYYY-MM-DD ou RFC3339: %w", dateStr, err)
		}
		return t, nil
	default:
		return time.Time{}, fmt.Errorf("formato de data inválido %q: esperado YYYY-MM-DD ou RFC3339", dateStr)
	}
}

func parseIntQuery(c *gin.Context, key string, defaultValue int) int {
	valueStr := c.Query(key)
	if valueStr == "" {
		return defaultValue
	}

	value, err := strconv.Atoi(valueStr)
	if err != nil {
		return defaultValue
	}

	return value
}
