package routes

import (
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
				"error": "invalid initial_date format, use YYYY-MM-DD or RFC3339",
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
				"error": "invalid end_date format, use YYYY-MM-DD or RFC3339",
			})
			return
		}
	} else {
		endDate = time.Now()
	}

	// Valida range de datas
	if startDate.After(endDate) {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": "initial_date must be before end_date",
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
	result, err := h.mongoClient.GetStatements(contaID, startDate, endDate, page, itemsPerPage)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"error": "Error fetching statements",
		})
		return
	}

	c.JSON(http.StatusOK, result)
}

func parseDate(dateStr string) (time.Time, error) {
	// Tenta parsear como data simples (YYYY-MM-DD)
	if t, err := time.Parse("2006-01-02", dateStr); err == nil {
		return t, nil
	}

	// Tenta parsear como RFC3339
	if t, err := time.Parse(time.RFC3339, dateStr); err == nil {
		return t, nil
	}

	return time.Time{}, nil
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
