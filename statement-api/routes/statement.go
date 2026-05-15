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
	"github.com/rs/zerolog"
)

type StatementHandler struct {
	mongoClient *mongodb.Client
	log         zerolog.Logger
}

func NewStatementHandler(mongoClient *mongodb.Client, log zerolog.Logger) *StatementHandler {
	return &StatementHandler{
		mongoClient: mongoClient,
		log:         log,
	}
}

func (h *StatementHandler) GetStatements(c *gin.Context) {
	accountID := c.Param("account_id")

	// Valida se account_id foi fornecido
	if accountID == "" {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": "account_id is required",
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
				"error": fmt.Sprintf("invalid initial_date: %s", err.Error()),
			})
			return
		}
	} else {
		// Last 30 days
		startDate = time.Now().AddDate(0, 0, -30)
	}

	if endDateStr != "" {
		endDate, err = parseDate(endDateStr)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{
				"error": fmt.Sprintf("invalid end_date: %s", err.Error()),
			})
			return
		}
	} else {
		endDate = time.Now()
	}

	// Validate date range
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
	h.log.Info().
		Str("account_id", accountID).
		Str("initial_date", initialDateStr).
		Str("end_date", endDateStr).
		Int("page", page).
		Int("items_per_page", itemsPerPage).
		Msg("Fetching statements")

	result, err := h.mongoClient.GetStatements(c.Request.Context(), accountID, startDate, endDate, page, itemsPerPage)
	if err != nil {
		if errors.Is(err, context.Canceled) {
			h.log.Error().
				Err(err).
				Str("account_id", accountID).
				Str("initial_date", initialDateStr).
				Str("end_date", endDateStr).
				Int("page", page).
				Int("items_per_page", itemsPerPage).
				Msg("Context canceled while fetching statements")
			return
		}
		h.log.Error().
			Err(err).
			Str("account_id", accountID).
			Str("initial_date", initialDateStr).
			Str("end_date", endDateStr).
			Int("page", page).
			Int("items_per_page", itemsPerPage).
			Msg("Error fetching statements")
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
			return time.Time{}, fmt.Errorf("invalid date format %q: expected YYYY-MM-DD or RFC3339: %w", dateStr, err)
		}
		return t, nil
	case len(dateStr) >= 20:
		t, err := time.Parse(time.RFC3339, dateStr)
		if err != nil {
			return time.Time{}, fmt.Errorf("invalid date format %q: expected YYYY-MM-DD or RFC3339: %w", dateStr, err)
		}
		return t, nil
	default:
		return time.Time{}, fmt.Errorf("invalid date format %q: expected YYYY-MM-DD or RFC3339", dateStr)
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
