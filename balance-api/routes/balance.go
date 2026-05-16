package routes

import (
	"context"
	"net/http"
	"time"

	"balance-api/pkg/logger"
	"balance-api/pkg/observability"
	"balance-api/pkg/scylla"

	"github.com/gin-gonic/gin"
	"github.com/gocql/gocql"
	"github.com/rs/zerolog"
)

type BalanceHandler struct {
	scyllaClient *scylla.Client
	log          zerolog.Logger
}

type BalanceResponse struct {
	ID      string  `json:"id"`
	Balance float64 `json:"balance"`
}

func NewBalanceHandler(scyllaClient *scylla.Client) *BalanceHandler {
	return &BalanceHandler{
		scyllaClient: scyllaClient,
		log:          logger.Instance(),
	}
}

func (h *BalanceHandler) GetBalance(c *gin.Context) {
	accountID := c.Param("account_id")

	uuid, err := gocql.ParseUUID(accountID)
	if err != nil {
		h.log.Warn().Str("account_id", accountID).Err(err).Msg("invalid account ID format")
		c.JSON(http.StatusBadRequest, gin.H{
			"error": "Invalid account ID format",
		})
		return
	}

	const balanceLookupSLA = 500 * time.Millisecond
	ctx, cancel := context.WithTimeout(c.Request.Context(), balanceLookupSLA)
	defer cancel()
	h.log.Info().Str("account_id", accountID).Msg("searching balance")
	balance, err := h.scyllaClient.GetBalance(ctx, uuid)
	if err != nil {
		if err == gocql.ErrNotFound {
			h.log.Warn().Str("account_id", accountID).Msg("account not found")
			observability.BusinessLookupsTotal.WithLabelValues("not_found").Inc()
			c.JSON(http.StatusNotFound, gin.H{
				"error": "Account not found",
			})
			return
		}

		h.log.Error().Str("account_id", accountID).Err(err).Msg("error fetching balance")
		observability.BusinessLookupsTotal.WithLabelValues("error").Inc()
		c.JSON(http.StatusInternalServerError, gin.H{
			"error": "Error fetching balance",
		})
		return
	}

	h.log.Info().Str("account_id", accountID).Msg("balance returned")
	observability.BusinessLookupsTotal.WithLabelValues("found").Inc()
	c.JSON(http.StatusOK, BalanceResponse{
		ID:      accountID,
		Balance: balance,
	})
}
