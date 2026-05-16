package routes

import (
	"context"
	"net/http"
	"time"

	"balance-api/pkg/scylla"

	"github.com/gin-gonic/gin"
	"github.com/gocql/gocql"
)

type BalanceHandler struct {
	scyllaClient *scylla.Client
}

type BalanceResponse struct {
	ID      string  `json:"id"`
	Balance float64 `json:"balance"`
}

func NewBalanceHandler(scyllaClient *scylla.Client) *BalanceHandler {
	return &BalanceHandler{
		scyllaClient: scyllaClient,
	}
}

func (h *BalanceHandler) GetBalance(c *gin.Context) {
	accountID := c.Param("account_id")

	uuid, err := gocql.ParseUUID(accountID)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": "Invalid account ID format",
		})
		return
	}

	const balanceLookupSLA = 500 * time.Millisecond
	ctx, cancel := context.WithTimeout(c.Request.Context(), balanceLookupSLA)
	defer cancel()

	balance, err := h.scyllaClient.GetBalance(ctx, uuid)
	if err != nil {
		if err == gocql.ErrNotFound {
			c.JSON(http.StatusNotFound, gin.H{
				"error": "Account not found",
			})
			return
		}

		c.JSON(http.StatusInternalServerError, gin.H{
			"error": "Error fetching balance",
		})
		return
	}

	c.JSON(http.StatusOK, BalanceResponse{
		ID:      accountID,
		Balance: balance,
	})
}
