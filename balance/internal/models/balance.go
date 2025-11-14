package models

import (
	"time"

	"github.com/gocql/gocql"
)

type BalanceUpdated struct {
	Balance   float64    `json:"balance"`
	ContaID   gocql.UUID `json:"conta_id"`
	Timestamp time.Time  `json:"timestamp"`
	Version   int        `json:"version"`
}

type Balance struct {
	ID      gocql.UUID
	Balance float64
	Version int
}

type NewAccount struct {
	ContaID      gocql.UUID `json:"conta_id"`
	SaldoInicial float64    `json:"saldo_inicial"`
	Version      int        `json:"version"`
}
