package models

import "time"

type TransactionConfirmed struct {
	BalanceAfter   float64   `json:"balance_after" bson:"balance_after"`
	ConfirmedAt    time.Time `json:"confirmed_at" bson:"confirmed_at"`
	ContaID        string    `json:"conta_id" bson:"conta_id"`
	Descricao      string    `json:"descricao" bson:"descricao"`
	EventID        int       `json:"event_id" bson:"event_id"`
	MovimentacaoID string    `json:"movimentacao_id" bson:"-"`
	OcorridoEm     time.Time `json:"ocorrido_em" bson:"ocorrido_em"`
	Tipo           string    `json:"tipo" bson:"tipo"`
	Valor          float64   `json:"valor" bson:"valor"`
}

type Transaction struct {
	ID           string    `bson:"_id"`
	ContaID      string    `bson:"conta_id"`
	EventID      int       `bson:"event_id"`
	Tipo         string    `bson:"tipo"`
	Valor        float64   `bson:"valor"`
	Descricao    string    `bson:"descricao"`
	BalanceAfter float64   `bson:"balance_after"`
	OcorridoEm   time.Time `bson:"ocorrido_em"`
	ConfirmedAt  time.Time `bson:"confirmed_at"`
}
