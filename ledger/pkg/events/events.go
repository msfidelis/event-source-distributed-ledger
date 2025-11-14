package events

import (
	"time"

	"github.com/google/uuid"
)

// TipoMovimentacao define se é crédito ou débito
type TipoMovimentacao string

const (
	TipoCredito TipoMovimentacao = "credito"
	TipoDebito  TipoMovimentacao = "debito"
)

// ContaCriada é o evento de criação de uma conta
type ContaCriada struct {
	ContaID          uuid.UUID `json:"conta_id"`
	NomeProprietario string    `json:"nome_proprietario"`
	SaldoInicial     float64   `json:"saldo_inicial"`
	Moeda            string    `json:"moeda"`
	CriadoEm         time.Time `json:"criado_em"`
}

// ContaMovimentacao é o evento de movimentação (crédito ou débito)
type ContaMovimentacao struct {
	MovimentacaoID uuid.UUID        `json:"movimentacao_id"`
	ContaID        uuid.UUID        `json:"conta_id"`
	Tipo           TipoMovimentacao `json:"tipo"`
	Valor          float64          `json:"valor"`
	Descricao      string           `json:"descricao"`
	OcorridoEm     time.Time        `json:"ocorrido_em"`

	// Para transferências
	ContaDestinoID *uuid.UUID `json:"conta_destino_id,omitempty"`
}

// EventEnvelope é o envelope genérico para eventos no Kafka
type EventEnvelope struct {
	EventID   uuid.UUID         `json:"event_id"`
	EventType string            `json:"event_type"`
	Data      interface{}       `json:"data"`
	Timestamp time.Time         `json:"timestamp"`
	Metadata  map[string]string `json:"metadata,omitempty"`
}

const (
	EventTypeContaCriada       = "conta_criada"
	EventTypeContaMovimentacao = "conta_movimentacao"
)
