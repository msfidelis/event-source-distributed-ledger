package models

import (
	"time"

	"github.com/google/uuid"
	"github.com/uptrace/bun"
)

// Account representa uma conta bancária
type Account struct {
	bun.BaseModel `bun:"table:accounts,alias:a"`

	AggregateID uuid.UUID `bun:"aggregate_id,pk,type:uuid"`
	OwnerName   string    `bun:"owner_name,notnull"`
	Balance     float64   `bun:"balance,notnull"`
	Status      string    `bun:"status,notnull"`
	CreatedAt   time.Time `bun:"created_at,notnull"`
	UpdatedAt   time.Time `bun:"updated_at,notnull"`
}

// Event representa um evento no event store
type Event struct {
	bun.BaseModel `bun:"table:events,alias:e"`

	ID            int64     `bun:"id,pk,autoincrement"`
	AggregateID   uuid.UUID `bun:"aggregate_id,notnull,type:uuid"`
	AggregateType string    `bun:"aggregate_type,notnull"`
	EventType     string    `bun:"event_type,notnull"`
	EventData     []byte    `bun:"event_data,notnull,type:jsonb"`
	Metadata      []byte    `bun:"metadata,type:jsonb"`
	Version       int       `bun:"version,notnull"`
	OccurredAt    time.Time `bun:"occurred_at,notnull,default:current_timestamp"`
}

// ProcessedMessage é o registro de idempotência: garante que cada event_id seja
// processado exatamente uma vez, mesmo em reentregas at-least-once do Kafka.
type ProcessedMessage struct {
	bun.BaseModel `bun:"table:processed_messages"`

	EventID     string    `bun:"event_id,pk"`
	Topic       string    `bun:"topic,notnull"`
	ProcessedAt time.Time `bun:"processed_at,notnull"`
}

// Transaction representa uma transação bancária
type Transaction struct {
	bun.BaseModel `bun:"table:transactions,alias:t"`

	ID              uuid.UUID `bun:"id,pk,type:uuid"`
	AccountID       uuid.UUID `bun:"account_id,notnull,type:uuid"`
	TransactionType string    `bun:"transaction_type,notnull"`
	Amount          float64   `bun:"amount,notnull"`
	BalanceAfter    float64   `bun:"balance_after,notnull"`
	Description     string    `bun:"description"`
	OccurredAt      time.Time `bun:"occurred_at,notnull"`
	CreatedAt       time.Time `bun:"created_at,notnull,default:current_timestamp"`
}
