package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	// Event Store Metrics
	EventsAppendedTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "ledger_events_appended_total",
			Help: "Total number of events appended to the Event Store",
		},
		[]string{"aggregate_type", "event_type", "status"},
	)

	EventsAppendDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "ledger_events_append_duration_seconds",
			Help:    "Duration of event append operations in seconds",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"aggregate_type", "event_type"},
	)

	EventsVersionConflictsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "ledger_events_version_conflicts_total",
			Help: "Total number of version conflicts (optimistic locking failures)",
		},
		[]string{"aggregate_type"},
	)

	// Event Processing Metrics
	EventsProcessedTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "ledger_events_processed_total",
			Help: "Total number of events processed successfully",
		},
		[]string{"event_type", "listener"},
	)

	EventsFailedTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "ledger_events_failed_total",
			Help: "Total number of failed event processing attempts",
		},
		[]string{"event_type", "listener", "error_type"},
	)

	EventsProcessingDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "ledger_events_processing_duration_seconds",
			Help:    "Duration of event processing in seconds",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"event_type", "listener"},
	)

	// Business Metrics
	AccountsCreatedTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "ledger_accounts_created_total",
			Help: "Total number of accounts created",
		},
	)

	TransactionsProcessedTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "ledger_transactions_processed_total",
			Help: "Total number of transactions processed",
		},
		[]string{"transaction_type"},
	)

	TransactionsRollbackTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "ledger_transactions_rollback_total",
			Help: "Total number of transaction rollbacks",
		},
		[]string{"reason"},
	)

	TransactionsPerAccount = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "ledger_transactions_per_account",
			Help: "Number of transactions per account",
		},
		[]string{"account_id"},
	)
)
