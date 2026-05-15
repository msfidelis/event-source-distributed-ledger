package observability

import "github.com/prometheus/client_golang/prometheus"

var (
	HTTPRequestsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "statement_api",
			Subsystem: "http",
			Name:      "requests_total",
			Help:      "Total de requisições HTTP recebidas.",
		},
		[]string{"method", "path", "status_code"},
	)

	HTTPRequestDurationSeconds = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "statement_api",
			Subsystem: "http",
			Name:      "request_duration_seconds",
			Help:      "Distribuição de latência das requisições HTTP.",
			Buckets:   []float64{0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0},
		},
		[]string{"method", "path"},
	)

	MongoOperationDurationSeconds = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "statement_api",
			Subsystem: "mongodb",
			Name:      "operation_duration_seconds",
			Help:      "Distribuição de latência das operações MongoDB por tipo.",
			Buckets:   []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 5.0, 10.0},
		},
		[]string{"operation", "collection", "result"},
	)

	MongoConnectionPoolInUse = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "statement_api",
			Subsystem: "mongodb",
			Name:      "connection_pool_in_use",
			Help:      "Conexões MongoDB em uso no pool.",
		},
	)

	MongoConnectionPoolSize = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "statement_api",
			Subsystem: "mongodb",
			Name:      "connection_pool_size",
			Help:      "Tamanho total do pool de conexões MongoDB.",
		},
	)

	StatementTransactionsReturned = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "statement_api",
			Subsystem: "business",
			Name:      "transactions_returned_per_query",
			Help:      "Número de transações retornadas por consulta.",
			Buckets:   []float64{0, 1, 5, 10, 25, 50, 100, 250, 500, 1000},
		},
	)
)

func RegisterMetrics(reg prometheus.Registerer) {
	reg.MustRegister(
		HTTPRequestsTotal,
		HTTPRequestDurationSeconds,
		MongoOperationDurationSeconds,
		MongoConnectionPoolInUse,
		MongoConnectionPoolSize,
		StatementTransactionsReturned,
	)
}
