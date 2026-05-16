package observability

import "github.com/prometheus/client_golang/prometheus"

var HTTPBuckets = []float64{0.005, 0.01, 0.025, 0.05, 0.1, 0.2, 0.3, 0.5, 0.75, 1.0, 2.5}
var ScyllaBuckets = []float64{0.001, 0.002, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0}

var (
	HTTPRequestsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "balance_api",
			Subsystem: "http",
			Name:      "requests_total",
			Help:      "Total de requisições HTTP, por método, rota e status_code.",
		},
		[]string{"method", "route", "status_code"},
	)

	HTTPRequestDurationSeconds = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "balance_api",
			Subsystem: "http",
			Name:      "request_duration_seconds",
			Help:      "Distribuição de latência das requisições HTTP.",
			Buckets:   HTTPBuckets,
		},
		[]string{"method", "route"},
	)

	ScyllaQueriesTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "balance_api",
			Subsystem: "scylla",
			Name:      "queries_total",
			Help:      "Total de queries executadas no ScyllaDB, por operação e resultado.",
		},
		[]string{"operation", "result"},
	)

	ScyllaQueryDurationSeconds = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "balance_api",
			Subsystem: "scylla",
			Name:      "query_duration_seconds",
			Help:      "Distribuição de latência das queries ScyllaDB.",
			Buckets:   ScyllaBuckets,
		},
		[]string{"operation"},
	)

	CircuitBreakerState = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "balance_api",
			Subsystem: "circuit_breaker",
			Name:      "state",
			Help:      "Estado atual do circuit breaker: 0=closed, 1=half-open, 2=open.",
		},
		[]string{"name"},
	)

	CircuitBreakerTransitions = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "balance_api",
			Subsystem: "circuit_breaker",
			Name:      "transitions_total",
			Help:      "Total de transições de estado do circuit breaker.",
		},
		[]string{"name", "from", "to"},
	)

	BusinessLookupsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "balance_api",
			Subsystem: "business",
			Name:      "balance_lookups_total",
			Help:      "Total de consultas de saldo por resultado de negócio.",
		},
		// nunca adicionar account_id como label — alta cardinalidade
		[]string{"result"},
	)

	DependencyHealthChecks = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "balance_api",
			Subsystem: "dependency",
			Name:      "health_checks_total",
			Help:      "Total de verificações de saúde de dependências externas.",
		},
		[]string{"dependency", "result"},
	)

	BuildInfo = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "balance_api",
			Name:      "build_info",
			Help:      "Informação de build do serviço (valor sempre 1).",
		},
		[]string{"version", "service"},
	)
)

func RegisterMetrics(reg prometheus.Registerer) {
	reg.MustRegister(
		HTTPRequestsTotal,
		HTTPRequestDurationSeconds,
		ScyllaQueriesTotal,
		ScyllaQueryDurationSeconds,
		CircuitBreakerState,
		CircuitBreakerTransitions,
		BusinessLookupsTotal,
		DependencyHealthChecks,
		BuildInfo,
	)
}
