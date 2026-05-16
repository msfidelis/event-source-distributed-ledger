package metrics

import "github.com/prometheus/client_golang/prometheus"

// HTTPBuckets são calibrados para SLA de 500ms com resolução na cauda.
var HTTPBuckets = []float64{0.005, 0.01, 0.025, 0.05, 0.1, 0.2, 0.3, 0.5, 0.75, 1.0, 2.5}

// ScyllaBuckets são calibrados para ScyllaDB: resposta normal em 1–10ms, cauda até 500ms.
var ScyllaBuckets = []float64{0.001, 0.002, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0}

// Metrics agrupa todas as métricas do serviço e o registry não-default.
type Metrics struct {
	Registry *prometheus.Registry

	// RED metrics do endpoint HTTP
	HTTPRequestsTotal          *prometheus.CounterVec
	HTTPRequestDurationSeconds *prometheus.HistogramVec

	// Métricas de operação ScyllaDB
	ScyllaQueriesTotal         *prometheus.CounterVec
	ScyllaQueryDurationSeconds *prometheus.HistogramVec

	// Circuit breaker
	CircuitBreakerState       *prometheus.GaugeVec
	CircuitBreakerTransitions *prometheus.CounterVec

	// Métricas funcionais de negócio
	BusinessLookupsTotal *prometheus.CounterVec

	// Saúde de dependências (health probes)
	DependencyHealthChecks *prometheus.CounterVec

	// Build info
	BuildInfo *prometheus.GaugeVec
}

// New instancia todas as métricas, registra no registry interno e retorna o conjunto.
func New() *Metrics {
	reg := prometheus.NewRegistry()

	m := &Metrics{
		Registry: reg,

		HTTPRequestsTotal: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: "balance_api",
				Subsystem: "http",
				Name:      "requests_total",
				Help:      "Total de requisições HTTP, por método, rota e status_code.",
			},
			[]string{"method", "route", "status_code"},
		),

		HTTPRequestDurationSeconds: prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Namespace: "balance_api",
				Subsystem: "http",
				Name:      "request_duration_seconds",
				Help:      "Distribuição de latência das requisições HTTP.",
				Buckets:   HTTPBuckets,
			},
			[]string{"method", "route", "status_code"},
		),

		ScyllaQueriesTotal: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: "balance_api",
				Subsystem: "scylla",
				Name:      "queries_total",
				Help:      "Total de queries executadas no ScyllaDB, por operação e resultado.",
			},
			[]string{"operation", "result"},
		),

		ScyllaQueryDurationSeconds: prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Namespace: "balance_api",
				Subsystem: "scylla",
				Name:      "query_duration_seconds",
				Help:      "Distribuição de latência das queries ScyllaDB.",
				Buckets:   ScyllaBuckets,
			},
			[]string{"operation"},
		),

		CircuitBreakerState: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace: "balance_api",
				Subsystem: "circuit_breaker",
				Name:      "state",
				Help:      "Estado atual do circuit breaker: 0=closed, 1=half-open, 2=open.",
			},
			[]string{"name"},
		),

		CircuitBreakerTransitions: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: "balance_api",
				Subsystem: "circuit_breaker",
				Name:      "transitions_total",
				Help:      "Total de transições de estado do circuit breaker.",
			},
			[]string{"name", "from", "to"},
		),

		BusinessLookupsTotal: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: "balance_api",
				Subsystem: "business",
				Name:      "balance_lookups_total",
				Help:      "Total de consultas de saldo por resultado de negócio.",
			},
			// AVISO: nunca adicionar account_id como label — alta cardinalidade
			[]string{"result"},
		),

		DependencyHealthChecks: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: "balance_api",
				Subsystem: "dependency",
				Name:      "health_checks_total",
				Help:      "Total de verificações de saúde de dependências externas.",
			},
			[]string{"dependency", "result"},
		),

		BuildInfo: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace: "balance_api",
				Name:      "build_info",
				Help:      "Informação de build do serviço (valor sempre 1).",
			},
			[]string{"version", "service"},
		),
	}

	reg.MustRegister(
		prometheus.NewGoCollector(),
		prometheus.NewProcessCollector(prometheus.ProcessCollectorOpts{}),
		m.HTTPRequestsTotal,
		m.HTTPRequestDurationSeconds,
		m.ScyllaQueriesTotal,
		m.ScyllaQueryDurationSeconds,
		m.CircuitBreakerState,
		m.CircuitBreakerTransitions,
		m.BusinessLookupsTotal,
		m.DependencyHealthChecks,
		m.BuildInfo,
	)

	return m
}
