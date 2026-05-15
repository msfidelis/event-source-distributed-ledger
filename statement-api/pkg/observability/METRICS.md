# Statement API — Métricas Prometheus

Endpoint de exposição: `GET /metrics`
Registry: isolado (não usa o registry global do Prometheus).

---

## HTTP

### `statement_api_http_requests_total`

| Campo | Valor |
|-------|-------|
| Tipo | Counter |
| Labels | `method`, `path`, `status_code` |
| Fonte | `pkg/middleware/prometheus.go` |

Conta o total de requisições HTTP recebidas. Incrementado ao final de cada request, após o handler executar.

**Valores de `path`**: sempre usa `c.FullPath()` — ex: `/statements/:account_id`, `/metrics`, `/readyz`. Nunca contém o valor real do parâmetro.

**Exemplo de query SLI de disponibilidade (target 99,9%):**
```promql
sum(rate(statement_api_http_requests_total{status_code!~"5.."}[5m]))
/ sum(rate(statement_api_http_requests_total[5m]))
```

---

### `statement_api_http_request_duration_seconds`

| Campo | Valor |
|-------|-------|
| Tipo | Histogram |
| Labels | `method`, `path` |
| Buckets | 10ms, 25ms, 50ms, 100ms, 250ms, 500ms, 1s, 2.5s, 5s, 10s |
| Fonte | `pkg/middleware/prometheus.go` |

Distribução de latência das requisições HTTP, medida do recebimento do request até o fim do handler.

**Exemplo de query SLI de latência p99 (target < 1s):**
```promql
histogram_quantile(0.99,
  sum(rate(statement_api_http_request_duration_seconds_bucket{path="/statements/:account_id"}[5m])) by (le)
)
```

Séries derivadas geradas automaticamente pelo Prometheus:
- `statement_api_http_request_duration_seconds_bucket{le="<bound>"}`
- `statement_api_http_request_duration_seconds_sum`
- `statement_api_http_request_duration_seconds_count`

---

## MongoDB

### `statement_api_mongodb_operation_duration_seconds`

| Campo | Valor |
|-------|-------|
| Tipo | Histogram |
| Labels | `operation`, `collection`, `result` |
| Buckets | 1ms, 5ms, 10ms, 25ms, 50ms, 100ms, 250ms, 500ms, 1s, 5s, 10s |
| Fonte | `pkg/mongodb/client.go` — `GetStatements` |

Latência de cada operação MongoDB, medida individualmente.

**Valores de `operation`:**
| Valor | Quando |
|-------|--------|
| `count_documents` | `collection.CountDocuments` |
| `find` | `collection.Find` |
| `cursor_decode` | `cursor.All` (apenas em erro) |

**Valores de `result`:** `success` ou `error`.

**Valores de `collection`:** `transactions`.

**Exemplo — detectar qual operação está lenta:**
```promql
histogram_quantile(0.99,
  sum(rate(statement_api_mongodb_operation_duration_seconds_bucket[5m])) by (le, operation)
)
```

---

### `statement_api_mongodb_connection_pool_in_use`

| Campo | Valor |
|-------|-------|
| Tipo | Gauge |
| Labels | nenhum |
| Fonte | `pkg/mongodb/client.go` — `SetPoolMonitor` |

Número de conexões MongoDB atualmente em uso (obtidas do pool, ainda não devolvidas). Incrementado em `GetSucceeded`, decrementado em `ConnectionReturned`.

---

### `statement_api_mongodb_connection_pool_size`

| Campo | Valor |
|-------|-------|
| Tipo | Gauge |
| Labels | nenhum |
| Fonte | `pkg/mongodb/client.go` — `SetPoolMonitor` |

Tamanho atual do pool de conexões MongoDB (conexões existentes). Incrementado em `ConnectionCreated`, decrementado em `ConnectionClosed`.

**Exemplo — alerta de saturação do pool (> 80%):**
```promql
statement_api_mongodb_connection_pool_in_use
/ statement_api_mongodb_connection_pool_size
> 0.8
```

---

## Negócio

### `statement_api_business_transactions_returned_per_query`

| Campo | Valor |
|-------|-------|
| Tipo | Histogram |
| Labels | nenhum |
| Buckets | 0, 1, 5, 10, 25, 50, 100, 250, 500, 1000 |
| Fonte | `pkg/mongodb/client.go` — `GetStatements` |

Número de transações retornadas por consulta bem-sucedida. Permite detectar queries que retornam volumes anormais de dados.

**Exemplo — percentil 95 de transações por query:**
```promql
histogram_quantile(0.95, sum(rate(statement_api_business_transactions_returned_per_query_bucket[5m])) by (le))
```

---

## Runtime Go (collectors automáticos)

Registrados via `collectors.NewGoCollector()` e `collectors.NewProcessCollector()`.

| Prefixo | Descrição |
|---------|-----------|
| `go_goroutines` | Goroutines ativas |
| `go_gc_duration_seconds` | Pausas de GC |
| `go_memstats_alloc_bytes` | Heap alocado |
| `go_memstats_heap_inuse_bytes` | Heap em uso |
| `process_cpu_seconds_total` | CPU consumida pelo processo |
| `process_resident_memory_bytes` | Memória residente (RSS) |
| `process_open_fds` | File descriptors abertos |

---

## Aviso de cardinalidade

> **NUNCA** adicionar `account_id`, `conta_id`, `user_id`, `request_id` ou qualquer identificador de alta cardinalidade como label Prometheus. Com milhares de contas, isso gera milhares de séries temporais e pode causar OOM no Prometheus. O middleware sempre usa `c.FullPath()` — ex: `/statements/:account_id` — e nunca `c.Param("account_id")`.
