# Strategy Doc — Consistência para APIs Financeiras no Monorepo do Ledger

| Campo | Valor |
|---|---|
| Status | Adotado |
| Versão | 1.0 |
| Data | 2026-06-01 |
| Dono | Squad Ledger (revisão trimestral com Squad Plataforma de Dados e SRE) |
| Origem | [RFC-002](../rfcs/RFC-002-inconsistencia-saldo-consultivo-simulacoes.md) → [RFC-006](../rfcs/RFC-006-api-consulta-saldo-transacional.md) (absorveu [RFC-007](../rfcs/RFC-007-api-simulacao-debito-credito-v2.md)) e ADRs 0001–0005 |

## Por que este documento existe

Entre fevereiro e maio de 2026, o time percorreu um ciclo completo — proposta ([RFC-001](../rfcs/RFC-001-api-simulacao-debito-credito.md)), incidente em produção, três alternativas descartadas ([RFC-003](../rfcs/RFC-003-consistencia-forte-scylladb.md), [RFC-004](../rfcs/RFC-004-simulacao-sincrona-embarcada-ledger.md), [RFC-005](../rfcs/RFC-005-cache-aside-saldo-simulacao.md)) e uma solução aceita ([RFC-006](../rfcs/RFC-006-api-consulta-saldo-transacional.md), que absorveu o [RFC-007](../rfcs/RFC-007-api-simulacao-debito-credito-v2.md) na revisão final) — para resolver um problema que, em retrospecto, era sempre o mesmo: **um consumidor usou um read model com o contrato de consistência errado para a decisão que precisava tomar.**

Este documento existe para que a próxima vez que alguém precisar de uma nova API sobre o Ledger, a pergunta "qual fonte de dados eu uso?" tenha uma resposta padrão — sem precisar reviver o Incidente #2026-118 nem redescobrir, por tentativa e erro, por que ScyllaDB-com-mais-consistência e simulação-embarcada-no-Ledger não funcionam.

## Princípios

### 1. O Postgres do Ledger é a única fonte da verdade

O Event Store (`events`) e os read models transacionais (`accounts`, `transactions`) mantidos pelo `ledger` dentro da mesma transação SQL são a única representação autoritativa do estado de uma conta. Qualquer outro read model (ScyllaDB, MongoDB, ou futuros) é uma **projeção derivada**, nunca uma fonte alternativa de verdade.

### 2. Todo read model deve declarar seu contrato de consistência

Nenhuma API que expõe dado do Ledger deve deixar implícito se serve dado eventualmente consistente ou fortemente consistente. Isso deve estar:
- No nome do serviço/endpoint quando possível (ex.: `simulation-api` deixa claro que é consulta e decisão sobre o Ledger, não ao read model consultivo).
- Na documentação da API (OpenAPI/contrato), incluindo a ordem de grandeza esperada de defasagem (ex.: "saldo consultivo: eventualmente consistente, defasagem típica sub-segundo, pode chegar a segundos sob pico").
- Em métricas observáveis (lag de consumer, lag de replicação), não apenas em texto.

| Read model | Contrato | Uso apropriado |
|---|---|---|
| `accounts`/`transactions` (Postgres, dentro do `ledger`) | Forte (fonte da verdade) | Uso interno do `ledger` apenas |
| `simulation-api` (réplica de leitura Postgres) | Forte (via fail-closed sobre lag de replicação) | Decisões financeiras síncronas: simulação, pré-autorização, qualquer "aprova ou recusa agora" |
| `balance-api` / ScyllaDB | Eventual (via LWT, propagado por Kafka) | Consulta de saldo em app, dashboards, qualquer leitura que tolera defasagem de segundos |
| `statement-api` / MongoDB | Eventual (propagado por Kafka) | Extrato histórico — por natureza não representa "agora" |

### 3. Decisão financeira binária (aprova/recusa) exige consistência forte, sem exceção

Qualquer funcionalidade que responde "sim" ou "não" para uma operação financeira — simulação, limite, pré-autorização, bloqueio — deve ler de uma fonte com o contrato do item 2 marcado como "Forte". Reduzir a probabilidade de erro (cache com TTL curto, lag menor) não é equivalente a eliminar a classe de erro, e não deve ser aceito como mitigação para este tipo de caso de uso — ver o racional completo em [ADR-0003](../adrs/ADR-0003-rejeitar-consistencia-forte-scylladb.md) e [RFC-005](../rfcs/RFC-005-cache-aside-saldo-simulacao.md).

### 4. Caminhos de leitura fortemente consistente não compartilham processo com o caminho de escrita crítico

O `ledger` processa comandos (`conta_criada`, `conta_movimentacao`) e é protegido por rate limiting dedicado (Envoy) precisamente porque é o único caminho de escrita do sistema. Nenhuma nova necessidade de leitura — por mais legítima que seja — deve ser resolvida adicionando rotas HTTP síncronas ao processo do `ledger`. Consistência forte para leitura se obtém via réplica dedicada (`simulation-api`), nunca competindo por recursos com a escrita. Ver [ADR-0004](../adrs/ADR-0004-rejeitar-simulacao-embarcada-ledger.md).

### 5. Fail-closed, não fail-open, quando a garantia de consistência não pode ser cumprida

Qualquer API que prometa consistência forte deve monitorar seu próprio sinal de defasagem (lag de replicação, lag de consumer) e recusar servir a resposta (`503`) quando esse sinal ultrapassar o limiar aceitável, em vez de servir um dado potencialmente incorreto. Consumidores dessas APIs devem tratar essa recusa como resposta esperada, não como bug.

### 6. Novos casos de uso ganham serviços novos quando o contrato de consistência muda

Quando uma nova necessidade tem um contrato de consistência diferente de um serviço existente, a resposta padrão é um novo serviço (ou endpoint isolado com fonte de dados própria), não a extensão de um serviço existente para "também" servir o novo contrato. Isso mantém a topologia do sistema como documentação viva de quais garantias cada API oferece — ver [ADR-0005](../adrs/ADR-0005-servico-simulacao-desacoplado.md).

Isso não significa multiplicar serviços por reflexo: o [ADR-0002](../adrs/ADR-0002-api-consulta-saldo-transacional-ledger.md) originalmente propôs dois serviços em cadeia (um só de leitura, outro só de regra de negócio) e os fundiu em um único `simulation-api` na revisão final, por não existir nenhum outro consumidor real para a leitura isolada. O princípio é "um serviço por contrato de consistência", não "um serviço por responsabilidade" — separe quando os contratos divergem, não por antecipação de reuso que ainda não existe.

## Processo de governança (como este documento se conecta a RFCs e ADRs)

1. **RFC** propõe uma mudança ou abre uma investigação. Pode gerar RFCs filhas para alternativas — cada alternativa levada a sério, mesmo se descartada, ganha sua própria RFC, não um parágrafo dentro da RFC principal.
2. **ADR** registra a decisão resultante de cada RFC fechada — inclusive (e especialmente) as decisões de **não fazer** algo. Uma ADR nunca é editada retroativamente para "consertar" uma decisão; uma nova ADR supersede a anterior quando o contexto muda (ver nota de atualização em [ADR-0001](../adrs/ADR-0001-saldo-consultivo-eventualmente-consistente.md), que não reverte a decisão original, apenas explicita seu limite).
3. **Strategy Doc** (este documento) é revisado, não substituído a cada ciclo — consolida princípios extraídos de múltiplas RFCs/ADRs para que a próxima decisão semelhante comece daqui, em vez de do zero.

## Aplicação a futuras APIs

Antes de propor uma nova API sobre dados do Ledger, responda, na própria RFC:

1. Esta API precisa saber o estado "agora" para tomar uma decisão binária, ou tolera saber o estado "há alguns segundos"?
2. Se a resposta for "agora": ela deve consumir `simulation-api` (ou uma capacidade equivalente futura com o mesmo contrato), nunca um read model eventualmente consistente, e nunca uma rota nova no processo do `ledger`.
3. Qual é o comportamento quando a garantia de consistência não pode ser cumprida (fail-closed) — e isso está com testabilidade e alertas definidos antes do rollout, não descoberto em um incidente.

## Anexo — Projeto de Referência: Implementação do `simulation-api`

Esta seção é o projeto de referência que materializa os princípios acima em código. Não é uma RFC (não propõe uma decisão nova) nem uma ADR (não registra uma decisão de arquitetura isolada) — é a implementação concreta que qualquer squad pode consultar como ponto de partida ao construir uma API com contrato de consistência forte sobre o Ledger. Decisões de arquitetura já feitas aqui remetem ao [RFC-006](../rfcs/RFC-006-api-consulta-saldo-transacional.md) e ao [ADR-0002](../adrs/ADR-0002-api-consulta-saldo-transacional-ledger.md).

### A.1 Stack

Mesma stack dos demais serviços do monorepo (`ledger`, `balance-api`), para minimizar custo cognitivo de quem já opera o sistema:

| Camada | Escolha | Motivo |
|---|---|---|
| HTTP | `gin-gonic/gin` v1.11 | Mesmo framework de `balance-api`/`statement-api` |
| Acesso a dados | `uptrace/bun` v1.2 + `jackc/pgx/v5` (via `stdlib`) | Mesmo driver/ORM do `ledger` — nenhum driver Postgres novo entra no monorepo |
| Logs | `rs/zerolog` | Padrão já adotado em todos os serviços |
| Métricas | `prometheus/client_golang` | Padrão já adotado; convenção de `Namespace`/`Subsystem` igual à `balance-api` |
| Circuit breaker | `sony/gobreaker/v2` | Já usado em `balance-api` para proteger chamadas à dependência externa (aqui, a réplica) |
| Testes | `stretchr/testify` + `testcontainers-go` (Postgres) | Unit tests de tabela para a regra de negócio; integration tests contra um Postgres real com uma réplica simulada |

Nenhuma dependência nova de infraestrutura além da própria réplica de leitura Postgres (decidida no RFC-006).

### A.2 Estrutura de diretórios

```
simulation-api/
├── main.go
├── go.mod
├── Dockerfile
├── Dockerfile.dev
├── routes/
│   ├── simulate.go        # POST /simulacoes/debito-credito
│   └── probes.go          # /health, /livez, /readyz
├── internal/
│   └── simulate/
│       └── simulate.go     # regra de negócio pura, sem I/O
└── pkg/
    ├── config/
    │   └── config.go
    ├── logger/
    │   └── logger.go
    ├── middleware/
    │   └── middleware.go    # Prometheus + request logging, iguais aos de balance-api
    ├── observability/
    │   └── metrics.go
    ├── money/                # extraído de ledger/internal/utils/money.go (RFC-006, item 3 do rollout)
    │   └── money.go
    └── replica/
        └── replica.go        # cliente Postgres read-only + checagem de lag
```

`pkg/money` é o pacote compartilhado com o `ledger` mencionado no plano de rollout do RFC-006 — existe fisicamente em ambos os módulos (via replace directive local ou publicado como módulo Go interno) para que a regra de arredondamento nunca divirja entre simulação e execução real.

### A.3 Configuração (variáveis de ambiente)

Segue a mesma convenção `getEnv`/`getEnvAsInt`/`getEnvAsDuration` usada em `ledger/pkg/config` e `balance-api/pkg/config`. Todos os parâmetros de connection pool são externalizados — **nenhum valor de pool é hardcoded no código**, e o pool é dimensionado uma única vez na inicialização, nunca recalculado em runtime:

| Variável | Descrição | Padrão |
|---|---|---|
| `REPLICA_DATABASE_URL` | Connection string da réplica de leitura Postgres (usuário `readonly`) | `postgres://readonly:readonly@replica:5432/eventsourcing?sslmode=disable` |
| `REPLICA_MAX_OPEN_CONNS` | Conexões máximas simultâneas no pool, isolado do pool do `ledger` | `50` |
| `REPLICA_MAX_IDLE_CONNS` | Conexões ociosas mantidas abertas no pool | `10` |
| `REPLICA_CONN_MAX_LIFETIME` | Tempo máximo de vida de uma conexão antes de ser reciclada | `5m` |
| `REPLICA_CONN_MAX_IDLE_TIME` | Tempo máximo que uma conexão pode ficar ociosa antes de ser fechada | `1m` |
| `REPLICA_LAG_THRESHOLD_MS` | Limiar de lag de replicação acima do qual a API falha fechado | `200` |
| `REPLICA_LAG_CHECK_TIMEOUT` | Timeout da query de verificação de lag | `50ms` |
| `REPLICA_QUERY_TIMEOUT` | Timeout da query de saldo | `100ms` |
| `PORT` | Porta HTTP | `8086` |
| `ENVIRONMENT` | `development`/`production` | `development` |
| `LOG_LEVEL` | Nível de log | `info` |

Deliberadamente **não existe** variável de TTL de cache — reforça o princípio 5 (fail-closed) e a rejeição do RFC-005.

`pkg/config/config.go` carrega esses valores uma única vez, no boot do processo, e os repassa como um `ReplicaConfig` imutável para `replica.GetInstance` (ver A.8):

```go
package config

import (
	"os"
	"strconv"
	"time"
)

type Config struct {
	Replica ReplicaConfig
	Server  ServerConfig
	App     AppConfig
}

// ReplicaConfig concentra toda a parametrização do pool de conexões com a
// réplica de leitura — nada relacionado a pool é definido fora daqui.
type ReplicaConfig struct {
	DatabaseURL     string
	MaxOpenConns    int
	MaxIdleConns    int
	ConnMaxLifetime time.Duration
	ConnMaxIdleTime time.Duration
	LagThreshold    time.Duration
	LagCheckTimeout time.Duration
	QueryTimeout    time.Duration
}

type ServerConfig struct {
	Port string
}

type AppConfig struct {
	Environment string
	LogLevel    string
}

func Load() *Config {
	return &Config{
		Replica: ReplicaConfig{
			DatabaseURL:     getEnv("REPLICA_DATABASE_URL", "postgres://readonly:readonly@replica:5432/eventsourcing?sslmode=disable"),
			MaxOpenConns:    getEnvAsInt("REPLICA_MAX_OPEN_CONNS", 50),
			MaxIdleConns:    getEnvAsInt("REPLICA_MAX_IDLE_CONNS", 10),
			ConnMaxLifetime: getEnvAsDuration("REPLICA_CONN_MAX_LIFETIME", 5*time.Minute),
			ConnMaxIdleTime: getEnvAsDuration("REPLICA_CONN_MAX_IDLE_TIME", 1*time.Minute),
			LagThreshold:    getEnvAsDuration("REPLICA_LAG_THRESHOLD_MS", 200*time.Millisecond),
			LagCheckTimeout: getEnvAsDuration("REPLICA_LAG_CHECK_TIMEOUT", 50*time.Millisecond),
			QueryTimeout:    getEnvAsDuration("REPLICA_QUERY_TIMEOUT", 100*time.Millisecond),
		},
		Server: ServerConfig{
			Port: getEnv("PORT", "8086"),
		},
		App: AppConfig{
			Environment: getEnv("ENVIRONMENT", "development"),
			LogLevel:    getEnv("LOG_LEVEL", "info"),
		},
	}
}

func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

func getEnvAsInt(key string, defaultValue int) int {
	if value, err := strconv.Atoi(os.Getenv(key)); err == nil {
		return value
	}
	return defaultValue
}

func getEnvAsDuration(key string, defaultValue time.Duration) time.Duration {
	raw := os.Getenv(key)
	if raw == "" {
		return defaultValue
	}
	// REPLICA_LAG_THRESHOLD_MS é numérico em ms; os demais aceitam sufixo Go (5m, 1m, 50ms)
	if ms, err := strconv.Atoi(raw); err == nil {
		return time.Duration(ms) * time.Millisecond
	}
	if value, err := time.ParseDuration(raw); err == nil {
		return value
	}
	return defaultValue
}
```

### A.4 Fluxo de decisão — requisição de simulação

```mermaid
flowchart TD
    A["POST /simulacoes/debito-credito"] --> B{"Payload válido?<br/>conta_id (UUID), tipo (debito|credito),<br/>valor (> 0)"}
    B -- não --> B1["400 Bad Request<br/>payload_invalido"]
    B -- sim --> C["Verifica lag de replicação<br/>(circuit breaker sobre a réplica)"]
    C --> D{"Réplica alcançável<br/>e lag_ms <= limiar?"}
    D -- não --> D1["503 Service Unavailable<br/>replica_lag_exceeded"]
    D -- sim --> E["SELECT balance, version<br/>FROM accounts WHERE aggregate_id = ?"]
    E --> F{"Conta encontrada?"}
    F -- não --> F1["404 Not Found<br/>account_not_found"]
    F -- sim --> G["balance_after = saldo_atual ± valor<br/>via pkg/money.RoundMoney"]
    G --> H{"tipo = debito<br/>e balance_after < 0?"}
    H -- sim --> H1["status = recusado<br/>motivo_recusa = saldo_insuficiente"]
    H -- não --> H2["status = aprovado"]
    H1 --> I["200 OK"]
    H2 --> I
```

### A.5 Fluxo de decisão — readiness (`/readyz`) vs. fail-closed por requisição

Um ponto de design que costuma ser confundido: **lag alto não deve derrubar o pod inteiro**, só recusar a simulação específica. Se `/readyz` retornasse `503` sempre que o lag estivesse acima do limiar, o Kubernetes removeria o pod do Service assim que qualquer pico de replicação ocorresse — transformando um problema de dado em um problema de disponibilidade, o oposto do que o [ADR-0002](../adrs/ADR-0002-api-consulta-saldo-transacional-ledger.md) pretende.

```mermaid
flowchart TD
    R["GET /readyz"] --> S{"Consegue abrir<br/>conexão com a réplica?"}
    S -- não --> S1["503 — remove o pod do Service<br/>(falha de conectividade real)"]
    S -- sim --> T["200 OK<br/>(lag alto NÃO afeta /readyz)"]

    U["POST /simulacoes/... (por requisição)"] --> V{"lag_ms <= limiar<br/>no momento desta chamada?"}
    V -- não --> V1["503 nesta resposta apenas<br/>pod continua no Service"]
    V -- sim --> V2["segue fluxo normal (A.4)"]
```

`/readyz` responde por conectividade; o fail-closed do princípio 5 é decidido **por requisição**, dentro do handler de simulação — nunca no probe.

### A.6 Contratos e endpoints

| Método | Rota | Descrição |
|---|---|---|
| `POST` | `/simulacoes/debito-credito` | Simula um débito ou crédito; nunca escreve |
| `GET` | `/health` | Liveness simples (processo de pé) |
| `GET` | `/livez` | Alias de liveness, mesmo padrão de `balance-api` |
| `GET` | `/readyz` | Conectividade com a réplica (ver A.5) |
| `GET` | `/metrics` | Exposição Prometheus |

**`POST /simulacoes/debito-credito`**

```json
// Request
{
  "conta_id": "e424ed00-134e-4e92-92c1-40d57a7586c5",
  "tipo": "debito",
  "valor": 250.00
}
```

| Status | Corpo | Quando |
|---|---|---|
| `200` | `{"status":"aprovado","saldo_atual":500.00,"balance_after":250.00}` | Saldo suficiente |
| `200` | `{"status":"recusado","saldo_atual":180.00,"balance_after":-70.00,"motivo_recusa":"saldo_insuficiente"}` | Saldo insuficiente |
| `400` | `{"error":"payload_invalido","detalhe":"valor deve ser > 0"}` | Payload malformado |
| `404` | `{"error":"account_not_found"}` | `conta_id` inexistente na réplica |
| `503` | `{"error":"replica_lag_exceeded","replica_lag_ms":340,"threshold_ms":200}` | Fail-closed (lag ou réplica indisponível) |

Nenhum destes caminhos gera efeito colateral: não publica em `conta_movimentacao`, não persiste eventos, não altera `accounts`.

### A.7 Regra de negócio (`internal/simulate/simulate.go`)

Função pura, sem I/O, testável por tabela sem precisar de banco:

```go
package simulate

import "simulation-api/pkg/money"

type Tipo string

const (
	Debito  Tipo = "debito"
	Credito Tipo = "credito"
)

type Resultado struct {
	Status        string  `json:"status"` // "aprovado" | "recusado"
	SaldoAtual    float64 `json:"saldo_atual"`
	BalanceAfter  float64 `json:"balance_after"`
	MotivoRecusa  string  `json:"motivo_recusa,omitempty"`
}

// Simulate aplica a mesma regra usada pelo ledger ao processar conta_movimentacao:
// crédito soma, débito subtrai, e nenhuma conta pode ficar com saldo negativo.
func Simulate(saldoAtual float64, tipo Tipo, valor float64) Resultado {
	balanceAfter := saldoAtual
	if tipo == Credito {
		balanceAfter += valor
	} else {
		balanceAfter -= valor
	}
	balanceAfter = money.RoundMoney(balanceAfter)

	if tipo == Debito && balanceAfter < 0 {
		return Resultado{
			Status:       "recusado",
			SaldoAtual:   saldoAtual,
			BalanceAfter: balanceAfter,
			MotivoRecusa: "saldo_insuficiente",
		}
	}

	return Resultado{
		Status:       "aprovado",
		SaldoAtual:   saldoAtual,
		BalanceAfter: balanceAfter,
	}
}
```

### A.8 Cliente da réplica (`pkg/replica/replica.go`)

**Especificação:** o client Postgres da réplica é um **singleton por processo**, inicializado via `sync.Once` — mesmo padrão já usado neste monorepo em `ledger/pkg/db` (`GetDBConn`/`GetPGX`/`GetDB`) e em `ledger/pkg/envoyratelimit` (`GetInstance`). Isso garante que, independente de quantos handlers ou goroutines chamem `replica.GetInstance`, apenas um pool de conexões é aberto durante todo o ciclo de vida do serviço — nunca um pool por requisição, nunca um pool por handler. Todos os parâmetros de dimensionamento do pool (máximo de conexões, ociosas, tempo de vida) vêm de `config.ReplicaConfig` (A.3), lido uma única vez do ambiente; o código do client nunca hardcoda esses valores.

```go
package replica

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
	"time"

	"simulation-api/pkg/config"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/uptrace/bun"
	"github.com/uptrace/bun/dialect/pgdialect"
)

type Client struct {
	db           *bun.DB
	lagThreshold time.Duration
}

type Account struct {
	AggregateID uuid.UUID `bun:"aggregate_id,pk"`
	Balance     float64   `bun:"balance"`
	Version     int       `bun:"version"`
}

var (
	once     sync.Once
	instance *Client
	initErr  error
)

var ErrLagExceeded = fmt.Errorf("replica_lag_exceeded")
var ErrNotFound = fmt.Errorf("account_not_found")

// GetInstance retorna o client singleton da réplica. A conexão é aberta uma
// única vez por processo; chamadas subsequentes reaproveitam a mesma instância
// e o mesmo pool, não importa quantos handlers a invoquem concorrentemente.
func GetInstance(cfg config.ReplicaConfig) (*Client, error) {
	once.Do(func() {
		instance, initErr = newClient(cfg)
	})
	return instance, initErr
}

// newClient é privado — só é alcançável através de GetInstance, para que não
// seja possível, por engano (ex.: em um novo handler), abrir um segundo pool
// de conexões com a réplica dentro do mesmo processo.
func newClient(cfg config.ReplicaConfig) (*Client, error) {
	pgxCfg, err := pgx.ParseConfig(cfg.DatabaseURL)
	if err != nil {
		return nil, fmt.Errorf("erro ao parsear REPLICA_DATABASE_URL: %w", err)
	}

	sqldb := stdlib.OpenDB(*pgxCfg)

	// Parametrização do pool inteiramente externalizada — ver A.3.
	sqldb.SetMaxOpenConns(cfg.MaxOpenConns)
	sqldb.SetMaxIdleConns(cfg.MaxIdleConns)
	sqldb.SetConnMaxLifetime(cfg.ConnMaxLifetime)
	sqldb.SetConnMaxIdleTime(cfg.ConnMaxIdleTime)

	db := bun.NewDB(sqldb, pgdialect.New())

	return &Client{
		db:           db,
		lagThreshold: cfg.LagThreshold,
	}, nil
}

// ReplicationLag consulta o quão atrás esta réplica está do primário.
// pg_last_xact_replay_timestamp() retorna o timestamp do último WAL replicado;
// a diferença para now() é o lag efetivo percebido por esta réplica.
func (c *Client) ReplicationLag(ctx context.Context) (time.Duration, error) {
	var lagSeconds float64
	err := c.db.NewSelect().
		ColumnExpr("EXTRACT(EPOCH FROM (now() - pg_last_xact_replay_timestamp()))").
		Scan(ctx, &lagSeconds)
	if err != nil {
		return 0, err
	}
	return time.Duration(lagSeconds * float64(time.Second)), nil
}

// GetBalance falha fechado: nunca retorna um saldo se o lag estiver acima do limiar.
func (c *Client) GetBalance(ctx context.Context, accountID uuid.UUID) (Account, error) {
	lag, err := c.ReplicationLag(ctx)
	if err != nil {
		return Account{}, err
	}
	if lag > c.lagThreshold {
		return Account{}, ErrLagExceeded
	}

	var acc Account
	err = c.db.NewSelect().
		Model(&acc).
		Where("aggregate_id = ?", accountID).
		Scan(ctx)
	if err == sql.ErrNoRows {
		return Account{}, ErrNotFound
	}
	return acc, err
}
```

Em `main.go`, o singleton é obtido uma única vez no boot e injetado nos handlers — nenhum handler chama `newClient` diretamente nem recebe uma connection string para abrir sua própria conexão:

```go
cfg := config.Load()

replicaClient, err := replica.GetInstance(cfg.Replica)
if err != nil {
	log.Fatal().Err(err).Msg("erro ao inicializar client da réplica")
}

simulationHandler := routes.NewSimulationHandler(replicaClient)
```

### A.9 Handler HTTP (`routes/simulate.go`)

```go
package routes

import (
	"errors"
	"net/http"

	"simulation-api/internal/simulate"
	"simulation-api/pkg/logger"
	"simulation-api/pkg/observability"
	"simulation-api/pkg/replica"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/rs/zerolog"
)

type SimulateRequest struct {
	ContaID uuid.UUID     `json:"conta_id" binding:"required"`
	Tipo    simulate.Tipo `json:"tipo" binding:"required,oneof=debito credito"`
	Valor   float64       `json:"valor" binding:"required,gt=0"`
}

type SimulationHandler struct {
	replicaClient *replica.Client
	log           zerolog.Logger
}

func NewSimulationHandler(r *replica.Client) *SimulationHandler {
	return &SimulationHandler{replicaClient: r, log: logger.Instance()}
}

func (h *SimulationHandler) Simulate(c *gin.Context) {
	var req SimulateRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "payload_invalido", "detalhe": err.Error()})
		return
	}

	acc, err := h.replicaClient.GetBalance(c.Request.Context(), req.ContaID)
	switch {
	case errors.Is(err, replica.ErrLagExceeded):
		observability.SimulationsTotal.WithLabelValues(string(req.Tipo), "fail_closed_lag").Inc()
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "replica_lag_exceeded"})
		return
	case errors.Is(err, replica.ErrNotFound):
		observability.SimulationsTotal.WithLabelValues(string(req.Tipo), "not_found").Inc()
		c.JSON(http.StatusNotFound, gin.H{"error": "account_not_found"})
		return
	case err != nil:
		observability.SimulationsTotal.WithLabelValues(string(req.Tipo), "error").Inc()
		h.log.Error().Err(err).Str("conta_id", req.ContaID.String()).Msg("erro ao consultar réplica")
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "replica_lag_exceeded"})
		return
	}

	result := simulate.Simulate(acc.Balance, req.Tipo, req.Valor)
	observability.SimulationsTotal.WithLabelValues(string(req.Tipo), result.Status).Inc()
	c.JSON(http.StatusOK, result)
}
```

### A.10 Observabilidade — catálogo de métricas Prometheus

Mesma convenção `Namespace`/`Subsystem` de `balance-api/pkg/observability`: `Namespace: "simulation_api"`, um `Subsystem` por área (`http`, `business`, `replica`, `circuit_breaker`, `dependency`). Nenhuma métrica leva `conta_id` como label — alta cardinalidade, mesmo cuidado já documentado em `balance-api` (`BusinessLookupsTotal`).

#### A.10.1 Catálogo completo

| Métrica | Tipo | Labels | Subsystem | O que responde |
|---|---|---|---|---|
| `simulation_api_http_requests_total` | Counter | `method`, `route`, `status_code` | `http` | Quantas requisições, e com que status, por rota |
| `simulation_api_http_request_duration_seconds` | Histogram | `method`, `route` | `http` | Latência HTTP fim-a-fim |
| `simulation_api_business_simulations_total` | Counter | `result` (`aprovado`\|`recusado`\|`not_found`\|`fail_closed_lag`\|`error`) | `business` | Volume e distribuição de resultado das simulações — a métrica central do serviço |
| `simulation_api_business_simulations_total` por `tipo` | Counter | `tipo` (`debito`\|`credito`), `result` | `business` | Mesma contagem, segmentada por tipo de movimentação simulada |
| `simulation_api_replica_lag_seconds` | Gauge | — | `replica` | Lag de replicação na última verificação — o sinal que decide fail-closed |
| `simulation_api_replica_query_duration_seconds` | Histogram | `operation` (`lag_check`\|`get_balance`) | `replica` | Latência de cada tipo de query contra a réplica |
| `simulation_api_replica_queries_total` | Counter | `operation`, `result` (`ok`\|`timeout`\|`error`) | `replica` | Taxa de erro/timeout na réplica, independente do lag |
| `simulation_api_circuit_breaker_state` | Gauge | `name` (`replica`) | `circuit_breaker` | Estado do breaker que protege a réplica: 0=closed, 1=half-open, 2=open |
| `simulation_api_circuit_breaker_transitions_total` | Counter | `name`, `from`, `to` | `circuit_breaker` | Quantas vezes o breaker abriu/fechou — sinal de instabilidade recorrente |
| `simulation_api_dependency_health_checks_total` | Counter | `dependency` (`replica`), `result` (`ok`\|`error`) | `dependency` | Resultado de cada checagem feita pelo `/readyz` (conectividade, não lag — ver A.5) |
| `simulation_api_build_info` | Gauge | `version`, `service` | — | Versão em execução, para correlacionar incidentes com deploys |

#### A.10.2 Fechando o loop com o `ledger` (a métrica que faltava)

Nenhuma das métricas acima, sozinha, mede o que o [RFC-002](../rfcs/RFC-002-inconsistencia-saldo-consultivo-simulacoes.md) definiu como critério de sucesso: **taxa de simulações aprovadas que depois são recusadas na movimentação real** (meta: < 0,2%). Essa taxa só pode ser calculada correlacionando uma resposta do `simulation-api` com o resultado real, minutos depois, no `ledger` — os dois serviços não compartilham processo nem banco.

Para fechar esse loop sem acoplar os serviços:

1. `simulation-api` retorna um `simulation_id` (UUID) em toda resposta de aprovação.
2. O canal, ao efetivar a movimentação real, propaga esse `simulation_id` no `metadata` do evento `conta_movimentacao` (campo já existente no envelope, ver `ledger/pkg/events`).
3. O `ledger`, ao processar a movimentação, emite uma métrica dedicada quando `metadata.simulation_id` está presente e o resultado diverge do que a simulação prometeu:

```go
// ledger/pkg/metrics — nova métrica, mesma convenção das existentes em ledger_*
SimulationMismatchTotal = promauto.NewCounterVec(
    prometheus.CounterOpts{
        Name: "ledger_transactions_simulation_mismatch_total",
        Help: "Total de movimentações cujo resultado divergiu da simulação prévia (simulation_id presente no metadata).",
    },
    []string{"simulation_result", "actual_result"},
)
```

A taxa de sucesso do RFC-002 passa a ser uma expressão PromQL direta, não uma auditoria manual pós-incidente:

```promql
sum(rate(ledger_transactions_simulation_mismatch_total{simulation_result="aprovado", actual_result="recusado"}[30d]))
/
sum(rate(simulation_api_business_simulations_total{result="aprovado"}[30d]))
```

Sem essa correlação, o critério de sucesso definido na RFC permanece uma promessa não observável — exatamente o vácuo que permitiu o Incidente #2026-118 passar despercebido até reclamação de cliente.

#### A.10.3 Definição das métricas (`pkg/observability/metrics.go`)

```go
package observability

import "github.com/prometheus/client_golang/prometheus"

var HTTPBuckets = []float64{0.005, 0.01, 0.025, 0.05, 0.1, 0.2, 0.3, 0.5, 0.75, 1.0}
var ReplicaBuckets = []float64{0.001, 0.002, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5}

var (
	HTTPRequestsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "simulation_api",
			Subsystem: "http",
			Name:      "requests_total",
			Help:      "Total de requisições HTTP, por método, rota e status_code.",
		},
		[]string{"method", "route", "status_code"},
	)

	HTTPRequestDurationSeconds = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "simulation_api",
			Subsystem: "http",
			Name:      "request_duration_seconds",
			Help:      "Distribuição de latência das requisições HTTP.",
			Buckets:   HTTPBuckets,
		},
		[]string{"method", "route"},
	)

	SimulationsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "simulation_api",
			Subsystem: "business",
			Name:      "simulations_total",
			Help:      "Total de simulações, por tipo e resultado: aprovado, recusado, not_found, fail_closed_lag, error.",
		},
		[]string{"tipo", "result"},
	)

	ReplicaLagSeconds = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "simulation_api",
			Subsystem: "replica",
			Name:      "lag_seconds",
			Help:      "Lag de replicação observado na última verificação.",
		},
		[]string{},
	)

	ReplicaQueryDurationSeconds = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "simulation_api",
			Subsystem: "replica",
			Name:      "query_duration_seconds",
			Help:      "Latência das queries contra a réplica.",
			Buckets:   ReplicaBuckets,
		},
		[]string{"operation"},
	)

	ReplicaQueriesTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "simulation_api",
			Subsystem: "replica",
			Name:      "queries_total",
			Help:      "Total de queries executadas na réplica, por operação e resultado.",
		},
		[]string{"operation", "result"},
	)

	CircuitBreakerState = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "simulation_api",
			Subsystem: "circuit_breaker",
			Name:      "state",
			Help:      "Estado atual do circuit breaker: 0=closed, 1=half-open, 2=open.",
		},
		[]string{"name"},
	)

	CircuitBreakerTransitions = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "simulation_api",
			Subsystem: "circuit_breaker",
			Name:      "transitions_total",
			Help:      "Total de transições de estado do circuit breaker.",
		},
		[]string{"name", "from", "to"},
	)

	DependencyHealthChecks = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "simulation_api",
			Subsystem: "dependency",
			Name:      "health_checks_total",
			Help:      "Total de verificações de saúde de dependências externas (readyz).",
		},
		[]string{"dependency", "result"},
	)

	BuildInfo = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "simulation_api",
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
		SimulationsTotal,
		ReplicaLagSeconds,
		ReplicaQueryDurationSeconds,
		ReplicaQueriesTotal,
		CircuitBreakerState,
		CircuitBreakerTransitions,
		DependencyHealthChecks,
		BuildInfo,
	)
}
```

#### A.10.4 Alertas recomendados

| Alerta | Expressão PromQL | Severidade | Ação |
|---|---|---|---|
| Fail-closed sustentado por lag | `rate(simulation_api_business_simulations_total{result="fail_closed_lag"}[5m]) / rate(simulation_api_business_simulations_total[5m]) > 0.01` | Crítico | Réplica se distanciando do primário — investigar replicação Postgres antes que vire indisponibilidade percebida pelo canal |
| Lag em tendência de alta | `simulation_api_replica_lag_seconds > 0.15` por 3 minutos | Aviso | Antecipa o alerta crítico acima; ainda dentro do limiar (200ms) mas em trajetória de risco |
| Circuit breaker aberto | `simulation_api_circuit_breaker_state{name="replica"} == 2` | Crítico | Réplica considerada indisponível pelo breaker — toda simulação está em fail-closed |
| Divergência simulação vs. real acima da meta | expressão de A.10.2 > 0,2% em janela de 24h | Crítico | Viola diretamente o critério de sucesso do RFC-002 — reabre a investigação, não é tolerável como ruído |
| Latência HTTP p99 acima do SLA | `histogram_quantile(0.99, rate(simulation_api_http_request_duration_seconds_bucket[5m])) > 0.1` | Aviso | Compara com a meta de latência do RFC-006 (endpoint < 100ms) |
| Readiness falhando | `rate(simulation_api_dependency_health_checks_total{dependency="replica", result="error"}[5m]) > 0` | Crítico | Falha de conectividade real com a réplica — distinto de lag alto (ver A.5) |

### A.11 Exposição via Kong

```yaml
- name: simulation-service
  url: http://simulation-api:8086/simulacoes
  routes:
    - name: simulation-route
      paths:
        - /api/v1/simulacoes
      strip_path: true
      path_handling: v1
```

Rota nova em `kong/kong.yml`, isolada da `balance-service` já existente, com plugin de rate limiting próprio (Envoy) dimensionado para o volume mais alto e mais elástico da simulação (RFC-006, seção "Escopo explícito").

### A.12 Estratégia de testes

| Tipo | Alvo | Ferramenta |
|---|---|---|
| Unitário (tabela) | `internal/simulate.Simulate` — crédito, débito com saldo suficiente/insuficiente, valores fracionários, arredondamento | `testing` + `testify/assert` |
| Integração | `pkg/replica.Client` contra Postgres real com réplica simulada (`testcontainers-go`), incluindo cenário de lag artificial (`pg_wal_replay_pause`) | `testcontainers-go` |
| Contrato | Shadow mode comparando `simulation-api` com o antigo `balance-api /balance/simulate` durante a semana de transição (RFC-006, plano de rollout) | script de replay de tráfego + diff de resultado |
| Carga | Reproduzir o volume do Incidente #2026-118 usando o `simulador` em modo contínuo apontando para o ambiente de staging | `simulador` (já existente no monorepo) |

## Revisão

Este documento deve ser revisitado a cada trimestre, ou imediatamente após qualquer incidente relacionado a consistência de dados financeiros, seguindo o mesmo padrão do Incidente #2026-118: abrir uma RFC de investigação, registrar as alternativas descartadas com a mesma seriedade da aceita, e atualizar este documento se um novo princípio emergir.
