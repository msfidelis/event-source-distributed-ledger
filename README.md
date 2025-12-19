# Ledger Distribuído — Fluxo Transacional

Este documento descreve o fluxo transacional do sistema Ledger (event sourcing), todas as operações transacionais, o fluxo entre tabelas e tópicos Kafka, e como os read models são criados e mantidos.

Sumário
- Visão geral arquitetural
- Fluxos transacionais (criação de conta, movimentação)
- Tabelas principais (PostgreSQL)
- Tópicos Kafka (entrada/saída)
- Read models e onde ficam (ScyllaDB, MongoDB)
- Garantias (atomicidade, idempotência, ordenação)
- Exemplos de queries e comandos

---

Observações:
- O Ledger persiste eventos no Event Store (`events`), atualiza read models transacionais (`accounts`, `transactions`) dentro de transação SQL e só publica os tópicos após commit bem-sucedido.
- `ledger_saldo_atualizado` é consumido pelo Balance ingestion que usa LWT em Scylla para aplicar apenas versões maiores.
- `ledger_nova_transacao_confirmada` é consumido pelo Statement ingestion que insere documentos no MongoDB usando `movimentacao_id` como `_id` para idempotência.


---

## 1. Visão geral arquitetural

Componentes principais:
- Ledger (serviço que processa comandos e persiste eventos no Event Store/Postgres)
- Kafka (broker de mensagens para integração assíncrona)
- Balance-ingestion → ScyllaDB (read model de saldos)
- Statement-ingestion → MongoDB (read model de extratos)
- Balance API (consulta de saldo)
- Statement API (consulta de extrato)

Fluxo de alto nível:
1. Cliente/Simulador envia comando (via tópico `conta_criada` ou `conta_movimentacao`).
2. Ledger consome comando, valida, persiste evento no Event Store (`events`), aplica mudança no modelo transacional (`accounts`, `transactions`) dentro de uma transação SQL.
3. Após commit, Ledger publica confirmações em tópicos Kafka: `ledger_nova_conta_registrada`, `ledger_nova_transacao_confirmada`, `ledger_saldo_atualizado`.
4. Serviços de ingestão (balance, statements) consomem tópicos e atualizam seus read models (ScyllaDB / MongoDB).
5. APIs de consulta leem diretamente os read models para servir consultas rápidas.

---

## 2. Fluxos transacionais detalhados

### Arquitetura do Ledger (Ingestão, Calculo e Dispatch)

<img width="1006" height="588" alt="event-bus drawio(2)" src="https://github.com/user-attachments/assets/5182c305-5b19-4348-9764-7d1d6a582795" />


### Fluxo Transacional Completo 

```mermaid
sequenceDiagram
  autonumber
  participant Client
  participant Kafka
  participant Ledger
  participant Postgres
  participant KafkaOut as Kafka
  participant StatementIngest as Statement Ingestion
  participant BalanceIngest as Balance Ingestion

  Client->>Kafka: conta_movimentacao { aggregate_id, movimentacao_id, tipo, valor, descricao }
  Kafka->>Ledger: entrega mensagem

  Ledger->>Postgres: BEGIN
  Ledger->>Postgres: append_event(event_type=AccountTransaction, event_data, p_expected_version)
  Postgres-->>Ledger: event persisted (v+1)

  Ledger->>Postgres: SELECT balance FROM accounts WHERE aggregate_id
  Postgres-->>Ledger: current_balance

  Ledger->>Ledger: calcular new_balance
  alt new_balance < 0
    Ledger->>Postgres: ROLLBACK
    Ledger->>KafkaOut: public error/compensation? (optional)
  else
    Ledger->>Postgres: UPDATE accounts SET balance=new_balance
    Ledger->>Postgres: INSERT INTO transactions (id=movimentacao_id, account_id, amount, balance_after, ...)
    Postgres-->>Ledger: OK
    Ledger->>Postgres: COMMIT

    Ledger->>KafkaOut: ledger_nova_transacao_confirmada (detailed)
    Ledger->>KafkaOut: ledger_saldo_atualizado {conta_id, balance, version}

    KafkaOut->>StatementIngest: entrega ledger_nova_transacao_confirmada
    KafkaOut->>BalanceIngest: entrega ledger_saldo_atualizado

    StatementIngest->>Mongo: insert document (_id=movimentacao_id)
    BalanceIngest->>Scylla: INSERT IF NOT EXISTS / UPDATE IF version < ?
  end
```

### 2.1 Criação de Conta
Tópico de entrada: `conta_criada`

Passos:
1. Ledger consome mensagem `conta_criada`.
2. Inicia transação SQL (BEGIN).
3. Persiste evento no `events` (append-only): `event_type = 'AccountCreated'`, `event_data` contém payload.
4. Insere registro na tabela `accounts` (read model) com saldo inicial (normalmente 0).
5. Commit da transação.
6. Após commit, publica em Kafka:
   - `ledger_nova_conta_registrada` (informação para downstream)
   - `ledger_saldo_atualizado` (saldo inicial) — consumido pelo Balance ingestion

Garantias:
- Se ocorrer erro durante validação/insert, realiza rollback e não publica tópicos.
- Ordem do agregado é garantida via `version` no Event Store.

### 2.2 Movimentação (Débito / Crédito)
Tópico de entrada: `conta_movimentacao`

Passos (transação atômica):
1. Ledger consome mensagem de movimentação (envelope com metadata e payload).
2. Inicia transação SQL (BEGIN).
3. Persiste o evento no `events` (append-only) usando `version` para optimistic locking.
4. Lê saldo atual da tabela `accounts`.
5. Calcula `new_balance` (crédito aumenta, débito diminui).
6. Valida regras de negócio (ex.: saldo não pode ficar negativo) — se falhar, rollback e possivelmente publicar erro/compensação.
7. Atualiza `accounts.balance` e `accounts.updated_at`.
8. Insere registro em `transactions` (read model) com `id = movimentacao_id`.
9. Commit da transação.
10. Após commit, publica em Kafka:
    - `ledger_nova_transacao_confirmada` (payload detalhado da transação) — consumido pelo Statement ingestion
    - `ledger_saldo_atualizado` (saldo atualizado) — consumido pelo Balance ingestion

Pontos importantes:
- `saveEvent`, `processTransaction` e publicação de confirmação devem ocorrer de forma que a persistência e o endereço de publicação respeitem atomicidade: os writes no banco são commitados antes de publicar a confirmação.
- A publicação nos tópicos é feita somente após commit bem-sucedido.
- Para idempotência, `movimentacao_id` é usado (e.g., como `_id` no MongoDB) para evitar processamento duplicado.

---

## 3. Tabelas principais (PostgreSQL)

### `events` (Event Store)
- `id` BIGSERIAL PRIMARY KEY
- `aggregate_id` UUID NOT NULL
- `aggregate_type` VARCHAR(50)
- `event_type` VARCHAR(100)
- `event_data` JSONB
- `metadata` JSONB
- `version` INT NOT NULL
- `occurred_at` TIMESTAMPTZ NOT NULL

Constraints / índices:
- UNIQUE(aggregate_id, version) — garante ordem e evita versões duplicadas
- Índices em (`aggregate_id, version`), `event_type`, `occurred_at`

Uso:
- Fonte de verdade (source of truth). Todos os eventos são gravados aqui.
- Permite reconstrução de estado (replay) e snapshotting.

### `accounts` (read model)
- `aggregate_id` UUID PRIMARY KEY
- `owner_name` VARCHAR
- `balance` DECIMAL(15,2)
- `status` VARCHAR
- `created_at` TIMESTAMPTZ
- `updated_at` TIMESTAMPTZ

Uso:
- Modelo materializado para consultas de conta/saldo (operacional).
- Atualizado atomically durante processamento de movimentações.

### `transactions` (read model)
- `id` UUID PRIMARY KEY (movimentacao_id)
- `account_id` UUID REFERENCES accounts(aggregate_id)
- `transaction_type` VARCHAR(20)
- `amount` DECIMAL(15,2)
- `balance_after` DECIMAL(15,2)
- `description` VARCHAR
- `occurred_at` TIMESTAMPTZ
- `created_at` TIMESTAMPTZ

Índices:
- `idx_transactions_account` ON (account_id, occurred_at DESC)
- `idx_transactions_occurred` ON (occurred_at DESC)

Uso:
- Histórico transacional para auditoria e consultas cronológicas.

---

## 4. Tópicos Kafka

Tópicos de entrada (para o Ledger):
- `conta_criada` — comandos para criação de conta
- `conta_movimentacao` — comandos de débito/crédito

Tópicos publicados pelo Ledger (consumidos por read models):
- `ledger_nova_conta_registrada` — confirmação da criação de conta
- `ledger_nova_transacao_confirmada` — confirmação de processamento de transação + payload detalhado
- `ledger_saldo_atualizado` — atualização de saldo (conta_id, balance, version, timestamp)

Consumo downstream:
- Balance ingestion consome `ledger_saldo_atualizado` e atualiza ScyllaDB
- Statement ingestion consome `ledger_nova_transacao_confirmada` e insere documentos no MongoDB

---

## 5. Read models e patterns de ingestão

### Balance (ScyllaDB)

<img width="1036" height="663" alt="ledger-Saldo" src="https://github.com/user-attachments/assets/f0562e44-7e90-4349-8e31-d318605a07aa" />


- Keyspace: `balance_ks`
- Table: `balances` (id UUID PRIMARY KEY, balance DOUBLE, version INT)

Ingestão:
- Consome `ledger_saldo_atualizado`
- Estratégia: `INSERT IF NOT EXISTS` e `UPDATE ... IF version < ?` (LWT)
- Motivo: mensagens podem chegar fora de ordem; LWT garante apenas aplicar versões maiores
- Tradeoff: LWT tem custo de performance (Paxos), mas garante ordenação e consistência para esse read model

### Statement (MongoDB)

<img width="1036" height="663" alt="ledger-Extrato" src="https://github.com/user-attachments/assets/301a13ed-0b7b-4d29-ad10-8c93f851781e" />

- Database: `extrato`
- Collection: `transactions`
- Documento por transação; `_id = movimentacao_id` (UUID string)
- Índices: `{conta_id:1, ocorrido_em:-1}` e `_id` (padrão)

Ingestão:
- Consome `ledger_nova_transacao_confirmada`
- Insere documento (se duplicado — erro 11000 — ignore): idempotência

---

## 6. Garantias do sistema

- Atomicidade: operações críticas (salvar evento + atualizar read models transacionais) são realizadas em transação no Ledger (Postgres). A publicação em Kafka é feita após commit.
- Consistência/Eventual Consistency: Ledger (Postgres) é fonte de verdade. Read models são eventualmente consistentes e atualizados por consumers.
- Ordenação: `version` por agregado no Event Store e LWT em Scylla para garantir aplicação apenas de versões maiores.
- Idempotência: `movimentacao_id` como identificador único nos read models evita duplicação.

---

## 7. Exemplos de operations e queries

Inserção de evento (conceitual):
```sql
SELECT append_event(p_aggregate_id, p_aggregate_type, p_event_type, p_event_data::jsonb, p_metadata::jsonb, p_expected_version);
```

Query read model: saldo (Postgres `accounts`):
```sql
SELECT balance, owner_name FROM accounts WHERE aggregate_id = '...';
```

Query extrato (Postgres `transactions`):
```sql
SELECT * FROM transactions WHERE account_id = '...' ORDER BY occurred_at DESC LIMIT 100;
```

Query extrato (MongoDB):
```js
db.transactions.find({ conta_id: "..." }).sort({ocorrido_em:-1}).limit(100)
```

Query balance (Scylla via cqlsh):
```cql
SELECT balance, version FROM balance_ks.balances WHERE id = e424ed00-134e-4e92-92c1-40d57a7586c5;
```

---

## 8. Configuração de Variáveis de Ambiente

Todos os serviços suportam configuração via variáveis de ambiente para facilitar deploy em diferentes ambientes.

### 8.1 Ledger Service

Serviço principal de event sourcing que processa comandos e mantém o Event Store.

| Variável | Descrição | Padrão | Obrigatório |
|----------|-----------|--------|-------------|
| **Database** |
| `DATABASE_URL` | URL completa de conexão PostgreSQL | `postgres://postgres:postgres@localhost:5432/eventsourcing?sslmode=disable` | Não |
| `DATABASE_USER` | Usuário do PostgreSQL | `postgres` | Não |
| `DATABASE_PASSWORD` | Senha do PostgreSQL | `postgres` | Não |
| `DATABASE_HOST` | Host do PostgreSQL | `localhost` | Não |
| `DATABASE_PORT` | Porta do PostgreSQL | `5432` | Não |
| `DATABASE_DB` | Nome do database | `eventsourcing` | Não |
| `DATABASE_SSL_MODE` | Modo SSL (disable, require, verify-ca, verify-full) | `disable` | Não |
| **Kafka Brokers** |
| `KAFKA_BROKERS` | Lista de brokers separados por vírgula | `localhost:9092` | Não |
| **Kafka Consumer Topics** |
| `KAFKA_TOPIC_CONTA_CRIADA` | Tópico de entrada para criação de contas | `conta_criada` | Não |
| `KAFKA_TOPIC_CONTA_MOVIMENTACAO` | Tópico de entrada para movimentações | `conta_movimentacao` | Não |
| `KAFKA_TOPIC_REHYDRATATE` | Tópico para reprocessamento de eventos | `ledger_rehydratate_transactions` | Não |
| **Kafka Producer Topics** |
| `KAFKA_TOPIC_NOVA_CONTA_REGISTRADA` | Tópico de saída para contas criadas | `ledger_nova_conta_registrada` | Não |
| `KAFKA_TOPIC_NOVA_TRANSACAO_CONFIRMADA` | Tópico de saída para transações confirmadas | `ledger_nova_transacao_confirmada` | Não |
| `KAFKA_TOPIC_SALDO_ATUALIZADO` | Tópico de saída para atualizações de saldo | `ledger_saldo_atualizado` | Não |
| `KAFKA_TOPIC_TRANSACAO_RATE_LIMITED` | Tópico de saída para transações bloqueadas por rate limit | `ledger_transacao_ratelimited` | Não |
| **Kafka Consumer Groups** |
| `KAFKA_GROUP_ACCOUNT` | Consumer group para criação de contas | `ledger-account-group` | Não |
| `KAFKA_GROUP_TRANSACTION` | Consumer group para movimentações | `ledger-transaction-group` | Não |
| `KAFKA_GROUP_REHYDRATE` | Consumer group para reprocessamento | `ledger-rehydrate-group` | Não |
| **Application** |
| `PORT` | Porta do servidor HTTP | `8081` | Não |
| `ENVIRONMENT` | Ambiente de execução (development, production) | `development` | Não |
| `LOG_LEVEL` | Nível de log (debug, info, warn, error) | `info` | Não |
| **Rate Limiting** |
| `RATELIMIT_SERVICE` | Host do Envoy Rate Limiter (gRPC) | `ratelimit:8080` | Não |

### 8.2 Balance Ingestion Service

Serviço que consome eventos de saldo e atualiza o read model no ScyllaDB.

| Variável | Descrição | Padrão | Obrigatório |
|----------|-----------|--------|-------------|
| **ScyllaDB** |
| `SCYLLA_HOSTS` | Lista de hosts ScyllaDB separados por vírgula | `localhost:9042` | Não |
| `SCYLLA_KEYSPACE` | Keyspace do ScyllaDB | `balance` | Não |
| `SCYLLA_USERNAME` | Usuário do ScyllaDB | `` | Não |
| `SCYLLA_PASSWORD` | Senha do ScyllaDB | `` | Não |
| `SCYLLA_PORT` | Porta do ScyllaDB | `9042` | Não |
| **Kafka** |
| `KAFKA_BROKERS` | Lista de brokers separados por vírgula | `localhost:9092` | Não |
| `KAFKA_TOPIC_SALDO_ATUALIZADO` | Tópico de entrada para saldos | `ledger_saldo_atualizado` | Não |
| `KAFKA_TOPIC_NOVA_CONTA_REGISTRADA` | Tópico de entrada para contas | `ledger_nova_conta_registrada` | Não |
| `KAFKA_GROUP_BALANCE` | Consumer group para saldos | `balance-ingestion-group` | Não |
| `KAFKA_GROUP_ACCOUNTS` | Consumer group para contas | `balance-accounts-group` | Não |
| **Application** |
| `PORT` | Porta do servidor HTTP | `8082` | Não |
| `ENVIRONMENT` | Ambiente de execução | `development` | Não |
| `LOG_LEVEL` | Nível de log | `info` | Não |

### 8.3 Balance API Service

API REST para consulta de saldos no ScyllaDB.

| Variável | Descrição | Padrão | Obrigatório |
|----------|-----------|--------|-------------|
| **ScyllaDB** |
| `SCYLLA_HOSTS` | Lista de hosts ScyllaDB separados por vírgula | `localhost:9042` | Não |
| `SCYLLA_KEYSPACE` | Keyspace do ScyllaDB | `balance_ks` | Não |
| `SCYLLA_USERNAME` | Usuário do ScyllaDB | `` | Não |
| `SCYLLA_PASSWORD` | Senha do ScyllaDB | `` | Não |
| `SCYLLA_PORT` | Porta do ScyllaDB | `9042` | Não |
| `SCYLLA_CONSISTENCY` | Nível de consistência (QUORUM, ONE, ALL, LOCAL_QUORUM) | `QUORUM` | Não |
| `SCYLLA_TIMEOUT` | Timeout para queries | `10s` | Não |
| `SCYLLA_CONNECT_TIMEOUT` | Timeout para conexão | `10s` | Não |
| `SCYLLA_PROTO_VERSION` | Versão do protocolo CQL | `4` | Não |
| `SCYLLA_RETRY_ATTEMPTS` | Tentativas de retry | `3` | Não |
| **Server** |
| `PORT` | Porta do servidor HTTP | `8083` | Não |
| `SERVER_READ_TIMEOUT` | Timeout de leitura HTTP | `30s` | Não |
| `SERVER_WRITE_TIMEOUT` | Timeout de escrita HTTP | `30s` | Não |
| **Application** |
| `ENVIRONMENT` | Ambiente de execução | `development` | Não |
| `LOG_LEVEL` | Nível de log | `info` | Não |

### 8.4 Statement Ingestion Service

Serviço que consome eventos de transações e atualiza o read model no MongoDB.

| Variável | Descrição | Padrão | Obrigatório |
|----------|-----------|--------|-------------|
| **MongoDB** |
| `MONGODB_HOSTS` | Lista de hosts MongoDB separados por vírgula | `localhost:27017` | Não |
| `MONGODB_DATABASE` | Nome do database | `extrato` | Não |
| `MONGODB_USERNAME` | Usuário do MongoDB | `` | Não |
| `MONGODB_PASSWORD` | Senha do MongoDB | `` | Não |
| `MONGODB_URI` | URI completa (sobrescreve configurações acima) | `` | Não |
| `MONGODB_CONNECT_TIMEOUT` | Timeout para conexão | `10s` | Não |
| `MONGODB_QUERY_TIMEOUT` | Timeout para queries | `5s` | Não |
| `MONGODB_MAX_POOL_SIZE` | Tamanho máximo do pool de conexões | `100` | Não |
| `MONGODB_MIN_POOL_SIZE` | Tamanho mínimo do pool de conexões | `10` | Não |
| **Kafka** |
| `KAFKA_BROKERS` | Lista de brokers separados por vírgula | `localhost:9092` | Não |
| `KAFKA_TOPIC_TRANSACTION_CONFIRMED` | Tópico de entrada para transações | `ledger_nova_transacao_confirmada` | Não |
| `KAFKA_GROUP_STATEMENT` | Consumer group | `statement-ingestion-group` | Não |
| **Application** |
| `PORT` | Porta do servidor HTTP | `8084` | Não |
| `ENVIRONMENT` | Ambiente de execução | `development` | Não |
| `LOG_LEVEL` | Nível de log | `info` | Não |

### 8.5 Statement API Service

API REST para consulta de extratos no MongoDB.

| Variável | Descrição | Padrão | Obrigatório |
|----------|-----------|--------|-------------|
| **MongoDB** |
| `MONGODB_HOSTS` | Lista de hosts MongoDB separados por vírgula | `localhost:27017` | Não |
| `MONGODB_DATABASE` | Nome do database | `extrato` | Não |
| `MONGODB_USERNAME` | Usuário do MongoDB | `` | Não |
| `MONGODB_PASSWORD` | Senha do MongoDB | `` | Não |
| `MONGODB_URI` | URI completa (sobrescreve configurações acima) | `` | Não |
| `MONGODB_CONNECT_TIMEOUT` | Timeout para conexão | `10s` | Não |
| `MONGODB_QUERY_TIMEOUT` | Timeout para queries | `10s` | Não |
| `MONGODB_MAX_POOL_SIZE` | Tamanho máximo do pool de conexões | `100` | Não |
| `MONGODB_MIN_POOL_SIZE` | Tamanho mínimo do pool de conexões | `10` | Não |
| **Server** |
| `PORT` | Porta do servidor HTTP | `8085` | Não |
| `SERVER_READ_TIMEOUT` | Timeout de leitura HTTP | `10s` | Não |
| `SERVER_WRITE_TIMEOUT` | Timeout de escrita HTTP | `10s` | Não |
| **Application** |
| `ENVIRONMENT` | Ambiente de execução | `development` | Não |
| `LOG_LEVEL` | Nível de log | `info` | Não |

### 8.6 Simulador

Gerador de eventos para testes e demonstrações.

| Variável | Descrição | Padrão | Obrigatório |
|----------|-----------|--------|-------------|
| **Kafka** |
| `KAFKA_BROKERS` | Lista de brokers separados por vírgula | `localhost:9092` | Não |
| `KAFKA_TOPIC_CONTA_CRIADA` | Tópico para criação de contas | `conta_criada` | Não |
| `KAFKA_TOPIC_CONTA_MOVIMENTACAO` | Tópico para movimentações | `conta_movimentacao` | Não |
| `KAFKA_REQUIRED_ACKS` | Acks necessários (0=NoResponse, 1=Leader, -1=All) | `1` | Não |
| `KAFKA_ASYNC` | Modo assíncrono | `true` | Não |
| `KAFKA_BATCH_SIZE` | Tamanho do batch | `100` | Não |
| `KAFKA_BATCH_TIMEOUT` | Timeout do batch | `10ms` | Não |
| `KAFKA_COMPRESSION` | Compressão (snappy, gzip, lz4, zstd, none) | `snappy` | Não |
| **Simulation** |
| `SIM_NUM_CONTAS` | Número de contas a criar | `10` | Não |
| `SIM_CONTINUOUS_MODE` | Modo contínuo (true/false) | `false` | Não |
| `SIM_NUM_MOVIMENTACOES` | Número de movimentações (modo batch) | `100` | Não |
| `SIM_NUM_WORKERS` | Número de workers paralelos | `10` | Não |
| `SIM_EVENTS_PER_WORKER` | Eventos por worker antes de dormir (modo contínuo) | `1` | Não |
| `SIM_SLEEP_BETWEEN_EVENTS` | Sleep entre eventos (modo contínuo) | `1s` | Não |
| `SIM_CREDITO_PROBABILITY` | Probabilidade de crédito (0.0-1.0) | `0.7` | Não |
| `SIM_TRANSFER_PROBABILITY` | Probabilidade de transferência (0.0-1.0) | `0.2` | Não |
| `SIM_WAIT_AFTER_CREATE` | Tempo de espera após criar contas | `10s` | Não |
| **Application** |
| `ENVIRONMENT` | Ambiente de execução | `development` | Não |
| `LOG_LEVEL` | Nível de log | `info` | Não |

---

## 9. Imagens Docker

Todas as imagens estão disponíveis no Docker Hub e são otimizadas para produção com builds multi-stage.

| Imagem | Descrição | Tamanho (Uncompressed) | Tamanho (Compressed) |
|--------|-----------|------------------------|----------------------|
| `fidelissauro/ledger-event-sourcing:latest` | Ledger Service - Event Store principal | 66.7MB | 32MB |
| `fidelissauro/ledger-balance:latest` | Balance Ingestion - Atualiza ScyllaDB com as informações de saldo | 53.2MB | 25.8MB |
| `fidelissauro/ledger-balance-api:latest` | Balance API - Consulta de saldos | 68.3MB | 33.3MB |
| `fidelissauro/ledger-statement:latest` | Statement Ingestion - Atualiza MongoDB com os extratos | 60MB | 29.3MB |
| `fidelissauro/ledger-statement-api:latest` | Statement API - Consulta de extratos | 75.4MB | 36.9MB |
| `fidelissauro/ledger-simulador:latest` | Simulador - Gerador de eventos | 46.6MB | 22.5MB |

---

