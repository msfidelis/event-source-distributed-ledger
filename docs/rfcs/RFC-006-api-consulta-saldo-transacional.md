# RFC-006 — API de Simulação de Débito/Crédito sobre Réplica Transacional (`simulation-api`)

| Campo | Valor |
|---|---|
| Status | **Accepted** (2026-05-12) — escopo ampliado em 2026-05-20 para absorver o [RFC-007](RFC-007-api-simulacao-debito-credito-v2.md) (ver "Revisão" ao final) |
| Autor | Squad Ledger + Squad Plataforma de Dados |
| Data | 2026-05-04 |
| Revisores | SRE, Squad Canais de Pagamento |
| Relacionados | [RFC-002](RFC-002-inconsistencia-saldo-consultivo-simulacoes.md), [RFC-003](RFC-003-consistencia-forte-scylladb.md) (rejeitada), [RFC-004](RFC-004-simulacao-sincrona-embarcada-ledger.md) (rejeitada), [RFC-007](RFC-007-api-simulacao-debito-credito-v2.md) (merged nesta RFC), [ADR-0002](../adrs/ADR-0002-api-consulta-saldo-transacional-ledger.md) |

## Contexto

O RFC-002 concluiu que nenhuma correção sobre o saldo consultivo (ScyllaDB) resolve o problema de simulações incorretas, porque a garantia necessária — refletir o saldo no instante exato — só existe, por construção, na fonte da verdade: a tabela `accounts` no Postgres do Ledger. O RFC-004 descartou expor essa leitura dentro do próprio processo `ledger` pelo risco ao caminho de escrita crítico.

Falta, portanto, uma forma de ler `accounts.balance` com consistência forte **e** decidir, no mesmo instante, se um débito ou crédito seria aprovado — sem competir por recursos com o processamento de `conta_criada`/`conta_movimentacao`.

## Proposta

Criar um novo serviço, **`simulation-api`**, com responsabilidade única: simular movimentações de débito/crédito com consistência forte, sem nunca escrever no Ledger.

### Design

1. **Fonte de dados**: uma réplica de leitura PostgreSQL (streaming replication / hot standby) do banco `eventsourcing` do Ledger. O serviço nunca escreve — usuário de banco `readonly`, sem permissão de `INSERT`/`UPDATE`/`DELETE`.
2. **Isolamento de recursos**: processo, pool de conexões (`pgxpool`/`bun` dedicados) e deploy próprios, independentes do `ledger`. Falhas, picos de tráfego ou vazamento de conexões neste serviço não afetam a capacidade do `ledger` de processar comandos.
3. **Endpoint**: `POST /simulacoes/debito-credito`, recebendo o valor da movimentação que o canal pretende realizar e devolvendo se ela seria aprovada e qual seria o saldo resultante:
   ```json
   // request
   {
     "conta_id": "e424ed00-134e-4e92-92c1-40d57a7586c5",
     "tipo": "debito",
     "valor": 250.00
   }
   ```
   ```json
   // response 200 — aprovado
   {
     "status": "aprovado",
     "saldo_atual": 500.00,
     "balance_after": 250.00
   }
   ```
   ```json
   // response 200 — recusado
   {
     "status": "recusado",
     "saldo_atual": 180.00,
     "balance_after": -70.00,
     "motivo_recusa": "saldo_insuficiente"
   }
   ```
4. **Regra de negócio**: `balance_after = saldo_atual ± valor` (crédito soma, débito subtrai); `status = "recusado"` sempre que `balance_after < 0`, idêntica à regra aplicada pelo `ledger` ao processar `conta_movimentacao`. A checagem de saldo disponível é feita contra o valor lido na réplica no instante da requisição, nunca contra um valor em cache.
5. **Fail-safe em vez de fail-open**: o serviço monitora o lag de replicação (`pg_stat_replication` / `pg_last_wal_replay_lsn`) continuamente. Se o lag exceder um limiar configurável (padrão: 200ms), o endpoint responde `503 Service Unavailable` em vez de simular sobre um saldo potencialmente atrasado. Esta é a diferença estrutural em relação ao ScyllaDB via Kafka (RFC-003): replicação física do Postgres tem lag mensurável e tipicamente sub-100ms, e — mais importante — o lag pode ser **verificado no momento da leitura**, permitindo recusar a resposta em vez de arriscar uma simulação incorreta. O pipeline via Kafka (saldo consultivo) não oferece esse sinal de forma síncrona.
6. **Sem cache.** Toda simulação lê a réplica no momento da chamada. O volume esperado (simulação, não todo o tráfego de consulta de saldo do app) não justifica cache, e cache reintroduziria exatamente o problema descartado no RFC-005.
7. **Sem escrita, sempre.** Não publica em `conta_movimentacao`, não persiste eventos, não é alcançável a partir do `ledger` — apenas simula.

### Escopo explícito

- Este serviço **não substitui** a `balance-api`/ScyllaDB para os casos de uso já existentes (consulta de saldo em app, dashboards, extratos). Continua sendo apropriado usar o saldo consultivo eventualmente consistente para esses casos — ver [ADR-0001](../adrs/ADR-0001-saldo-consultivo-eventualmente-consistente.md).
- Uso pretendido: qualquer canal que precise saber, antes de efetivar um débito ou crédito real, se a operação seria aprovada e qual seria o saldo resultante.
- Kong: rota dedicada `/api/v1/simulacoes`, separada da rota `/api/v1/balance` da `balance-api`, com rate limiting próprio dimensionado para o padrão de tráfego de simulação (tipicamente 3–5x o volume de confirmações reais).

## Alternativas consideradas

Ver [RFC-003](RFC-003-consistencia-forte-scylladb.md) (rejeitada) e [RFC-004](RFC-004-simulacao-sincrona-embarcada-ledger.md) (rejeitada) para as alternativas de fonte de dados. Ver a seção "Revisão" abaixo para a alternativa de topologia (dois serviços em cadeia) considerada e descartada dentro desta própria RFC.

## Impacto

- Requer provisionar uma réplica de leitura PostgreSQL para o banco `eventsourcing` (infraestrutura nova).
- Novo serviço no monorepo (`simulation-api`), com seu próprio Dockerfile, deploy e dashboards, seguindo o mesmo padrão dos demais serviços (`balance-api`, `statement-api`).
- Remoção do endpoint `POST /balance/simulate` da `balance-api` (introduzido no [RFC-001](RFC-001-api-simulacao-debito-credito.md)).
- Nenhuma mudança em `ledger`, `balance`, `statement`, `statement-api`.

## Plano de rollout

1. Provisionar réplica de leitura do Postgres.
2. Implementar `simulation-api` com o endpoint de simulação e o monitor de lag.
3. Extrair a lógica de arredondamento monetário (`RoundMoney`, hoje em `ledger/internal/utils/money.go`) para um pacote compartilhado (`pkg/money`), importado tanto pelo `ledger` quanto pelo `simulation-api`, para eliminar o risco de as duas implementações divergirem silenciosamente.
4. Validar em ambiente de staging reproduzindo o cenário do Incidente #2026-118 (carga do `simulador` em modo contínuo apontando para o volume de pico).
5. Expor `/api/v1/simulacoes` via Kong.
6. Rodar em paralelo com o endpoint antigo (`balance-api /balance/simulate`) por 1 semana, comparando divergência de resultado entre as duas implementações para o mesmo request (shadow mode).
7. Migrar canais para `/api/v1/simulacoes`, desligar `/balance/simulate`, encerrar o [RFC-001](RFC-001-api-simulacao-debito-credito.md) como superseded.

## Métricas de sucesso

- Lag de replicação p99 < 100ms em produção.
- 0 respostas servidas acima do limiar de lag configurado (deve resultar em `503`, nunca em simulação sobre dado stale).
- Latência p99 do endpoint < 100ms.
- Taxa de simulações aprovadas e posteriormente recusadas pelo Ledger: < 0,2% (meta definida no RFC-002), medida 30 dias após rollout completo.
- Disponibilidade do endpoint ≥ 99,5% (aceita indisponibilidade correlacionada à da réplica de leitura, por design fail-closed).

## Revisão (2026-05-20) — fusão com o RFC-007

O desenho original desta RFC propunha um serviço somente-leitura (`ledger-query-api`, apenas `GET` de saldo), consumido por um segundo serviço de orquestração ([RFC-007](RFC-007-api-simulacao-debito-credito-v2.md), `simulation-api`) responsável por aplicar a regra de negócio de aprovação. Na revisão final antes do rollout, ficou claro que essa divisão introduzia um segundo hop de rede síncrono para aplicar uma regra trivial (`saldo_atual ± valor >= 0`) — indireção sem benefício real, já que nenhum outro consumidor de leitura pura de saldo transacional existia ou estava planejado.

O RFC-007 foi então **fundido** nesta RFC: o serviço passou a se chamar `simulation-api` desde a origem, lendo diretamente a réplica e aplicando a regra de simulação, sem camada de orquestração intermediária. O racional original do RFC-007 e do [ADR-0005](../adrs/ADR-0005-servico-simulacao-desacoplado.md) — um serviço dedicado, desacoplado de `balance-api` e do `ledger` — continua válido e é o que esta RFC entrega; o que mudou foi apenas o número de serviços em cadeia (de dois para um). RFC-007 e ADR-0005 permanecem publicados, marcados como *Merged*, como registro histórico da análise original. Ver [ADR-0002](../adrs/ADR-0002-api-consulta-saldo-transacional-ledger.md) (revisado) para a decisão de arquitetura correspondente.
