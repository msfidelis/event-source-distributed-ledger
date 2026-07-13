# RFC-004 — Simulação Síncrona Embarcada no Ledger

| Campo | Valor |
|---|---|
| Status | **Rejected** (2026-05-08) |
| Autor | Squad Ledger |
| Data | 2026-04-28 |
| Revisores | SRE, Squad Canais de Pagamento |
| Relacionados | [RFC-002](RFC-002-inconsistencia-saldo-consultivo-simulacoes.md), [ADR-0004](../adrs/ADR-0004-rejeitar-simulacao-embarcada-ledger.md) |

## Contexto

Ver [RFC-002](RFC-002-inconsistencia-saldo-consultivo-simulacoes.md). Se o problema é que nenhum read model fora do Ledger reflete o saldo no instante exato, uma segunda hipótese é eliminar o intermediário: fazer o próprio Ledger responder a simulação.

## Proposta avaliada

Adicionar um endpoint HTTP síncrono diretamente no serviço `ledger` (hoje um consumidor Kafka puro, sem rotas HTTP de domínio expostas publicamente — apenas `probes`/observabilidade), que:

1. Recebe a simulação via HTTP.
2. Executa, na mesma transação Postgres usada por `conta_movimentacao`, um `SELECT balance FROM accounts WHERE aggregate_id = ... FOR UPDATE` seguido do cálculo de saldo projetado.
3. Faz `ROLLBACK` explícito ao final — nunca commita, nunca escreve em `events` nem publica em Kafka.

## Por que foi descartada

- **Acopla um caminho de leitura de alto volume ao processo que protege o caminho de escrita crítico.** O `ledger` já implementa rate limiting via Envoy (`ledger/pkg/envoyratelimit`) especificamente para proteger a escrita no Event Store sob pico — o mesmo cenário do Incidente #2026-118. Expor um novo endpoint síncrono no mesmo processo, ainda que "somente leitura", compete por conexões do pool Postgres, CPU e I/O com o processamento de `conta_criada`/`conta_movimentacao`. Simulação é, por natureza, tráfego de maior volume e mais elástico (um canal pode simular 5x antes de confirmar 1x) — o pior tipo de carga para colocar ao lado do caminho transacional.
- **`SELECT ... FOR UPDATE` sem necessidade real de lock amplia contenção.** Mesmo fazendo rollback, o lock de linha é mantido durante toda a simulação, podendo bloquear a movimentação real da mesma conta que está em voo — criando latência induzida pelo próprio recurso que deveria evitar problema de concorrência.
- **Viola a fronteira de responsabilidade do serviço.** O `ledger` é hoje exclusivamente um consumidor de comandos (Kafka in) e publicador de eventos (Kafka out); não expõe API HTTP de domínio. Introduzir isso quebra o modelo mental do sistema (descrito no README: "Ledger consome comando, valida, persiste evento") e amplia a superfície de ataque e de operação de um componente que deveria ser o mais protegido do sistema.
- **Blast radius de incidentes.** Qualquer bug, vazamento de conexão ou pico de tráfego no endpoint de simulação passa a poder degradar a escrita do Ledger — o componente do qual todos os outros dependem. Isolar simulação em um serviço/API separada, mesmo lendo do mesmo Postgres, contém o raio de impacto.

## Alternativa recomendada

Ver [RFC-006](RFC-006-api-consulta-saldo-transacional.md): expor a leitura transacional como uma API dedicada, com seu próprio pool de conexões (idealmente contra uma réplica de leitura do Postgres), separada do processo `ledger`.

## Decisão

Rejeitada. Registrada formalmente em [ADR-0004](../adrs/ADR-0004-rejeitar-simulacao-embarcada-ledger.md).
