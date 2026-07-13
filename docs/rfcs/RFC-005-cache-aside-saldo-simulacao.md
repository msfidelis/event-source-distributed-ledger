# RFC-005 — Cache-Aside com TTL Curto sobre o Saldo Consultivo

| Campo | Valor |
|---|---|
| Status | **Rejected** (2026-05-08) |
| Autor | Squad Canais de Pagamento |
| Data | 2026-04-29 |
| Revisores | Squad Ledger, Squad Plataforma de Dados |
| Relacionados | [RFC-002](RFC-002-inconsistencia-saldo-consultivo-simulacoes.md) |

## Contexto

Ver [RFC-002](RFC-002-inconsistencia-saldo-consultivo-simulacoes.md). Proposta alternativa vinda da squad de Canais, buscando uma correção rápida sem depender de uma nova API no Ledger.

## Proposta avaliada

Introduzir um cache (Redis — já presente no `docker-compose.yml` do projeto, hoje usado pelo Envoy Rate Limiter) na frente do saldo consultivo:

1. `balance-api` verifica o cache antes de consultar o ScyllaDB.
2. Em caso de miss, consulta o Scylla e grava no cache com TTL de 2 segundos.
3. Reduz carga no Scylla sob pico e, por ironia, "resolveria" o sintoma observado no incidente ao forçar releitura mais frequente.

## Por que foi descartada

- **Ataca o sintoma errado.** O cache reduz carga de leitura, mas o problema do Incidente #2026-118 nunca foi volume de leitura na `balance-api` — foi o lag de **escrita** do consumer `balance-ingestion-group` em relação ao Postgres do Ledger. Um cache na frente de um dado já desatualizado apenas adiciona uma segunda fonte de atraso: na pior hipótese, uma simulação passaria a ler um valor com TTL de até 2s **somado** ao lag de ingestão já existente (até 4s observado no incidente), piorando o pior caso em vez de melhorá-lo.
- **Cria uma terceira cópia do saldo para manter consistente.** Passaríamos a ter saldo transacional (Postgres), saldo consultivo (Scylla) e agora um cache do saldo consultivo (Redis) — três níveis de staleness diferentes para a mesma informação, tornando qualquer investigação futura de "por que a simulação errou" mais difícil, não mais fácil.
- **Nenhuma garantia de consistência forte é obtida.** TTL curto reduz a probabilidade de dado velho, mas simulação de débito/crédito é uma decisão financeira binária (aprova ou recusa) — reduzir probabilidade de erro não é o mesmo que eliminar a classe de erro. O RFC-002 já definiu como critério de sucesso uma taxa de falso-positivo compatível com corrida legítima (<0,2%), o que exige eliminar a causa, não mitigá-la estatisticamente.

## Alternativa recomendada

Ver [RFC-006](RFC-006-api-consulta-saldo-transacional.md).

## Decisão

Rejeitada. Não foi aberta ADR dedicada para esta rejeição — o racional está coberto por [ADR-0003](../adrs/ADR-0003-rejeitar-consistencia-forte-scylladb.md), cujo argumento central ("não se pode obter consistência forte mitigando estatisticamente a leitura de um read model eventualmente consistente") se aplica igualmente aqui.
