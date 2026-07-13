# RFC-003 — Elevar Consistência do ScyllaDB para Uso em Simulação

| Campo | Valor |
|---|---|
| Status | **Rejected** (2026-05-06) |
| Autor | Squad Plataforma de Dados |
| Data | 2026-04-27 |
| Revisores | Squad Ledger, SRE |
| Relacionados | [RFC-002](RFC-002-inconsistencia-saldo-consultivo-simulacoes.md), [ADR-0003](../adrs/ADR-0003-rejeitar-consistencia-forte-scylladb.md) |

## Contexto

Ver [RFC-002](RFC-002-inconsistencia-saldo-consultivo-simulacoes.md). Uma das hipóteses de correção mais óbvias, dado que o problema aparenta ser "consistência", é atacar o próprio ScyllaDB: aumentar o nível de consistência de leitura/escrita e reduzir o lag do consumer de ingestão.

## Proposta avaliada

1. Elevar `SCYLLA_CONSISTENCY` na `balance-api` de `QUORUM` para `LOCAL_QUORUM` combinado com escrita em `ALL` no serviço `balance` (ingestão), forçando toda réplica a confirmar antes do consumer commitar o offset do Kafka.
2. Aumentar o paralelismo do consumer group `balance-ingestion-group` para reduzir lag sob pico.
3. Adicionar um mecanismo de "leitura com barreira": a simulação aguardaria até que a versão (`version`) mais recente conhecida pelo Ledger estivesse refletida no Scylla, via polling com backoff.

## Por que foi descartada

- **Não resolve o problema, só reduz a janela.** O ScyllaDB já usa LWT (`INSERT IF NOT EXISTS` / `UPDATE ... IF version < ?`) precisamente porque mensagens podem chegar fora de ordem — isso é uma característica estrutural da ingestão via Kafka, não um bug de configuração. Mesmo com `ALL` e zero lag de consumer, sempre existirá uma janela entre o commit no Postgres do Ledger e a entrega da mensagem em `ledger_saldo_atualizado`. Reduzir de 4s para 200ms não elimina o caso de corrida, só o torna mais raro e mais difícil de reproduzir — o pior cenário possível para um bug financeiro.
- **Custo de performance real e imediato.** O próprio README do projeto já documenta o tradeoff: "LWT tem custo de performance (Paxos)". Elevar consistência de escrita para `ALL` no cluster Scylla aumenta a latência de ingestão e reduz a disponibilidade do read model sempre que uma réplica fica indisponível — o oposto do motivo pelo qual o saldo consultivo existe (servir leituras rápidas e baratas para os demais consumidores).
- **"Leitura com barreira" (polling) transfere latência para o canal.** Sob pico — exatamente quando o incidente ocorreu — é quando o polling mais precisaria esperar, tornando a simulação mais lenta justamente no pior momento.
- **Mistura dois contratos de consistência num único read model.** Forçar o saldo consultivo a se comportar como fortemente consistente para atender simulação penaliza todos os outros consumidores (app, dashboards) que não precisam dessa garantia e que se beneficiam da leitura barata atual.

## Alternativa recomendada

Ver [RFC-006](RFC-006-api-consulta-saldo-transacional.md): usar a fonte que já é fortemente consistente por natureza — o Postgres do Ledger — em vez de tentar transformar um read model eventualmente consistente em algo que ele não foi desenhado para ser.

## Decisão

Rejeitada. Registrada formalmente em [ADR-0003](../adrs/ADR-0003-rejeitar-consistencia-forte-scylladb.md) para evitar que a mesma proposta volte a ser considerada sem o contexto desta análise.
