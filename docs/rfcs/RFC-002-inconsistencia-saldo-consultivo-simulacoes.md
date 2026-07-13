# RFC-002 — Inconsistência do Saldo Consultivo em Simulações de Débito/Crédito

| Campo | Valor |
|---|---|
| Status | **Accepted** (investigação encerrada em 2026-05-20, ver documentos filhos) |
| Autor | Squad Ledger + Squad Plataforma de Dados |
| Data | 2026-04-20 |
| Revisores | Squad Canais de Pagamento, SRE |
| Relacionados | [RFC-001](RFC-001-api-simulacao-debito-credito.md), [ADR-0001](../adrs/ADR-0001-saldo-consultivo-eventualmente-consistente.md) |

## Contexto

Desde 2026-02-03 ([RFC-001](RFC-001-api-simulacao-debito-credito.md)), `POST /balance/simulate` consulta o ScyllaDB (saldo consultivo) para aprovar ou recusar simulações de débito/crédito.

### Incidente #2026-118 (14–16/04/2026)

Durante uma janela de pico de movimentações (campanha de crédito em massa processada pelo `simulador` em modo contínuo, gerando ~3x o volume normal de eventos em `conta_movimentacao`), o canal de pagamentos reportou clientes com débitos recusados pelo Ledger (saldo insuficiente) **apesar de a simulação prévia ter aprovado a operação**.

Causa raiz identificada:

- O saldo consultivo no ScyllaDB é atualizado de forma assíncrima pelo serviço `balance`, que consome `ledger_saldo_atualizado` e aplica a atualização via LWT (`UPDATE ... IF version < ?`), conforme documentado no README do projeto.
- Sob pico de throughput no Kafka, o lag de consumo do consumer group `balance-ingestion-group` chegou a ~4 segundos.
- Em contas com múltiplas movimentações concorrentes nesse intervalo, a simulação lia um `balance` mais antigo que o saldo real no Postgres (`accounts.balance` no Ledger), aprovando débitos que o Ledger corretamente recusou minutos — ou segundos — depois.
- Não houve inconsistência de dados: o ScyllaDB estava correto para a versão (`version`) que já havia recebido. O problema é o **uso de um read model eventualmente consistente para uma decisão que exige consistência forte no instante da simulação.**

Este é exatamente o ponto que a "Métrica de sucesso" do RFC-001 deixou sem baseline: taxa de simulações aprovadas que depois são recusadas na movimentação real. No incidente, essa taxa chegou a 6,8% das simulações durante a janela de pico (baseline fora de pico: ~0,1%, atribuível a corridas legítimas entre duas movimentações simultâneas na mesma conta).

## Problema a resolver

Definir uma fonte de dados para a simulação de débito/crédito que:

1. Reflita o saldo **no momento exato da simulação**, sem depender do lag de um pipeline assíncrono.
2. Não introduza acoplamento síncrono entre o canal de pagamentos e o caminho de escrita crítico do Ledger (que já é protegido por rate limiting via Envoy — ver `ledger/pkg/envoyratelimit`).
3. Não regrida a performance ou disponibilidade do saldo consultivo existente, que continua sendo a fonte correta para os demais consumidores (apps, dashboards, extratos).

## Alternativas avaliadas

Cada alternativa abaixo foi registrada como uma RFC independente, para que a decisão de descartar fique documentada com o mesmo rigor da decisão aceita:

| RFC | Alternativa | Resultado |
|---|---|---|
| [RFC-003](RFC-003-consistencia-forte-scylladb.md) | Elevar consistency level do ScyllaDB (`LOCAL_QUORUM`/`ALL`) e reduzir lag de ingestão | **Rejeitada** |
| [RFC-004](RFC-004-simulacao-sincrona-embarcada-ledger.md) | Embarcar a simulação no caminho síncrono de escrita do Ledger | **Rejeitada** |
| [RFC-005](RFC-005-cache-aside-saldo-simulacao.md) | Cache-aside com TTL curto sobre o saldo consultivo | **Rejeitada** |
| [RFC-006](RFC-006-api-consulta-saldo-transacional.md) | Nova API de simulação sobre réplica transacional do Ledger (`simulation-api`) | **Aceita** |

## Decisão

Adotar o RFC-006 como fonte de dados **e** capacidade de simulação: o `simulation-api` lê a réplica transacional do Ledger e aplica a regra de aprovação no mesmo serviço. (O RFC-006 originalmente propunha apenas a leitura de saldo, com a regra de simulação em um segundo serviço decidido no [RFC-007](RFC-007-api-simulacao-debito-credito-v2.md); os dois foram fundidos em 2026-05-20 durante a revisão final — ver nota de fusão no RFC-007.) O saldo consultivo (ScyllaDB) permanece eventualmente consistente e continua sendo a fonte correta para os demais casos de uso (consulta de saldo em app, dashboards) — ver [ADR-0001](../adrs/ADR-0001-saldo-consultivo-eventualmente-consistente.md), cujo escopo foi clarificado, não revertido.

## Impacto

- Nenhuma mudança imediata em `balance` (ingestão) ou `balance-api` além da remoção do endpoint `/balance/simulate` (RFC-001).
- Novo caminho de leitura direto ao Postgres do Ledger, com isolamento descrito no RFC-006.

## Métricas de sucesso

- Taxa de simulações aprovadas e posteriormente recusadas pelo Ledger: meta < 0,2% (equivalente à taxa de corrida legítima fora de pico), medida por 30 dias após o rollout do `simulation-api` (RFC-006).
