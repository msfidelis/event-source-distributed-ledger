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

## O que é o Ledger

O `ledger` é o serviço de event sourcing que processa comandos financeiros (`conta_criada`, `conta_movimentacao`) e é a única fonte da verdade sobre o estado de uma conta no monorepo. Ele persiste cada mudança como evento imutável no Event Store (`events`, Postgres) e mantém, dentro da mesma transação SQL, os read models transacionais (`accounts`, `transactions`) que refletem o estado "agora". Após o commit, publica confirmações em Kafka para que outros serviços — hoje, ingestão de saldo (ScyllaDB) e de extrato (MongoDB) — projetem esse estado em read models otimizados para consulta.

Todo o restante do sistema (`balance-api`, `statement-api`, e qualquer API financeira futura) existe para servir uma necessidade de leitura *derivada* do que o `ledger` decidiu. Nenhum desses serviços grava estado financeiro; eles projetam ou consultam o que já foi decidido e persistido pelo `ledger`.

## Regras de negócio do Ledger

- **Toda movimentação é um evento append-only.** Nenhuma conta é atualizada "no lugar" sem que a mudança primeiro exista como evento no Event Store, com `version` incremental por agregado — a base da reconstrução de estado (replay) e da ordenação.
- **Saldo nunca fica negativo.** Ao processar `conta_movimentacao`, o `ledger` lê o saldo atual, calcula o saldo resultante e recusa (rollback, sem publicar confirmação) qualquer débito que deixaria a conta negativa. Esta é a única regra de negócio que decide se uma movimentação é aceita ou recusada.
- **Persistência e publicação são atômicas em relação ao commit.** Os tópicos de confirmação (`ledger_nova_conta_registrada`, `ledger_nova_transacao_confirmada`, `ledger_saldo_atualizado`) só são publicados depois que a transação SQL correspondente é commitada — nunca antes, para que nenhum consumidor downstream projete um estado que pode ainda ser revertido.
- **Idempotência via identificador de negócio.** `movimentacao_id` identifica unicamente cada movimentação e é usado pelos read models downstream (ex.: `_id` no MongoDB) para que reprocessamento de mensagens não duplique efeito.
- **O caminho de escrita é protegido e não é reaproveitado para leitura.** O processo do `ledger` é o único caminho de escrita financeira do sistema e é protegido por rate limiting dedicado (Envoy). Nenhuma necessidade de leitura, por mais legítima que seja, deve virar uma rota HTTP síncrona nesse processo — ver princípio 4.

## Por que esta aplicação existe dentro do negócio

O Ledger existe para que decisões financeiras — criar conta, debitar, creditar, aprovar ou recusar uma operação — tenham exatamente uma origem de verdade, auditável e reconstruível, em vez de estarem espalhadas entre múltiplos bancos com contratos de consistência divergentes. Sem essa centralização, cada novo consumidor (app do cliente, canal de pagamento, dashboard interno) corre o risco de tomar uma decisão financeira binária — a categoria de erro mais cara do domínio — com base em um dado que já mudou. O Incidente #2026-118 (simulações aprovadas indevidamente por lerem saldo eventualmente consistente) é o exemplo concreto do custo de não ter essa separação explícita, e é a motivação direta dos princípios abaixo.

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

## Processo de governança (como este documento se conecta a RFCs, ADRs e Tech Specs)

1. **RFC** propõe uma mudança ou abre uma investigação. Pode gerar RFCs filhas para alternativas — cada alternativa levada a sério, mesmo se descartada, ganha sua própria RFC, não um parágrafo dentro da RFC principal.
2. **ADR** registra a decisão resultante de cada RFC fechada — inclusive (e especialmente) as decisões de **não fazer** algo. Uma ADR nunca é editada retroativamente para "consertar" uma decisão; uma nova ADR supersede a anterior quando o contexto muda (ver nota de atualização em [ADR-0001](../adrs/ADR-0001-saldo-consultivo-eventualmente-consistente.md), que não reverte a decisão original, apenas explicita seu limite).
3. **Strategy Doc** (este documento) é revisado, não substituído a cada ciclo — consolida princípios extraídos de múltiplas RFCs/ADRs para que a próxima decisão semelhante comece daqui, em vez de do zero.
4. **Tech Spec** (`docs/specs/`) documenta a implementação concreta que materializa uma decisão já tomada — stack, estrutura de código, configuração, contratos de API, métricas e testes. Não propõe decisão nova (isso é papel da RFC) nem registra decisão de arquitetura isolada (isso é papel da ADR); é o ponto de partida operacional para quem vai construir ou dar manutenção no serviço.

## Aplicação a futuras APIs

Antes de propor uma nova API sobre dados do Ledger, responda, na própria RFC:

1. Esta API precisa saber o estado "agora" para tomar uma decisão binária, ou tolera saber o estado "há alguns segundos"?
2. Se a resposta for "agora": ela deve consumir `simulation-api` (ou uma capacidade equivalente futura com o mesmo contrato), nunca um read model eventualmente consistente, e nunca uma rota nova no processo do `ledger`.
3. Qual é o comportamento quando a garantia de consistência não pode ser cumprida (fail-closed) — e isso está com testabilidade e alertas definidos antes do rollout, não descoberto em um incidente.

## Implementação de referência

Os princípios acima estão materializados em código no `simulation-api`, documentado como projeto de referência em [Tech Spec — spec-001: Implementação do `simulation-api`](../specs/spec-001-simulation-api.md) — stack, estrutura de diretórios, configuração, contratos de API, observabilidade e estratégia de testes. Qualquer squad construindo uma nova API com contrato de consistência forte sobre o Ledger deve usar esse documento como ponto de partida.

## Revisão

Este documento deve ser revisitado a cada trimestre, ou imediatamente após qualquer incidente relacionado a consistência de dados financeiros, seguindo o mesmo padrão do Incidente #2026-118: abrir uma RFC de investigação, registrar as alternativas descartadas com a mesma seriedade da aceita, e atualizar este documento se um novo princípio emergir.
