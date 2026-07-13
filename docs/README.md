# Governança de Documentação — Estudo de Caso


## Case de Negócio

Um canal de pagamento precisa saber, antes de efetivar um débito, **se a operação vai ser aprovada e qual será o saldo resultante**. A primeira versão dessa simulação foi construída sobre o saldo consultivo (ScyllaDB, eventualmente consistente) e passou a aprovar operações que o saldo transacional real (Postgres, fonte da verdade) teria recusado. A organização precisou decidir, de forma documentada, como resolver isso sem comprometer a arquitetura de leitura que já existia para outros consumidores.

## Linha do tempo

| Data | Documento | Decisão |
|---|---|---|
| 2026-02-03 | [RFC-001](rfcs/RFC-001-api-simulacao-debito-credito.md) | Criar API de simulação de débito/crédito usando o saldo consultivo existente (Balance API / ScyllaDB) |
| 2026-02-05 | [ADR-0001](adrs/ADR-0001-saldo-consultivo-eventualmente-consistente.md) | Formaliza a decisão (já em produção) de saldo consultivo eventualmente consistente via LWT no ScyllaDB |
| 2026-04-14/16 | *Incidente #2026-118 (contexto, não é um documento deste exercício)* | Simulações aprovadas indevidamente durante pico de movimentações — saldo consultivo atrasado em relação ao saldo transacional |
| 2026-04-20 | [RFC-002](rfcs/RFC-002-inconsistencia-saldo-consultivo-simulacoes.md) | Abre a investigação formal e lista as alternativas a avaliar |
| 2026-04-27 | [RFC-003](rfcs/RFC-003-consistencia-forte-scylladb.md) — **Descartada** | Escalar consistência (LOCAL_QUORUM/ALL) no ScyllaDB |
| 2026-04-28 | [RFC-004](rfcs/RFC-004-simulacao-sincrona-embarcada-ledger.md) — **Descartada** | Embarcar a simulação no caminho síncrono de escrita do Ledger |
| 2026-04-29 | [RFC-005](rfcs/RFC-005-cache-aside-saldo-simulacao.md) — **Descartada** | Cache-aside com TTL curto sobre o saldo consultivo |
| 2026-05-04 | [RFC-006](rfcs/RFC-006-api-consulta-saldo-transacional.md) — **Aceita** | Nova API de simulação sobre réplica transacional do Ledger (`simulation-api`) |
| 2026-05-11 | [RFC-007](rfcs/RFC-007-api-simulacao-debito-credito-v2.md) — **Merged** | Propõe a simulação como um segundo serviço sobre o RFC-006; fundida nele em 2026-05-20 |
| 2026-05-06 | [ADR-0003](adrs/ADR-0003-rejeitar-consistencia-forte-scylladb.md) — **Rejeitada** | Registra por que a consistência forte no Scylla foi descartada |
| 2026-05-08 | [ADR-0004](adrs/ADR-0004-rejeitar-simulacao-embarcada-ledger.md) — **Rejeitada** | Registra por que a simulação embarcada no Ledger foi descartada |
| 2026-05-12 | [ADR-0002](adrs/ADR-0002-api-consulta-saldo-transacional-ledger.md) — **Aceita** | Registra a criação do `simulation-api` como capacidade de plataforma |
| 2026-05-20 | [ADR-0005](adrs/ADR-0005-servico-simulacao-desacoplado.md) — **Merged** | Racional original de serviço desacoplado; fundida no ADR-0002 na mesma data |
| 2026-05-20 | *Revisão RFC-006/ADR-0002* | Fusão final: `simulation-api` nasce lendo a réplica **e** aplicando a regra de simulação no mesmo processo, sem o segundo serviço (`ledger-query-api`) originalmente previsto |
| 2026-06-01 | [Strategy Doc](strategy/strategy-001-consistencia-apis-financeiras.md) | Consolida os princípios de consistência para qualquer API financeira futura |

## Como ler isso como palestra

1. **RFC-001 + ADR-0001** — o ponto de partida ingênuo, mas razoável: reusar o que já existe.
2. **O incidente** — o gatilho que expõe o limite do design (eventual consistency vazando para um caso de uso que exige garantias transacionais).
3. **RFC-002 → RFC-003/004/005** — o processo de descartar alternativas *com registro*, não apenas na cabeça de quem decidiu. Cada RFC descartada tem uma ADR irmã explicando o "não" para a posteridade.
4. **RFC-006 + ADR-0002 (absorvendo RFC-007/ADR-0005)** — a solução aceita, por que ela não repete o erro original, e por que dois serviços viraram um só: RFCs e ADRs também podem ser fundidas quando a implementação revela indireção desnecessária — e essa fusão fica registrada, não escondida.
5. **Strategy Doc** — o aprendizado generalizado: uma regra que qualquer API financeira futura no monorepo deve seguir, para não reabrir a mesma discussão a cada novo caso de uso.

## Convenções usadas

- **RFC** (`docs/rfcs/`): proposta de mudança técnica, decidida ou em decisão. Estado no cabeçalho (`Draft`, `Accepted`, `Rejected`, `Superseded`).
- **ADR** (`docs/adrs/`): registro imutável de uma decisão de arquitetura já tomada, incluindo decisões de **não fazer** algo. Nunca é editado após aceito — decisões que mudam geram uma nova ADR que supersede a anterior.
- **Strategy Doc** (`docs/strategy/`): documento vivo, revisado periodicamente, que consolida princípios extraídos de múltiplas RFCs/ADRs em uma diretriz reutilizável.
