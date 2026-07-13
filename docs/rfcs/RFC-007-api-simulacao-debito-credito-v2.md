# RFC-007 — API de Simulação de Débito/Crédito (v2)

| Campo | Valor |
|---|---|
| Status | **Merged** em [RFC-006](RFC-006-api-consulta-saldo-transacional.md) (2026-05-20) — ver "Nota de fusão" ao final |
| Autor | Squad Canais de Pagamento + Squad Ledger |
| Data | 2026-05-11 |
| Revisores | SRE, Squad Plataforma de Dados |
| Relacionados | [RFC-001](RFC-001-api-simulacao-debito-credito.md), [RFC-002](RFC-002-inconsistencia-saldo-consultivo-simulacoes.md), [RFC-006](RFC-006-api-consulta-saldo-transacional.md) (absorveu esta RFC), [ADR-0005](../adrs/ADR-0005-servico-simulacao-desacoplado.md) |

> **Nota:** esta RFC está preservada como registro histórico da proposta original de dois serviços em cadeia (`ledger-query-api` + `simulation-api`). O design efetivamente implementado está descrito no [RFC-006](RFC-006-api-consulta-saldo-transacional.md), que absorveu esta proposta — ver "Nota de fusão" ao final deste documento.

## Contexto

O [RFC-001](RFC-001-api-simulacao-debito-credito.md) implementou simulação sobre o saldo consultivo (ScyllaDB), o que causou o Incidente #2026-118 (ver [RFC-002](RFC-002-inconsistencia-saldo-consultivo-simulacoes.md)). O [RFC-006](RFC-006-api-consulta-saldo-transacional.md) criou uma fonte de leitura fortemente consistente (`ledger-query-api`) especificamente para casos como este. Esta RFC substitui a implementação original por uma versão que consome essa nova capacidade.

## Proposta

Criar um novo serviço dedicado, **`simulation-api`**, desacoplado tanto da `balance-api` quanto do `ledger`, com a única responsabilidade de simular movimentações sem efetivá-las.

### Por que um serviço novo, e não reaproveitar a `balance-api`

- `balance-api` foi desenhada para servir o saldo consultivo (ScyllaDB) — sua razão de existir é ser rápida e barata para o volume alto de consultas de app/dashboard. Colocar simulação nela reintroduz o acoplamento implícito entre "quero ver meu saldo" e "quero saber se uma operação vai passar", que foi exatamente a origem do problema do RFC-001.
- Um serviço próprio deixa explícito, na topologia do sistema, que simulação tem um contrato de consistência diferente (forte) do saldo consultivo (eventual) — reforça o princípio consolidado no [Strategy Doc](../strategy/strategy-001-consistencia-apis-financeiras.md).

### Design

1. **Endpoint**: `POST /simulacoes/debito-credito`
   ```json
   // request
   { "conta_id": "e424ed00-134e-4e92-92c1-40d57a7586c5", "tipo": "debito", "valor": 250.00 }

   // response (200)
   {
     "aprovado": false,
     "saldo_atual": 180.00,
     "saldo_projetado": -70.00,
     "motivo_recusa": "saldo_insuficiente"
   }
   ```
2. **Fonte de saldo**: chamada síncrona a `ledger-query-api` ([RFC-006](RFC-006-api-consulta-saldo-transacional.md)) — nunca ao ScyllaDB.
3. **Regra de negócio**: `saldo_projetado = saldo_atual ± valor`; recusa se `saldo_projetado < 0`, idêntico à regra aplicada pelo `ledger` ao processar `conta_movimentacao`.
4. **Paridade de regra com o Ledger**: a lógica de arredondamento monetário (`RoundMoney`, hoje em `ledger/internal/utils/money.go`) é extraída para um pacote compartilhado (`pkg/money`) importado tanto pelo `ledger` quanto pelo `simulation-api`, para eliminar o risco de as duas implementações divergirem silenciosamente ao longo do tempo — um dos riscos identificados na revisão desta RFC.
5. **Fail-closed**: se `ledger-query-api` responder `503` (lag de replicação acima do limiar) ou timeout, `simulation-api` responde `503` — nunca aprova uma simulação sem saldo confiável. Preferir recusar a simular incorretamente.
6. **Sem escrita**: como no RFC-001, não publica em `conta_movimentacao`, não persiste eventos, não gera efeito colateral no Ledger.

### Exposição

- Kong: novo `service` `simulation-service` → nova `route` `/api/v1/simulacoes`, com rate limiting próprio (Envoy), dimensionado para o padrão de tráfego de simulação (maior volume que confirmação real, tipicamente 3–5x).
- O endpoint `POST /balance/simulate` da `balance-api` (RFC-001) é removido nesta entrega.

## Alternativas consideradas

Nenhuma alternativa de arquitetura nova nesta RFC — as alternativas de fonte de dados já foram exploradas e decididas em [RFC-002](RFC-002-inconsistencia-saldo-consultivo-simulacoes.md)/[RFC-003](RFC-003-consistencia-forte-scylladb.md)/[RFC-004](RFC-004-simulacao-sincrona-embarcada-ledger.md)/[RFC-005](RFC-005-cache-aside-saldo-simulacao.md). A única decisão em aberto nesta RFC foi manter simulação na `balance-api` vs. extrair um serviço novo — resolvida a favor do serviço novo pelas razões acima e registrada em [ADR-0005](../adrs/ADR-0005-servico-simulacao-desacoplado.md).

## Impacto

- Novo serviço `simulation-api` no monorepo.
- Remoção do endpoint de simulação da `balance-api`.
- Dependência de runtime de `simulation-api` em `ledger-query-api` (RFC-006) — se este estiver indisponível, simulação fica indisponível (aceitável: fail-closed é a postura desejada para este domínio).

## Plano de rollout

1. Implementar `simulation-api` consumindo `ledger-query-api`.
2. Extrair `pkg/money` compartilhado e migrar `ledger` e `simulation-api` para usá-lo.
3. Rodar em paralelo com o endpoint antigo (`balance-api /balance/simulate`) por 1 semana, comparando divergência de resultado entre as duas implementações para o mesmo request (shadow mode).
4. Migrar canais para `/api/v1/simulacoes`, desligar `/balance/simulate`.
5. Encerrar RFC-001 como superseded.

## Métricas de sucesso

- Taxa de simulações aprovadas e posteriormente recusadas pelo Ledger: < 0,2% (meta definida no RFC-002), medida 30 dias após rollout completo.
- Disponibilidade do endpoint de simulação ≥ 99,5% (aceita indisponibilidade correlacionada à da réplica de leitura, por design fail-closed).

## Nota de fusão (2026-05-20)

Na revisão final antes do rollout, a divisão proposta aqui — um serviço `ledger-query-api` (RFC-006 original) somente para leitura de saldo, consumido por este `simulation-api` para aplicar a regra de negócio — foi considerada indireção desnecessária: um segundo hop de rede síncrono para aplicar a regra trivial `saldo_atual ± valor >= 0`, sem nenhum outro consumidor real ou planejado para a leitura pura de saldo transacional.

Esta RFC foi **fundida** no [RFC-006](RFC-006-api-consulta-saldo-transacional.md), que passou a entregar diretamente o serviço `simulation-api` — lendo a réplica transacional e aplicando a regra de simulação no mesmo processo, sem camada de orquestração intermediária. O racional desta RFC (serviço dedicado, desacoplado da `balance-api` e do `ledger`, fail-closed) permanece integralmente válido e é o que o RFC-006 entrega; apenas o número de serviços em cadeia mudou (de dois para um). Esta RFC permanece publicada como registro da análise original.
