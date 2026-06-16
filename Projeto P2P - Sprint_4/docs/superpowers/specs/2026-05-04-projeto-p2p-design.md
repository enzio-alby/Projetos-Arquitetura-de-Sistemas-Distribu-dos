# Projeto P2P — Especificação de Design (2026-05-04)

**Tópico:** Implementação das tarefas 01, 02 e 03 sem gerar regressões

**Resumo/Objetivo:** Definir comportamento, critérios de aceitação e estratégia de implementação incremental para as três tarefas solicitadas, preservando compatibilidade do protocolo master↔worker e cobrindo tudo com testes automatizados.

---

## Contexto do projeto
- Arquivos relevantes: `master.py` (master server), `worker/worker.py` (lógica do worker), diretório `skills/` com processos de trabalho.
- Comunicação: TCP sockets que trocam JSON terminados em `\n`.
	- Observação: o master agora é identificado por nome (`MASTER_NAME`) e porta (padrão `7011`). Mensagens podem incluir `MASTER_NAME` e, para compatibilidade, também `MASTER_IP`.

## Escopo das tarefas (alto nível)
- Tarefa 01: Robustez do heartbeat e detecção de falhas (reduzir falsos positivos, logging, e backoff configurável).
- Tarefa 02: Protocolo de eleição — tornar votes determinísticos e adicionar métricas (score) testáveis.
- Tarefa 03: Master temporário — garantir enfileiramento seguro de tarefas e confirmação de status sem perda de mensagens.

## Regras de compatibilidade e não-regressão
- Nenhuma mudança deve quebrar a API TCP/JSON atual. Mensagens existentes (`HEARTBEAT`, `ALIVE`, `ELECTION`, `ELECTED`, `MASTER:ONLINE`) devem manter seus campos obrigatórios.
- Todas as mudanças devem vir acompanhadas por testes automatizados (unit + integração leve).
- Alterações devem ser pequenas (commits freqüentes) e revertíveis.

## Critérios de aceitação (por tarefa)
- Tarefa 01: Worker não inicia eleição por ruídos de rede; parâmetros `HEARTBEAT_INTERVAL`, `MAX_HB_FAILURES` e `ELECTION_WAIT` permanecem configuráveis e testáveis.
- Tarefa 02: Eleição resulta no mesmo vencedor em cenários determinísticos (mockando `compute_score()`), e votos são trocados corretamente entre peers.
- Tarefa 03: Quando worker vira `TEMP_MASTER`, tarefas enfileiradas são entregues e acknowledgements confirmados; se master original retornar, temp master devolve tarefas corretamente.

## Arquitetura proposta
- Manter a estrutura atual, extraindo utilitários pequenos se necessário (ex.: `p2p/utils.py`) apenas quando isso reduzir complexidade local e facilitar testes.
- Testes: `tests/unit/` e `tests/integration/` com `pytest`. Usar `unittest.mock` para sockets quando possível.

## Medidas anti-regressão e CI
- Adicionar `requirements-dev.txt` com `pytest` e `pytest-mock` (se não existir).
- Incluir um workflow de CI (opcional conforme autorização) para rodar `pytest` antes de merge.

## Plano de validação e rollout
1. Escrever testes que cubram os comportamentos desejados (TDD).
2. Implementar mudanças mínimas para passar nos testes.
3. Executar testes locais e em runner CI (se disponível).
4. Usar branch/working tree isolado para desenvolver e revisar. Evitar alterações diretas em `master`.

---

> Especificação gerada a partir do brainstorm inicial. Se aprovar, confirmarei e executarei `writing-plans` para gerar o plano detalhado (salvo em `docs/superpowers/plans/`).
