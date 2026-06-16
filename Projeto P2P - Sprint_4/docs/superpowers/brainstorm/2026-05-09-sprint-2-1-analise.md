# Sprint 2.1 — Análise do Estado Atual e Revisão do Brainstorm (2026-05-09)

**Contexto:** Revisão completa do que foi feito na Sprint 2.1 (com Gemini Flash em sala), identificação de bugs, lacunas e próximos passos. Arquivo central: `workerp2.py` + `masterp2.py`.

---

## 1) O que foi feito

| Item | Status | Arquivo |
|------|--------|---------|
| Brainstorm inicial | ✅ | `docs/superpowers/brainstorm/2026-05-04-*.md` |
| Spec de design | ✅ | `docs/superpowers/specs/2026-05-04-*.md` |
| Plano de implementação | ✅ | `docs/superpowers/plans/2026-05-04-*.md` |
| Master standalone | ✅ | `masterp2.py` (porta 7011) |
| Worker com heartbeat + backoff | ✅ | `workerp2.py` |
| Worker com eleição determinística | ✅ | `workerp2.py` |
| Worker com servidor temp master | ✅ | `workerp2.py` |
| Requeue de tarefas ao retorno | ✅ (com bug) | `workerp2.py` |
| Testes unitários heartbeat | ✅ (importação errada) | `tests/unit/test_heartbeat.py` |
| Testes unitários eleição | ✅ (importação errada) | `tests/unit/test_election.py` |
| Runners sem pytest | ✅ (importação errada) | `tests/unit/run_*.py` |
| Teste de integração requeue | ✅ (falso positivo) | `tests/integration/run_temp_master_requeue.py` |

---

## 2) Bugs críticos encontrados

### BUG 1 — REQUEUE multi-tarefa (CRÍTICO)
**Arquivo:** `workerp2.py:344-392` + `masterp2.py:84-150`

**Problema:** `_requeue_tasks_to_master()` abre **uma única conexão TCP** e tenta enviar N tarefas em loop pela mesma conexão. Mas `masterp2.py.handle_worker()` lê **apenas UMA mensagem** por conexão e fecha o socket no `finally`.

**Consequência:** Com 2+ tarefas na fila do temp master, somente a primeira é reenfileirada no master original. As demais vão para o arquivo JSON de fallback sem aviso.

**Pior:** O teste de integração existente dá **falso positivo** — o fake master do teste lê múltiplas mensagens na mesma conexão (comportamento diferente do master real).

**Fix:** Abrir uma nova conexão por tarefa no `_requeue_tasks_to_master()`.

### BUG 2 — Testes não testam workerp2.py (CRÍTICO)
**Arquivo:** `tests/unit/test_heartbeat.py:5`, `tests/unit/test_election.py:3`

**Problema:** Todos os testes importam `from worker.worker import Worker`. O arquivo central pedido (`workerp2.py`) **não é testado pelos testes existentes**.

**Consequência:** Bugs exclusivos de `workerp2.py` não seriam detectados pelos testes.

### BUG 3 — Portas incompatíveis entre arquivos
**Arquivo:** `worker/worker.py:17`

**Problema:** `worker/worker.py` define `MASTER_PORT = 5000`. Os testes importam desse arquivo, então usam porta 5000. Mas `masterp2.py` escuta na porta **7011**.

**Consequência:** Teste de integração que usa `MASTER_PORT` do `worker.worker` abre fake master na porta 5000, não 7011.

---

## 3) Problemas de qualidade (não críticos)

| Problema | Arquivo | Linha |
|----------|---------|-------|
| Docstring dupla contraditória | `masterp2.py` | 1–15 |
| Spec referencia `master.py` e `worker/worker.py` (arquivos antigos) | spec doc | — |
| Plano referencia `worker/worker.py` para implementação | plan doc | — |
| Checkboxes do plano: nenhum marcado (plano não executado) | plan doc | — |
| Task 3 do plano tem erro de indentação (sintaxe inválida) | plan doc | linha 101 |
| Falta `requirements-dev.txt` com pytest | — | — |
| Falta `conftest.py` para configuração de path nos testes | — | — |
| `_become_temp_master` pré-popula fila com dados hardcoded | `workerp2.py` | 247–249 |

---

## 4) O que falta (lacunas)

1. **Correção do bug REQUEUE** — nova conexão por tarefa
2. **Testes apontando para workerp2.py** — não worker/worker.py
3. **conftest.py** para configurar sys.path nos testes com pytest
4. **requirements-dev.txt** (pytest, pytest-mock)
5. **Teste de integração para temp master servindo tarefas** — verificar que workers conseguem pegar tarefas do temp master (não só o requeue)
6. **Marcar itens concluídos no plano original**

---

## 5) Abordagem recomendada para os fixes

**A. Fix incremental conservador (recomendado)**
- Corrigir apenas `_requeue_tasks_to_master()` (nova conexão por tarefa)
- Atualizar imports dos testes existentes de `worker.worker` para `workerp2`
- Adicionar conftest.py e requirements-dev.txt
- Adicionar teste de integração para temp master servindo tarefas

**B. Refatoração modular**
- Extrair protocolo TCP para módulo separado (`p2p/protocol.py`)
- Extrair heartbeat, eleição, temp master como módulos
- Maior cobertura de testes isolados
- Risco: escopo maior, mais tempo

**Recomendação:** A — incremental. O código de `workerp2.py` já está bem estruturado; o esforço está nos testes e no bug do REQUEUE.

---

> Gerado na revisão de 2026-05-09. Ver plano de execução em `docs/superpowers/plans/2026-05-09-sprint-2-1-correcoes-plan.md`.
