# Sprint 4 — Plano de Implementação: Supervisor de Métricas e Dashboard Estruturado

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans or subagent-driven-development to implement task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Integrar o Master P2P ao Supervisor de Métricas externo via socket TLS sobre TCP puro, emitindo um payload `performance_report` completo a cada 10 segundos, e reformular o terminal para exibir um dashboard estruturado com todas as métricas da farm.

**Architecture:** Thread `supervisor_reporter` coleta métricas via `collect_metrics()` e chama `send_to_supervisor()` que abre conexão TLS raw (sem HTTP), envia JSON+`\n` e fecha imediatamente. Dashboard `print_dashboard()` exibe estado em grade estruturada com separadores.

**Tech Stack:** Python 3.x stdlib: `ssl`, `datetime`, `collections`. Opcional: `psutil` (degradação graciosa se ausente).

**Pre-conditions:**
- `master.py` Sprint 3 funcional (M2M, discovery, fault tolerance)
- `pytest tests/unit/ -v` → 24 PASS antes de começar

---

### Task 1: Imports, globals e constantes Sprint 4

**Files:**
- Modify: `master.py`

- [x] **Step 1: Adicionar imports**

```python
import ssl
import datetime
import collections

try:
    import psutil as _psutil
    _PSUTIL_OK = True
except ImportError:
    _psutil = None
    _PSUTIL_OK = False
```

- [x] **Step 2: Adicionar constantes do supervisor**

```python
SUPERVISOR_HOST = "nuted-ia.dev"
SUPERVISOR_PORT = 443
_start_time = time.time()
```

- [x] **Step 3: Adicionar tracking de fila**

```python
task_enqueue_times = collections.deque()
task_enqueue_lock  = threading.Lock()
```

Mantém timestamps FIFO paralelos ao `task_queue` — mesma ordem de entrada/saída.

---

### Task 2: `collect_metrics()` — coleta de estado completo

**Files:**
- Modify: `master.py`

- [x] **Step 1: Implementar `collect_metrics()`**

Campos do payload retornado:

| Campo | Fonte |
|-------|-------|
| `type` | literal `"performance_report"` |
| `timestamp` | `datetime.now(timezone.utc).isoformat()` |
| `master_id` | `MASTER_NAME` |
| `master_uuid` | `MASTER_UUID` |
| `uptime_s` | `time.time() - _start_time` |
| `system.cpu_percent` | `psutil.cpu_percent()` ou `-1.0` |
| `system.memory_*` | `psutil.virtual_memory()` ou `-1.0` |
| `system.disk_*` | `psutil.disk_usage('/')` ou `-1.0` |
| `farm.workers_known` | `len(known_workers)` |
| `farm.workers_borrowed` | `len(borrowed_workers)` |
| `farm.workers_lent` | `len(lent_workers)` |
| `farm.tasks_queued` | `task_queue.qsize()` |
| `farm.tasks_in_flight` | `len(in_flight_tasks)` |
| `farm.tasks_completed` | `stats['concluidas']` |
| `farm.tasks_failed` | `stats['falhas']` |
| `farm.heartbeats` | `stats['heartbeats']` |
| `farm.oldest_task_age_s` | `time.time() - task_enqueue_times[0]` ou `0.0` |
| `farm.neighbors_count` | `len(NEIGHBORS)` |
| `farm.saturation_pct` | `qsize / CAPACITY * 100` |
| `farm.capacity` | `CAPACITY` |

---

### Task 3: `send_to_supervisor()` — TLS sobre TCP puro

**Files:**
- Modify: `master.py`

- [x] **Step 1: Implementar `send_to_supervisor(payload)`**

```python
def send_to_supervisor(payload):
    ctx = ssl.create_default_context()
    raw = socket.create_connection((SUPERVISOR_HOST, SUPERVISOR_PORT), timeout=10)
    tls = ctx.wrap_socket(raw, server_hostname=SUPERVISOR_HOST)
    try:
        tls.sendall((json.dumps(payload) + "\n").encode("utf-8"))
    finally:
        tls.close()
```

**Restrições críticas:**
- Sem `recv()` — apenas conectar, enviar, fechar
- Sem HTTP — payload é JSON+`\n` direto sobre TLS
- SNI configurado via `server_hostname=SUPERVISOR_HOST`
- Exceções são capturadas pelo chamador (`supervisor_reporter`)

---

### Task 4: `supervisor_reporter()` — thread de envio periódico

**Files:**
- Modify: `master.py`

- [x] **Step 1: Implementar `supervisor_reporter()`**

```python
def supervisor_reporter():
    while True:
        time.sleep(10)
        try:
            payload = collect_metrics()
            send_to_supervisor(payload)
            print(f"[SUPERVISOR] Enviado — {payload['timestamp']} | ...")
        except Exception as e:
            print(f"[SUPERVISOR] Falha ao enviar: {e}")
```

- [x] **Step 2: Iniciar thread em `start_master()`**

```python
threading.Thread(target=supervisor_reporter, daemon=True).start()
```

---

### Task 5: `print_dashboard()` — dashboard estruturado

**Files:**
- Modify: `master.py`

- [x] **Step 1: Implementar `print_dashboard()`**

Formato de saída:
```
===========================================================================
 MASTER_8 (MASTER-P2-XXXX)           2026-06-09 14:35:22   Up: 0:02:15
===========================================================================
 [Info Atual ] CPU: 23.5%  |  RAM: 512MB/8192MB (6.2%)  |  Disco: 10.1%
 [Fila       ] Pendentes: 3    | Em execucao: 2   | + antiga: 45s
 [Workers    ] Conhecidos: 5   | Emprestados: 2   | Cedidos: 0
 [Tarefas    ] OK: 15     | Falhas: 1      | Heartbeats: 42
 [Masters    ] Vizinhos: 0    | Saturacao: 30.0% (3/10) | NORMAL
===========================================================================
```

- [x] **Step 2: Conectar ao comando `status` em `input_loop()`**

Substituir corpo do `elif cmd in ('status', 's')` por `print_dashboard()`.

---

### Task 6: Tracking de `task_enqueue_times`

**Files:**
- Modify: `master.py`

- [x] **Step 1: `input_loop()` — push ao enfileirar**
- [x] **Step 2: `handle_worker()` ALIVE — pop ao defileirar (`get_nowait`)**
- [x] **Step 3: Recovery — push em todas as 3 recolocações na fila**
  - Worker reconectou com tarefa interrompida (`interrupted_task`)
  - Conexão perdida após envio (`not status_report`)
  - Exceção inesperada (`except Exception as inner_e`)
- [x] **Step 4: Handler REQUEUE — push ao adicionar tarefa vinda de temp master**

---

### Task 7: Testes unitários Sprint 4

**Files:**
- Create: `tests/unit/test_sprint4_supervisor.py`

- [x] **Step 1: Criar arquivo com 19 casos de teste**

Casos cobertos:

**collect_metrics:**
- Chaves top-level presentes
- `type == "performance_report"`
- `timestamp` é ISO-8601 com timezone
- `uptime_s >= 0`
- Todas as chaves de `system` presentes
- Todas as chaves de `farm` presentes
- `tasks_queued` reflete fila real
- `oldest_task_age_s == 0.0` quando fila vazia
- `oldest_task_age_s >= 29s` quando há tarefa de 30s na fila
- `saturation_pct == 0.0` quando fila vazia
- `saturation_pct == 100.0` quando fila = CAPACITY
- `tasks_completed` e `tasks_failed` refletem `stats`
- `master_id` e `master_uuid` corretos

**send_to_supervisor:**
- Envia JSON + `\n`
- **Nunca chama `recv`** (restrição crítica)
- Fecha o socket após envio
- Usa SNI correto (`server_hostname = SUPERVISOR_HOST`)
- Conecta em `(SUPERVISOR_HOST, SUPERVISOR_PORT)`
- Não contém verbos HTTP no payload

- [x] **Step 2: Rodar testes**

Run: `pytest tests/unit/ -v`
Expected: **43 PASS, 0 FAIL** (24 sprint 1-3 + 19 sprint 4)

---

## Self-review checklist

- [x] Restrição HTTP: `send_to_supervisor` usa apenas `ssl` + `socket`, zero HTTP.
- [x] Sem recv: `tls.close()` no `finally` sem nenhum `recv` ou `read`.
- [x] SNI: `server_hostname=SUPERVISOR_HOST` configurado em `wrap_socket`.
- [x] Degradação psutil: fallback `-1.0` em todos os campos de sistema se ausente.
- [x] Thread safety: `task_enqueue_times` protegido por `task_enqueue_lock` em todos os pontos de acesso.
- [x] Consistência FIFO: push/pop de `task_enqueue_times` espelha exatamente put/get do `task_queue`.
- [x] Timestamp ISO-8601 com timezone UTC (`timezone.utc`, sem `utcnow` deprecated).
- [x] Dashboard: todos os 6 grupos de métricas representados (Info Atual, Fila, Workers, Tarefas, Masters/Saturação).
- [x] Sem regressão: Sprint 1/2/3 inalterados; 24 testes existentes continuam passando.
