# Sprint 3 — Plano de Implementação: Protocolo M2M e Redirecionamento Dinâmico de Workers

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans or subagent-driven-development to implement task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implementar o protocolo de negociação Master-to-Master completo (request_help, command_redirect, register_temporary_worker, command_release, notify_worker_returned) sobre a base funcional das Sprints 1 e 2, sem quebrar nenhum teste ou comportamento existente.

**Architecture:** Protocolo baseado em JSON+`\n` via TCP. Mesmo socket porta 7011 para Workers e outros Masters — discriminação por campo `"type"` minúsculo. Workers redirecionados operam via piggyback no ciclo ALIVE existente.

**Tech Stack:** Python 3.x, `threading`, `socket`, `uuid`, `pytest`. Sem dependências externas novas.

**Pre-conditions:**
- `masterp2.py` Sprint 2.1 funcional (porta 7011, discovery UDP 5000)
- `workerp2.py` Sprint 2.1 funcional (heartbeat, ALIVE/QUERY/ACK, eleição, temp master)
- `pytest tests/unit/ -v` → 9 testes PASS antes de começar

---

### Task 1: Novos globals e helpers em masterp2.py

**Files:**
- Modify: `masterp2.py`

- [x] **Step 1: Adicionar globals de Sprint 3**

```python
CAPACITY = 10
RELEASE_THRESHOLD = 6
NEIGHBORS = []

known_workers = {}
known_workers_lock = threading.Lock()

redirect_targets = {}
redirect_targets_lock = threading.Lock()

borrowed_workers = {}
borrowed_workers_lock = threading.Lock()

lent_workers = set()
lent_workers_lock = threading.Lock()

release_pending = {}
release_pending_lock = threading.Lock()

_negotiating = threading.Lock()
_my_address = ""
```

- [x] **Step 2: Adicionar `get_my_ip()`, `make_m2m_msg()`, `log_m2m()`, `print_workers_state()`**

`make_m2m_msg(msg_type, request_id=None, payload=None)` → retorna dict com `type`, `request_id` (gera uuid4 se None), `payload`.
`log_m2m(direction, msg_type, request_id, addr="")` → imprime `[M2M HH:MM:SS] {direction} {type} | rid={rid[:8]} [addr]`.

- [x] **Step 3: Verificar que masterp2.py importa sem erros**

Run: `python -c "import masterp2"`
Expected: sem exceção

---

### Task 2: Monitoramento de saturação e função `request_help_from`

**Files:**
- Modify: `masterp2.py`

- [x] **Step 1: Implementar `saturation_monitor()` (thread)**

```python
def saturation_monitor():
    while True:
        time.sleep(5)
        if task_queue.qsize() <= CAPACITY or not NEIGHBORS:
            continue
        if not _negotiating.acquire(blocking=False):
            continue
        try:
            workers_needed = max(1, (task_queue.qsize() - CAPACITY) // 3 + 1)
            for neighbor in NEIGHBORS:
                if request_help_from(neighbor, workers_needed):
                    break
        finally:
            _negotiating.release()
```

- [x] **Step 2: Implementar `request_help_from(neighbor, workers_needed)`**

- Abre TCP para `neighbor["address"]`; timeout socket de 5s
- Envia `make_m2m_msg("request_help", rid, {..., "master_address": _my_address, ...})`
- Chama `log_m2m('->', 'request_help', rid, addr)`
- Aguarda resposta na mesma conexão
- `response_accepted` → retorna `True`; `response_rejected` → retorna `False`; timeout → retorna `False`

- [x] **Step 3: Iniciar thread em `start_master()`**

```python
threading.Thread(target=saturation_monitor, daemon=True).start()
```

---

### Task 3: Monitoramento de liberação e `notify_worker_returned`

**Files:**
- Modify: `masterp2.py`

- [x] **Step 1: Implementar `release_monitor()` (thread)**

```python
def release_monitor():
    while True:
        time.sleep(8)
        with borrowed_workers_lock:
            if not borrowed_workers or task_queue.qsize() >= RELEASE_THRESHOLD:
                continue
            to_release = list(borrowed_workers.keys())
        with release_pending_lock, borrowed_workers_lock:
            for wid in to_release:
                if wid in borrowed_workers and wid not in release_pending:
                    release_pending[wid] = {
                        "original_master_address": borrowed_workers[wid]["original_master_address"]
                    }
```

- [x] **Step 2: Implementar `notify_worker_returned(origin_address, worker_id)`**

- Parseia `"ip:porta"` de `origin_address`
- Abre TCP; envia `make_m2m_msg("notify_worker_returned", rid, {"worker_id": worker_id})`
- Fecha conexão; chama `log_m2m`

- [x] **Step 3: Iniciar thread em `start_master()`**

```python
threading.Thread(target=release_monitor, daemon=True).start()
```

---

### Task 4: Handlers M2M no masterp2.py

**Files:**
- Modify: `masterp2.py`

- [x] **Step 1: Implementar `handle_m2m(conn, addr, msg)`**

Dispatcher que lê `msg["type"]` e chama:
- `"request_help"` → `handle_request_help`
- `"register_temporary_worker"` → `handle_register_temporary_worker`
- `"notify_worker_returned"` → `handle_notify_worker_returned`
- Outros → log e ignorar

- [x] **Step 2: Implementar `handle_request_help(conn, addr, msg)`**

1. Extrair `master_id`, `master_address` (= onde Workers devem ir), `workers_needed`
2. Se `task_queue.qsize() > RELEASE_THRESHOLD`: responder `response_rejected {"reason": "high_load"}`
3. Selecionar de `known_workers` (last_seen < 60s, não em `lent_workers`, não em `redirect_targets`)
4. Se nenhum: `response_rejected {"reason": "no_workers_available"}`
5. Adicionar selecionados a `lent_workers` e `redirect_targets` com `new_master_address = master_address`
6. Responder `response_accepted` com `workers_offered` e `worker_details`

- [x] **Step 3: Implementar `handle_register_temporary_worker(conn, addr, msg)`**

```python
worker_id = msg["payload"]["worker_id"]
original_address = msg["payload"]["original_master_address"]
with borrowed_workers_lock:
    borrowed_workers[worker_id] = {"original_master_address": original_address, "since": time.time()}
print_workers_state()
# Sem resposta: Worker operará via Sprint 02 (ALIVE com SERVER_UUID)
```

- [x] **Step 4: Implementar `handle_notify_worker_returned(conn, addr, msg)`**

```python
worker_id = msg["payload"]["worker_id"]
with lent_workers_lock:
    lent_workers.discard(worker_id)
print_workers_state()
```

---

### Task 5: Integrar M2M em `handle_worker()` e atualizar ALIVE handler

**Files:**
- Modify: `masterp2.py`

- [x] **Step 1: Adicionar detecção M2M no início de `handle_worker()`**

```python
# Primeiro bloco de `handle_worker`, antes dos checks de campo:
if "type" in message and "TYPE" not in message:
    handle_m2m(conn, addr, message)
    return
```

- [x] **Step 2: Atualizar handler ALIVE para rastrear `known_workers`**

Após identificar `worker_uuid`, antes de qualquer lógica de fila:
```python
with known_workers_lock:
    known_workers[worker_uuid] = {"last_seen": time.time(), "addr": addr[0]}
```

- [x] **Step 3: Verificar `release_pending` antes de distribuir tarefa**

```python
with release_pending_lock:
    if worker_uuid in release_pending:
        data = release_pending.pop(worker_uuid)
        rid = str(uuid.uuid4())
        send_message(conn, make_m2m_msg("command_release", rid, {
            "original_master_address": data["original_master_address"]
        }))
        log_m2m('->', 'command_release', rid)
        with borrowed_workers_lock:
            borrowed_workers.pop(worker_uuid, None)
        threading.Thread(
            target=notify_worker_returned,
            args=(data["original_master_address"], worker_uuid),
            daemon=True
        ).start()
        print_workers_state()
        return
```

- [x] **Step 4: Verificar `redirect_targets` antes de distribuir tarefa**

```python
with redirect_targets_lock:
    if worker_uuid in redirect_targets:
        target = redirect_targets.pop(worker_uuid)
        rid = str(uuid.uuid4())
        send_message(conn, make_m2m_msg("command_redirect", rid, {
            "new_master_address": target["new_master_address"]
        }))
        log_m2m('->', 'command_redirect', rid)
        return
```

- [x] **Step 5: Adicionar tag "(emprestado)" no log de tarefas distribuídas**

```python
origin_tag = f" (emprestado de {server_uuid})" if server_uuid else ""
print(f" [FILA] Tarefa '{task_data['USER']}' -> {worker_uuid}{origin_tag}.")
```

---

### Task 6: Expandir CLI de masterp2.py

**Files:**
- Modify: `masterp2.py`

- [x] **Step 1: Adicionar argumentos `--port`, `--capacity`, `--neighbors`**

```python
parser.add_argument('--port', type=int, default=None)
parser.add_argument('--capacity', type=int, default=None)
parser.add_argument('--neighbors', nargs='*', default=None,
                    help='Formato: MASTER_ID@ip:porta')
```

- [x] **Step 2: Parsear `--neighbors` no formato `ID@ip:porta`**

```python
if args.neighbors:
    for entry in args.neighbors:
        if '@' in entry:
            mid, addr = entry.split('@', 1)
            NEIGHBORS.append({"master_id": mid.strip(), "address": addr.strip()})
```

- [x] **Step 3: Definir `_my_address` e exibir config no startup**

```python
_my_address = f"{get_my_ip()}:{PORT}"
print(f"Saturacao: >{CAPACITY} | Liberacao: <{RELEASE_THRESHOLD}")
if NEIGHBORS:
    print(f"Vizinhos: {[n['master_id']+'@'+n['address'] for n in NEIGHBORS]}")
```

---

### Task 7: Novos métodos em workerp2.py

**Files:**
- Modify: `workerp2.py`

- [x] **Step 1: Implementar `_handle_command_redirect(self, msg)`**

```python
def _handle_command_redirect(self, msg):
    payload = msg.get("payload", {})
    new_master_address = payload.get("new_master_address", "")
    if not new_master_address:
        return
    old_address = f"{self.master_ip}:{self.master_port}"
    self.borrowed_from = old_address
    parts = new_master_address.rsplit(':', 1)
    if len(parts) != 2:
        return
    self.set_master(parts[0], int(parts[1]))
    self._register_temporary_worker(parts[0], int(parts[1]), old_address)
```

- [x] **Step 2: Implementar `_handle_command_release(self, msg)`**

```python
def _handle_command_release(self, msg):
    payload = msg.get("payload", {})
    original_address = payload.get("original_master_address", "")
    if not original_address:
        return
    self.borrowed_from = None
    parts = original_address.rsplit(':', 1)
    if len(parts) == 2:
        self.set_master(parts[0], int(parts[1]))
```

- [x] **Step 3: Implementar `_register_temporary_worker(self, ip, port, original_address)`**

```python
def _register_temporary_worker(self, ip, port, original_address):
    try:
        sock = socket.create_connection((ip, port), timeout=5)
        self._send(sock, {
            "type": "register_temporary_worker",
            "request_id": str(uuid.uuid4()),
            "payload": {
                "worker_id": self.uuid,
                "original_master_address": original_address,
            },
        })
        sock.close()
    except Exception as e:
        print(f"[REDIRECT] Falha ao registrar: {e}")
```

- [x] **Step 4: Modificar `_request_task()` para interceptar M2M antes de QUERY/NO_TASK**

```python
# Após: response = self._recv(sock)
msg_type = response.get("type", "")
if msg_type == "command_redirect":
    self._handle_command_redirect(response)
    return
if msg_type == "command_release":
    self._handle_command_release(response)
    return
# fluxo existente: TASK field...
```

---

### Task 8: Testes unitários Sprint 3

**Files:**
- Create: `tests/unit/test_sprint3_m2m.py`

- [x] **Step 1: Criar arquivo de testes com 15 casos**

Casos cobertos:
- `make_m2m_msg` → estrutura correta com e sem request_id
- `_handle_command_redirect` → seta master, seta borrowed_from, chama register, trata endereço inválido, trata payload vazio
- `_handle_command_release` → limpa borrowed_from, restaura master, trata payload sem address
- `_request_task` → intercepta command_redirect, intercepta command_release, QUERY normal não é interceptado
- ALIVE com `borrowed_from` → inclui SERVER_UUID; sem `borrowed_from` → não inclui

- [x] **Step 2: Rodar testes**

Run: `pytest tests/unit/ -v`
Expected: **24 PASS, 0 FAIL**

---

### Task 9: Teste de integração M2M

**Files:**
- Create: `tests/integration/run_m2m_negotiation.py`

- [x] **Step 1: Criar servidor simulado Master B**

Script que escuta na porta 7099, recebe `request_help`, responde `response_accepted`, e verifica o payload:
- `type == "request_help"` ✓
- `request_id` presente ✓
- `payload.current_load` é int ✓
- `payload.workers_needed >= 1` ✓
- `response_accepted.request_id == request_help.request_id` ✓

- [x] **Step 2: Documentar como executar**

```bash
# Terminal 1:
python masterp2.py --master-name MASTER_A --capacity 2 --neighbors MASTER_B@127.0.0.1:7099

# Terminal 2:
python tests/integration/run_m2m_negotiation.py

# Terminal 3 (opcional — para testar register_temporary_worker):
python workerp2.py --master-host 127.0.0.1 --master-port 7011
```

Expected output do terminal 2:
```
[SUCESSO] request_help recebido e response_accepted enviado.
[OK] type == request_help
[OK] request_id presente
[OK] payload.current_load int
[OK] payload.workers_needed >= 1
RESULTADO: PASSOU
```

---

## Self-review checklist

- [x] Spec coverage: todas as 7 mensagens M2M da spec têm implementação correspondente.
- [x] Compatibilidade Sprint 1/2: handler `handle_worker()` mantém todos os fluxos existentes.
- [x] Thread safety: nenhum lock adquirido dentro de outro lock na mesma thread (sem deadlock).
- [x] Histerese: `RELEASE_THRESHOLD (6) < CAPACITY (10)` — sem ping-pong.
- [x] Timeout: `request_help_from` com socket.settimeout(5) + try/except socket.timeout.
- [x] `request_id` correlação: `response_accepted/rejected` reutiliza `request_id` do `request_help`.
- [x] Testes: 9 existentes + 15 novos = 24 PASS.
- [x] Tipo desconhecido: `handle_m2m()` loga e ignora sem derrubar processo.
- [x] CLI: `--capacity`, `--port`, `--neighbors` funcionais em `main()`.
