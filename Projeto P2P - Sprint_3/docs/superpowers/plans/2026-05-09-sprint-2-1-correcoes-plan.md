# Sprint 2.1 — Plano de Correções e Lacunas

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans or subagent-driven-development to implement task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Corrigir o bug crítico de REQUEUE, atualizar imports dos testes para `workerp2`, adicionar infraestrutura de testes e cobertura de integração faltante.

**Architecture:** Todos os fixes são cirúrgicos em `workerp2.py` e nos arquivos de teste. O protocolo TCP/JSON não muda. O master recebe uma mensagem por conexão — o worker deve abrir uma conexão por tarefa no requeue.

**Tech Stack:** Python 3.x, `pytest`, `unittest.mock`, sockets TCP localhost.

---

### Task 1: Infraestrutura de testes (conftest + requirements)

**Files:**
- Create: `tests/conftest.py`
- Create: `requirements-dev.txt`

- [x] **Step 1: Criar `tests/conftest.py`**

```python
# tests/conftest.py
import sys
import os

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
```

- [x] **Step 2: Criar `requirements-dev.txt`**

```
pytest>=7.0
pytest-mock>=3.0
```

- [x] **Step 3: Verificar que pytest encontra os testes**

Run: `pytest tests/ --collect-only -q`
Expected: lista de testes sem ImportError

---

### Task 2: Atualizar testes unitários para importar de workerp2

**Files:**
- Modify: `tests/unit/test_heartbeat.py`
- Modify: `tests/unit/test_election.py`

- [x] **Step 1: Atualizar `tests/unit/test_heartbeat.py`**

Substituir:
```python
from worker.worker import Worker, MAX_HB_FAILURES
```
Por:
```python
import sys, os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
import workerp2 as w_mod
from workerp2 import Worker

MAX_HB_FAILURES = w_mod.MAX_HB_FAILURES
```

- [x] **Step 2: Atualizar `tests/unit/test_election.py`**

Substituir:
```python
from worker.worker import Worker
```
Por:
```python
import sys, os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
from workerp2 import Worker
```

- [x] **Step 3: Rodar testes unitários**

Run: `pytest tests/unit/ -v`
Expected: 3 testes passando (test_heartbeat_resets_failures_on_alive, test_heartbeat_increments_and_triggers_election_on_failure, test_deterministic_election)

---

### Task 3: Corrigir bug de REQUEUE multi-tarefa em workerp2.py

**Files:**
- Modify: `workerp2.py` (função `_requeue_tasks_to_master`, linhas ~344–392)

**Problema:** A implementação atual abre UMA conexão TCP e tenta enviar N tarefas. O master lê apenas UMA mensagem por conexão. Resultado: somente a primeira tarefa é reenfileirada; as demais vão para o fallback JSON sem aviso.

- [x] **Step 1: Substituir `_requeue_tasks_to_master` em `workerp2.py`**

```python
def _requeue_tasks_to_master(self):
    tasks = []
    while True:
        try:
            tasks.append(self._temp_queue.get_nowait())
        except _queue.Empty:
            break

    if not tasks:
        print("[REQUEUE] Nenhuma tarefa pendente para reencaminhar.")
        return

    print(f"[REQUEUE] Tentando reencaminhar {len(tasks)} tarefas para {MASTER_IP}:{MASTER_PORT}...")

    remaining = []
    for t in tasks:
        try:
            sock = socket.create_connection((MASTER_IP, MASTER_PORT), timeout=5)
            try:
                self._send(sock, {"TASK": "REQUEUE", "TASK_DATA": t, "TEMP_MASTER": self.uuid})
                resp = self._recv(sock)
                if not resp or resp.get('STATUS', '').upper() != 'ACK':
                    remaining.append(t)
            finally:
                sock.close()
        except Exception as e:
            print(f"[REQUEUE] Falha ao reencaminhar tarefa {t}: {e}")
            remaining.append(t)

    if remaining:
        filename = f"temp_tasks_{self.uuid}.json"
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(remaining, f, ensure_ascii=False, indent=2)
        print(f"[REQUEUE] {len(remaining)} tarefas não reencaminhadas. Persistidas em {filename}")
    else:
        print(f"[REQUEUE] Todas as {len(tasks)} tarefas reencaminhadas com sucesso.")
```

- [x] **Step 2: Escrever teste unitário para requeue multi-tarefa**

```python
# tests/unit/test_requeue.py
import sys, os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
import socket
import workerp2 as w_mod
from workerp2 import Worker


def test_requeue_opens_new_connection_per_task(monkeypatch):
    """Cada tarefa deve usar uma conexão TCP separada (master: 1 msg por conn)."""
    conn_count = [0]

    class FakeSock:
        def sendall(self, data):
            pass
        def settimeout(self, t):
            pass
        def recv(self, n):
            return b'{"STATUS":"ACK"}\n'
        def close(self):
            pass

    def fake_create(addr, timeout=None):
        conn_count[0] += 1
        return FakeSock()

    monkeypatch.setattr(socket, 'create_connection', fake_create)
    monkeypatch.setattr(Worker, '_recv', lambda self, sock, timeout=5: {'STATUS': 'ACK'})

    w = Worker()
    w._temp_queue.put({'TASK': 'QUERY', 'USER': 'A'})
    w._temp_queue.put({'TASK': 'QUERY', 'USER': 'B'})

    w._requeue_tasks_to_master()

    assert conn_count[0] == 2, f"Esperado 2 conexões, obtido {conn_count[0]}"
```

- [x] **Step 3: Rodar teste**

Run: `pytest tests/unit/test_requeue.py -v`
Expected: PASS

---

### Task 4: Corrigir teste de integração de requeue

**Files:**
- Modify: `tests/integration/run_temp_master_requeue.py`

**Problema:** O fake master do teste lê múltiplas mensagens na MESMA conexão — comportamento oposto ao master real. O teste dá falso positivo para o bug da Task 3. Após corrigir `workerp2.py`, o fake master também deve ser corrigido para aceitar UMA conexão por mensagem.

- [x] **Step 1: Substituir `master_server` em `run_temp_master_requeue.py`**

```python
def master_server(collected, stop_event):
    """Fake master: aceita uma conexão por vez, lê UMA mensagem por conexão."""
    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind(('0.0.0.0', 7011))
    srv.listen()
    srv.settimeout(2)

    while not stop_event.is_set():
        try:
            conn, _ = srv.accept()
        except socket.timeout:
            continue
        try:
            data = b''
            while True:
                ch = conn.recv(1)
                if not ch or ch == b'\n':
                    break
                data += ch
            if data:
                msg = json.loads(data.decode('utf-8'))
                collected.append(msg)
                if msg.get('TASK') == 'REQUEUE':
                    conn.sendall((json.dumps({'STATUS': 'ACK'}) + '\n').encode('utf-8'))
        except Exception:
            pass
        finally:
            conn.close()

    srv.close()
```

- [x] **Step 2: Atualizar `run()` para usar workerp2 e nova porta**

```python
import os, sys
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

import socket, threading, time, json
import workerp2 as w_mod
from workerp2 import Worker

def run():
    collected = []
    stop_event = threading.Event()

    t = threading.Thread(target=master_server, args=(collected, stop_event), daemon=True)
    t.start()
    time.sleep(0.3)

    w_mod.MASTER_IP = '127.0.0.1'
    w_mod.MASTER_PORT = 7011

    w = Worker()
    w._temp_queue.put({'TASK': 'QUERY', 'USER': 'A'})
    w._temp_queue.put({'TASK': 'QUERY', 'USER': 'B'})

    w.state = 'TEMP_MASTER'
    w._on_original_master_returned()

    time.sleep(1)
    stop_event.set()

    requeued = [m for m in collected if m.get('TASK') == 'REQUEUE']
    ok = len(requeued) == 2
    print(f'run_temp_master_requeue: {"PASS" if ok else "FAIL"} ({len(requeued)}/2 tarefas reencaminhadas)')

if __name__ == '__main__':
    run()
```

- [x] **Step 3: Rodar teste de integração**

Run: `python tests/integration/run_temp_master_requeue.py`
Expected: `run_temp_master_requeue: PASS (2/2 tarefas reencaminhadas)`

---

### Task 5: Adicionar teste de integração — temp master servindo tarefas

**Files:**
- Create: `tests/integration/run_temp_master_serve.py`

**O que testa:** Um worker (cliente) conecta ao temp master, pede tarefa (WORKER:ALIVE), recebe `{"TASK":"QUERY"}`, responde `{"STATUS":"OK"}`, recebe ACK. Verifica o ciclo completo.

- [x] **Step 1: Criar `tests/integration/run_temp_master_serve.py`**

```python
"""Teste de integração: temp master serve tarefas a workers."""
import os, sys
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

import socket, threading, time, json
import workerp2 as w_mod
from workerp2 import Worker

TEMP_PORT = 7011


def send_recv(sock, payload):
    sock.sendall((json.dumps(payload) + '\n').encode('utf-8'))
    data = b''
    while True:
        ch = sock.recv(1)
        if not ch or ch == b'\n':
            break
        data += ch
    return json.loads(data.decode('utf-8'))


def run():
    w_mod.MASTER_IP = '127.0.0.1'
    w_mod.MASTER_PORT = TEMP_PORT

    w = Worker()
    w._temp_queue.put({'TASK': 'QUERY', 'USER': 'TestUser'})
    w.state = 'TEMP_MASTER'

    t = threading.Thread(target=w._temp_master_server, daemon=True)
    t.start()
    time.sleep(0.3)

    results = {}
    try:
        sock = socket.create_connection(('127.0.0.1', TEMP_PORT), timeout=3)

        # Pede tarefa
        resp = send_recv(sock, {'WORKER': 'ALIVE', 'WORKER_UUID': 'TEST-WORKER-001'})
        results['task_received'] = resp.get('TASK') == 'QUERY' and resp.get('USER') == 'TestUser'

        # Reporta status
        ack = send_recv(sock, {'STATUS': 'OK', 'TASK': 'QUERY', 'WORKER_UUID': 'TEST-WORKER-001'})
        results['ack_received'] = ack.get('STATUS') == 'ACK'

        sock.close()
    except Exception as e:
        results['error'] = str(e)

    # Parar temp master
    w.state = 'NORMAL'
    time.sleep(0.2)

    ok = results.get('task_received') and results.get('ack_received')
    print(f'run_temp_master_serve: {"PASS" if ok else "FAIL"} | detalhes: {results}')


if __name__ == '__main__':
    run()
```

- [x] **Step 2: Rodar teste**

Run: `python tests/integration/run_temp_master_serve.py`
Expected: `run_temp_master_serve: PASS | detalhes: {'task_received': True, 'ack_received': True}`

---

### Task 6: Corrigir masterp2.py — docstring dupla

**Files:**
- Modify: `masterp2.py` (linhas 1–15)

- [x] **Step 1: Remover docstring duplicada**

Manter apenas a segunda docstring (standalone, correta). Remover as linhas 1–7 que dizem "importa e roda master/master.py" (comportamento que não existe no arquivo).

---

## Self-review checklist

- [x] Spec coverage: cada correção da spec tem uma task correspondente no plano.
- [x] Placeholder scan: nenhum TBD/TODO nos steps — todos têm código real.
- [x] Type consistency: `Worker`, `MAX_HB_FAILURES`, `MASTER_IP`, `MASTER_PORT` consistentes entre tasks.
- [x] Bug do requeue: Task 3 corrige o código + Task 4 corrige o teste para ser fiel ao master real.
- [x] Task 5 testa o ciclo que estava completamente sem cobertura.
