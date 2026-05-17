# Sprint 2.1 Discovery — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans or subagent-driven-development to implement task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Adicionar descoberta dinâmica via UDP ao `masterp2.py` e `workerp2.py` conforme discovery.pdf — workers iniciam sem IP configurado, descobrem masters via broadcast, elegem pelo nome e conectam via TCP com handshake ELECTION_ACK.

**Architecture:** Adição incremental sobre o código existente. `MASTER_IP = None` ativa modo discovery. Se `--master-host` fornecido, skip discovery (retrocompatibilidade). Master ganha thread UDP listener + handler ELECTION_ACK no TCP.

**Tech Stack:** Python 3.x, `socket` (UDP + TCP), `pytest`, `threading`.

---

### Task 1: Constantes e CLI — masterp2.py

**Files:**
- Modify: `masterp2.py`

- [x] **Step 1: Adicionar constantes de discovery e MASTER_NAME**

Logo após as importações, antes de `HOST = '0.0.0.0'`, adicionar:

```python
MASTER_NAME = 'MASTER_1'   # nome para eleição lexicográfica
DISCOVERY_PORT = 5000       # porta UDP de descoberta
```

- [x] **Step 2: Adicionar argparse ao `masterp2.py`**

Substituir o bloco `if __name__ == '__main__': start_master()` por:

```python
def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--master-name', dest='master_name', default=None,
                        help='Nome deste master para eleicao (ex: MASTER_1)')
    args = parser.parse_args()

    global MASTER_NAME
    if args.master_name:
        MASTER_NAME = args.master_name

    start_master()


if __name__ == '__main__':
    main()
```

- [x] **Step 3: Verificar que masterp2.py ainda inicia sem erro**

Run: `C:\Python314\python.exe masterp2.py --help`
Expected: mostra opções sem traceback

---

### Task 2: UDP Discovery Listener — masterp2.py

**Files:**
- Modify: `masterp2.py`

- [x] **Step 1: Adicionar função `discovery_listener()`**

Adicionar antes de `handle_worker()`:

```python
def discovery_listener():
    """Escuta UDP DISCOVERY broadcasts e responde com DISCOVERY_REPLY unicast."""
    try:
        my_ip = socket.gethostbyname(socket.gethostname())
    except Exception:
        my_ip = '127.0.0.1'

    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    try:
        sock.bind(('', DISCOVERY_PORT))
    except Exception as e:
        print(f"[DISCOVERY] Nao foi possivel escutar na porta UDP {DISCOVERY_PORT}: {e}")
        return

    print(f"[DISCOVERY] Escutando UDP porta {DISCOVERY_PORT} como '{MASTER_NAME}'")

    while True:
        try:
            data, addr = sock.recvfrom(4096)
            text = data.decode('utf-8').strip()
            msg = json.loads(text)

            if msg.get('TYPE') == 'DISCOVERY':
                worker_uuid = msg.get('WORKER_UUID', '?')
                print(f"[DISCOVERY] Solicitacao de {worker_uuid} em {addr[0]}")

                reply = {
                    "TYPE": "DISCOVERY_REPLY",
                    "MASTER_NAME": MASTER_NAME,
                    "MASTER_IP": my_ip,
                    "MASTER_PORT": PORT,
                    "STATUS": "AVAILABLE",
                }
                sock.sendto((json.dumps(reply) + '\n').encode('utf-8'), addr)
                print(f"[DISCOVERY] Resposta enviada para {addr[0]}:{addr[1]}")

        except json.JSONDecodeError:
            print(f"[DISCOVERY] Payload invalido de {addr} — ignorado")
        except Exception as e:
            print(f"[DISCOVERY] Erro: {e}")
```

- [x] **Step 2: Iniciar thread de discovery em `start_master()`**

No início de `start_master()`, antes do `task_generator` thread, adicionar:

```python
threading.Thread(target=discovery_listener, daemon=True).start()
```

---

### Task 3: ELECTION_ACK TCP Handler — masterp2.py

**Files:**
- Modify: `masterp2.py` (função `handle_worker`)

- [x] **Step 1: Adicionar handling de `TYPE == ELECTION_ACK` em `handle_worker()`**

No bloco `if/elif` de `handle_worker()`, adicionar ANTES do `else` final:

```python
        elif message.get('TYPE', '').upper() == 'ELECTION_ACK':
            worker_uuid = message.get('WORKER_UUID', '?')
            selected = message.get('SELECTED_MASTER', '?')
            print(f"[ELECTION] Worker {worker_uuid} confirmou eleicao de {selected}")
            send_message(conn, {
                "TYPE": "ELECTION_ACK",
                "STATUS": "ACCEPTED",
                "MASTER_NAME": MASTER_NAME,
            })
```

---

### Task 4: Constantes de Discovery — workerp2.py

**Files:**
- Modify: `workerp2.py`

- [x] **Step 1: Alterar `MASTER_IP` default e adicionar constantes UDP**

Substituir:
```python
MASTER_IP = 'masterp2'
MASTER_PORT = 7011
```
Por:
```python
MASTER_IP = None        # None = modo discovery ativo
MASTER_PORT = 7011

DISCOVERY_PORT = 5000
DISCOVERY_MULTICAST = '239.255.255.250'
DISCOVERY_WAIT = 3      # segundos para coletar respostas UDP
```

---

### Task 5: Métodos de Discovery — workerp2.py

**Files:**
- Modify: `workerp2.py` (classe `Worker`)

- [x] **Step 1: Adicionar `_discover_masters()` antes de `_peer_server()`**

```python
    def _discover_masters(self):
        """Envia DISCOVERY via UDP broadcast/multicast e coleta respostas por DISCOVERY_WAIT s."""
        discovered = {}  # MASTER_NAME -> dict (deduplicar por nome)

        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        sock.bind(('', 0))

        payload = (json.dumps({"TYPE": "DISCOVERY", "WORKER_UUID": self.uuid}) + '\n').encode('utf-8')

        for dest in ('255.255.255.255', DISCOVERY_MULTICAST):
            try:
                sock.sendto(payload, (dest, DISCOVERY_PORT))
                print(f"[DISCOVERY] Pacote enviado para {dest}:{DISCOVERY_PORT}")
            except Exception as e:
                print(f"[DISCOVERY] Falha ao enviar para {dest}: {e}")

        deadline = time.time() + DISCOVERY_WAIT
        while True:
            remaining = deadline - time.time()
            if remaining <= 0:
                break
            sock.settimeout(remaining)
            try:
                data, addr = sock.recvfrom(4096)
                text = data.decode('utf-8').strip()
                msg = json.loads(text)

                name = msg.get('MASTER_NAME')
                ip = msg.get('MASTER_IP')
                port = msg.get('MASTER_PORT')

                if msg.get('TYPE') == 'DISCOVERY_REPLY' and name and ip and port:
                    if name not in discovered:
                        discovered[name] = msg
                        print(f"[DISCOVERY] Master encontrado: {name} em {ip}:{port}")
                else:
                    print(f"[DISCOVERY] Payload invalido de {addr} — ignorado")

            except socket.timeout:
                break
            except json.JSONDecodeError:
                print(f"[DISCOVERY] Payload nao-JSON recebido — ignorado")
            except Exception as e:
                print(f"[DISCOVERY] Erro ao receber: {e}")

        sock.close()
        return list(discovered.values())
```

- [x] **Step 2: Adicionar `_elect_master()` logo após `_discover_masters()`**

```python
    def _elect_master(self, masters):
        """Elege master pelo menor MASTER_NAME lexicografico."""
        if not masters:
            return None
        elected = sorted(masters, key=lambda m: m.get('MASTER_NAME', ''))[0]
        print(f"[ELECTION] Master eleito: {elected.get('MASTER_NAME')} "
              f"({elected.get('MASTER_IP')}:{elected.get('MASTER_PORT')})")
        return elected
```

- [x] **Step 3: Adicionar `_connect_and_ack()` logo após `_elect_master()`**

```python
    def _connect_and_ack(self, master):
        """Conecta TCP ao master eleito, envia ELECTION_ACK e aguarda ACCEPTED."""
        master_ip = master.get('MASTER_IP')
        master_port = master.get('MASTER_PORT', MASTER_PORT)
        master_name = master.get('MASTER_NAME', '?')

        print(f"[CONNECTING] Conectando a {master_name} em {master_ip}:{master_port}...")
        try:
            sock = socket.create_connection((master_ip, master_port), timeout=5)
            try:
                self._send(sock, {
                    "TYPE": "ELECTION_ACK",
                    "WORKER_UUID": self.uuid,
                    "SELECTED_MASTER": master_name,
                })
                resp = self._recv(sock, timeout=5)
                if (resp
                        and resp.get('TYPE') == 'ELECTION_ACK'
                        and resp.get('STATUS') == 'ACCEPTED'):
                    print(f"[CONNECTING] Eleicao confirmada! Master: {master_name}")
                    self.set_master(master_ip, master_port)
                    return True
                else:
                    print(f"[FALLBACK] Resposta inesperada: {resp}")
                    return False
            finally:
                sock.close()
        except Exception as e:
            print(f"[FALLBACK] Falha ao conectar a {master_name}: {e}")
            return False
```

- [x] **Step 4: Adicionar `_discovery_loop()` logo após `_connect_and_ack()`**

```python
    def _discovery_loop(self):
        """Descobre masters, elege e conecta. Retorna True quando conectado."""
        backoff = 1
        max_backoff = 60

        while self.running:
            print(f"\n[DISCOVERY] Iniciando descoberta de masters na rede...")
            masters = self._discover_masters()

            if not masters:
                print(f"[FALLBACK] NO_MASTER_FOUND. Aguardando {backoff}s...")
                time.sleep(backoff)
                backoff = min(backoff * 2, max_backoff)
                continue

            backoff = 1  # reset ao encontrar masters
            elected = self._elect_master(masters)
            if not elected:
                continue

            if self._connect_and_ack(elected):
                return True

            print(f"[FALLBACK] Conexao falhou. Invalidando cache e reiniciando discovery...")

        return False
```

---

### Task 6: Modificar `run()` e `main()` — workerp2.py

**Files:**
- Modify: `workerp2.py`

- [x] **Step 1: Modificar `run()` para chamar discovery se sem master**

No início de `run()`, antes do `print` de info, adicionar após a impressão inicial:

```python
        # Discovery: se nenhum master pre-configurado, descobrir na rede
        if self.master_ip is None:
            print("[DISCOVERY] Modo discovery ativo — buscando masters na rede...")
            if not self._discovery_loop():
                print("[ERRO] Nenhum master encontrado. Encerrando.")
                return
```

- [x] **Step 2: Modificar `main()` para tornar `--master-host` opcional (sem default)**

Substituir:
```python
    parser.add_argument('--master-host', '--master-name', '--master-ip', dest='master_host', default=None,
                        help='Master hostname or IP')
```
Deixar como está (já é `default=None`) — confirmar que sem `--master-host`, `MASTER_IP` permanece `None`.

Verificar o bloco de atribuição global:
```python
    if args.master_host:
        MASTER_IP = args.master_host
```
Isso já está correto — se não fornecido, `MASTER_IP` fica `None`.

---

### Task 7: Testes — eleição por nome

**Files:**
- Create: `tests/unit/test_election_by_name.py`

- [x] **Step 1: Criar teste de eleição determinística por MASTER_NAME**

```python
# tests/unit/test_election_by_name.py
import pytest
from workerp2 import Worker


def test_elects_lexicographically_smallest():
    w = Worker()
    masters = [
        {"TYPE": "DISCOVERY_REPLY", "MASTER_NAME": "MASTER_2", "MASTER_IP": "10.0.0.2", "MASTER_PORT": 7011},
        {"TYPE": "DISCOVERY_REPLY", "MASTER_NAME": "MASTER_1", "MASTER_IP": "10.0.0.1", "MASTER_PORT": 7011},
        {"TYPE": "DISCOVERY_REPLY", "MASTER_NAME": "MASTER_10", "MASTER_IP": "10.0.0.10", "MASTER_PORT": 7011},
    ]
    elected = w._elect_master(masters)
    assert elected["MASTER_NAME"] == "MASTER_1"


def test_elect_single_master():
    w = Worker()
    masters = [
        {"TYPE": "DISCOVERY_REPLY", "MASTER_NAME": "MASTER_3", "MASTER_IP": "10.0.0.3", "MASTER_PORT": 7011},
    ]
    elected = w._elect_master(masters)
    assert elected["MASTER_NAME"] == "MASTER_3"


def test_elect_empty_returns_none():
    w = Worker()
    assert w._elect_master([]) is None
```

- [x] **Step 2: Rodar testes**

Run: `C:\Python314\python.exe -m pytest tests/unit/ -v`
Expected: todos PASS (agora 7 testes)

---

### Task 8: Teste de integração — discovery flow

**Files:**
- Create: `tests/integration/run_discovery.py`

- [x] **Step 1: Criar teste de integração do fluxo completo**

```python
"""Teste de integracao: worker descobre master via UDP, elege e faz handshake TCP."""
import os, sys
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

import socket, threading, time, json
import workerp2 as w_mod
from workerp2 import Worker

UDP_PORT = 15000   # porta isolada para o teste
TCP_PORT = 17011   # porta TCP isolada para o teste
FAKE_MASTER_NAME = 'MASTER_1'
FAKE_MASTER_IP = '127.0.0.1'


def fake_udp_master(stop_event):
    """Fake master UDP: responde DISCOVERY com DISCOVERY_REPLY."""
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.bind(('', UDP_PORT))
    sock.settimeout(1)
    while not stop_event.is_set():
        try:
            data, addr = sock.recvfrom(4096)
            msg = json.loads(data.decode('utf-8').strip())
            if msg.get('TYPE') == 'DISCOVERY':
                reply = {
                    "TYPE": "DISCOVERY_REPLY",
                    "MASTER_NAME": FAKE_MASTER_NAME,
                    "MASTER_IP": FAKE_MASTER_IP,
                    "MASTER_PORT": TCP_PORT,
                    "STATUS": "AVAILABLE",
                }
                sock.sendto((json.dumps(reply) + '\n').encode('utf-8'), addr)
        except socket.timeout:
            continue
        except Exception:
            pass
    sock.close()


def fake_tcp_master(results, stop_event):
    """Fake master TCP: aceita ELECTION_ACK e responde ACCEPTED."""
    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind(('127.0.0.1', TCP_PORT))
    srv.listen()
    srv.settimeout(5)
    try:
        conn, _ = srv.accept()
        data = b''
        while True:
            ch = conn.recv(1)
            if not ch or ch == b'\n':
                break
            data += ch
        msg = json.loads(data.decode('utf-8'))
        results['election_ack_received'] = msg.get('TYPE') == 'ELECTION_ACK'
        results['selected_master'] = msg.get('SELECTED_MASTER')

        reply = {"TYPE": "ELECTION_ACK", "STATUS": "ACCEPTED", "MASTER_NAME": FAKE_MASTER_NAME}
        conn.sendall((json.dumps(reply) + '\n').encode('utf-8'))
        conn.close()
    except Exception as e:
        results['error'] = str(e)
    finally:
        srv.close()


def run():
    # Redirecionar discovery para portas isoladas do teste
    w_mod.MASTER_IP = None
    w_mod.DISCOVERY_PORT = UDP_PORT
    w_mod.MASTER_PORT = TCP_PORT

    stop_event = threading.Event()
    results = {}

    threading.Thread(target=fake_udp_master, args=(stop_event,), daemon=True).start()
    threading.Thread(target=fake_tcp_master, args=(results, stop_event), daemon=True).start()
    time.sleep(0.3)

    w = Worker()
    w._discovery_loop()

    time.sleep(0.5)
    stop_event.set()

    ok = (results.get('election_ack_received') is True
          and results.get('selected_master') == FAKE_MASTER_NAME)
    print(f'run_discovery: {"PASS" if ok else "FAIL"} | detalhes: {results}')
    print(f'  master_ip apos discovery: {w.master_ip}:{w.master_port}')


if __name__ == '__main__':
    run()
```

- [x] **Step 2: Rodar teste de integracao**

Run: `C:\Python314\python.exe tests/integration/run_discovery.py`
Expected: `run_discovery: PASS`

---

## Self-review checklist

- [x] Spec coverage: cada objetivo (O1–O5) e CT (CT01–CT05) tem task correspondente.
- [x] Placeholder scan: todos os steps têm código real.
- [x] Retrocompatibilidade: `--master-host` preserva comportamento anterior; testes existentes não quebram.
- [x] Deduplicação: `_discover_masters` usa dict por MASTER_NAME.
- [x] Eleição: `sorted(..., key=lambda m: m['MASTER_NAME'])` — lexicográfico puro (MASTER_1 < MASTER_10 < MASTER_2).
- [x] CT05 (payload malformado): `_discover_masters` descarta e loga warning.
- [x] CT03 (sem masters): `_discovery_loop` loga NO_MASTER_FOUND e faz backoff.
