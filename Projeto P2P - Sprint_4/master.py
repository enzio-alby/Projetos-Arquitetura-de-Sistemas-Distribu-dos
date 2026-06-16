"""Standalone master — Master com descoberta UDP, negociacao M2M (Sprint 3) e
supervisor de metricas via TLS/TCP (Sprint 4).

Usage:
    python master.py
    python master.py --master-name MASTER_8 --port 7011
    python master.py --capacity 5 --neighbors MASTER_2@192.168.1.2:7011

Sprint 1: Heartbeat (Worker <-> Master)
Sprint 2: Ciclo de tarefas (ALIVE/QUERY/NO_TASK/STATUS/ACK) + discovery UDP
Sprint 3: Negociacao Master-to-Master + redirecionamento dinamico de Workers
Sprint 4: Metricas de desempenho via TLS/TCP + dashboard estruturado
"""
import socket
import json
import uuid
import threading
import queue
import time
import argparse
import datetime
import collections

try:
    import psutil as _psutil
    _PSUTIL_OK = True
except ImportError:
    _psutil = None
    _PSUTIL_OK = False

HOST = '0.0.0.0'
PORT = 7011
MASTER_NAME = 'MASTER_8'
DISCOVERY_PORT = 5000
MASTER_UUID = f"MASTER-P2-{uuid.uuid4().hex[:4].upper()}"

# Sprint 3: thresholds e vizinhos
CAPACITY = 10            # saturacao: tarefas pendentes acima disso dispara request_help
RELEASE_THRESHOLD = 6    # liberacao: tarefas pendentes abaixo disso devolve workers (histerese)
NEIGHBORS = []           # [{"master_id": str, "address": "ip:porta"}]

WORKER_PEER_ADDRESSES = []

# Sprint 4: Supervisor de metricas (TLS/TCP porta 8000 — IP local do professor)
SUPERVISOR_HOST = "10.62.206.206"
SUPERVISOR_PORT = 8002

_start_time = time.time()   # epoch do processo — para calculo de uptime

task_queue = queue.Queue()
stats = {"concluidas": 0, "falhas": 0, "heartbeats": 0}
stats_lock = threading.Lock()

# Sprint 4: timestamps de entrada na fila (FIFO, paralelo ao task_queue)
task_enqueue_times = collections.deque()
task_enqueue_lock = threading.Lock()

# Sprint 3: rastreamento de workers conhecidos (vistos recentemente)
known_workers = {}          # {worker_uuid: {"last_seen": float, "addr": str}}
known_workers_lock = threading.Lock()

# Workers especificos marcados para redirecionar (command_redirect pendente)
redirect_targets = {}       # {worker_uuid: {"new_master_address": str}}
redirect_targets_lock = threading.Lock()

# Workers emprestados que recebemos de outros masters
borrowed_workers = {}       # {worker_uuid: {"original_master_address": str, "since": float}}
borrowed_workers_lock = threading.Lock()

# Workers que emprestamos para outros masters
lent_workers = set()        # conjunto de worker_uuids
lent_workers_lock = threading.Lock()

# Workers a liberar (command_release pendente, aguardando proximo ALIVE)
release_pending = {}        # {worker_uuid: {"original_master_address": str}}
release_pending_lock = threading.Lock()

# Tarefas em processamento ativo — usado para recuperacao em caso de queda de conexao
in_flight_tasks = {}        # {worker_uuid: task_data}
in_flight_lock = threading.Lock()

# Evita negociacoes concorrentes simultaneas
_negotiating = threading.Lock()

# Status dos masters vizinhos — atualizado a cada M2M bem-sucedido
neighbor_status = {}        # {master_id: {"status": str, "last_heartbeat": str}}
neighbor_status_lock = threading.Lock()

# Endereco TCP deste master (ip:porta), preenchido em start_master()
_my_address = ""


# ── Helpers de I/O e utilitarios ──────────────────────────────────────────

def get_my_ip():
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(('8.8.8.8', 80))
        ip = s.getsockname()[0]
        s.close()
        return ip
    except Exception:
        return '127.0.0.1'


def recv_message(conn):
    """Le bytes ate \\n e retorna o JSON parseado."""
    data = b""
    while True:
        ch = conn.recv(1)
        if not ch:
            return None
        if ch == b'\n':
            break
        data += ch
    return json.loads(data.decode('utf-8'))


def send_message(conn, payload):
    """Serializa payload como JSON + \\n e envia."""
    conn.sendall((json.dumps(payload) + "\n").encode('utf-8'))


def make_m2m_msg(msg_type, request_id=None, payload=None):
    """Cria mensagem M2M no formato padrao da Sprint 3."""
    return {
        "type": msg_type,
        "request_id": request_id or str(uuid.uuid4()),
        "payload": payload or {},
    }


def log_m2m(direction, msg_type, request_id, addr=""):
    """Log padronizado para mensagens M2M com timestamp e request_id."""
    ts = time.strftime("%H:%M:%S")
    short_rid = request_id[:8] if len(request_id) >= 8 else request_id
    addr_str = f" [{addr}]" if addr else ""
    print(f"[M2M {ts}] {direction} {msg_type} | rid={short_rid}{addr_str}")


def print_workers_state():
    """Exibe contagem atual de workers locais e emprestados."""
    with known_workers_lock:
        nk = len(known_workers)
    with borrowed_workers_lock:
        nb = len(borrowed_workers)
    with lent_workers_lock:
        nl = len(lent_workers)
    print(f"[WORKERS] Conhecidos={nk} | Recebidos(emprestados)={nb} | Cedidos(outros masters)={nl} | Fila={task_queue.qsize()}")


# ── Discovery UDP (Sprint 2.1) ────────────────────────────────────────────

def discovery_listener():
    """Escuta broadcasts UDP DISCOVERY e responde com DISCOVERY_REPLY unicast."""
    my_ip = get_my_ip()

    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    try:
        sock.bind(('', DISCOVERY_PORT))
    except Exception as e:
        print(f"[DISCOVERY] Nao foi possivel escutar UDP porta {DISCOVERY_PORT}: {e}")
        return

    print(f"[DISCOVERY] Escutando UDP porta {DISCOVERY_PORT} como '{MASTER_NAME}'")

    while True:
        try:
            data, addr = sock.recvfrom(4096)
            msg = json.loads(data.decode('utf-8').strip())

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
        except json.JSONDecodeError:
            pass
        except Exception as e:
            print(f"[DISCOVERY] Erro: {e}")


def announce_online():
    """Avisa workers conhecidos (via peer port) que este master esta online."""
    if not WORKER_PEER_ADDRESSES:
        return

    my_ip = get_my_ip()
    payload = {
        "MASTER": "ONLINE",
        "MASTER_UUID": MASTER_UUID,
        "MASTER_NAME": MASTER_NAME,
        "MASTER_IP": my_ip,
        "MASTER_PORT": PORT,
    }

    def _send(ip, peer_port):
        try:
            sock = socket.create_connection((ip, peer_port), timeout=3)
            sock.sendall((json.dumps(payload) + "\n").encode('utf-8'))
            sock.close()
        except Exception:
            pass

    for ip, peer_port in WORKER_PEER_ADDRESSES:
        threading.Thread(target=_send, args=(ip, peer_port), daemon=True).start()


# ── Sprint 3: Monitoramento de saturacao ─────────────────────────────────

def saturation_monitor():
    """Thread que detecta saturacao e solicita Workers emprestados aos vizinhos."""
    while True:
        time.sleep(5)
        load = task_queue.qsize()

        if load <= CAPACITY:
            continue

        if not NEIGHBORS:
            continue

        if not _negotiating.acquire(blocking=False):
            continue  # ja em negociacao

        try:
            workers_needed = max(1, (load - CAPACITY) // 3 + 1)
            print(f"\n[SATURACAO] Carga {load} > capacidade {CAPACITY}."
                  f" Solicitando {workers_needed} worker(s) aos vizinhos...")

            for neighbor in NEIGHBORS:
                success = request_help_from(neighbor, workers_needed)
                if success:
                    print(f"[SATURACAO] Negociacao bem-sucedida com {neighbor['master_id']}.")
                    break
            else:
                print(f"[SATURACAO] Nenhum vizinho disponivel para ajuda no momento.")
        finally:
            _negotiating.release()


def _update_neighbor_status(master_id, status="available"):
    if not master_id:
        return
    ts = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    with neighbor_status_lock:
        neighbor_status[master_id] = {"master_id": master_id, "status": status, "last_heartbeat": ts}


def request_help_from(neighbor, workers_needed):
    """Conecta ao master vizinho e envia request_help. Retorna True se aceito."""
    master_id = neighbor.get("master_id", "?")
    address = neighbor.get("address", "")

    if not address:
        return False

    parts = address.rsplit(':', 1)
    if len(parts) != 2:
        print(f"[M2M] Endereco invalido do vizinho '{master_id}': {address}")
        return False

    n_ip, n_port_str = parts
    try:
        n_port = int(n_port_str)
    except ValueError:
        return False

    rid = str(uuid.uuid4())

    try:
        sock = socket.create_connection((n_ip, n_port), timeout=5)
        sock.settimeout(5)

        msg = make_m2m_msg("request_help", rid, {
            "master_id": MASTER_NAME,
            "master_address": _my_address,
            "current_load": task_queue.qsize(),
            "capacity": CAPACITY,
            "workers_needed": workers_needed,
        })
        send_message(sock, msg)
        log_m2m('->', 'request_help', rid, f"{n_ip}:{n_port}")

        # Aguarda resposta na mesma conexao (mesmo request_id)
        response = recv_message(sock)
        sock.close()

        if not response:
            print(f"[M2M] Sem resposta de {master_id} — timeout.")
            return False

        resp_type = response.get("type", "")
        resp_rid = response.get("request_id", "?")
        log_m2m('<-', resp_type, resp_rid, f"{n_ip}:{n_port}")

        if resp_type == "response_accepted":
            _update_neighbor_status(master_id, "available")
            payload = response.get("payload", {})
            offered = payload.get("workers_offered", 0)
            details = payload.get("worker_details", [])
            print(f"[M2M] {master_id} aceitou! Oferecendo {offered} worker(s): "
                  f"{[d.get('id') for d in details]}")
            print_workers_state()
            return True

        elif resp_type == "response_rejected":
            _update_neighbor_status(master_id, "busy")
            reason = response.get("payload", {}).get("reason", "desconhecido")
            print(f"[M2M] {master_id} recusou o pedido. Motivo: {reason}")
            return False

        else:
            print(f"[M2M] Resposta inesperada de {master_id}: type='{resp_type}' — ignorado.")
            return False

    except socket.timeout:
        print(f"[M2M] Timeout aguardando resposta de {master_id}."
              f" Descartando rid={rid[:8]}.")
        return False
    except Exception as e:
        print(f"[M2M] Erro ao contactar {master_id} ({address}): {e}")
        return False


# ── Sprint 3: Monitoramento de liberacao ─────────────────────────────────

def release_monitor():
    """Thread que detecta normalizacao de carga e agenda devolucao de workers emprestados."""
    while True:
        time.sleep(8)

        with borrowed_workers_lock:
            if not borrowed_workers:
                continue

        load = task_queue.qsize()
        if load >= RELEASE_THRESHOLD:
            continue

        with borrowed_workers_lock:
            to_release = list(borrowed_workers.keys())

        print(f"\n[LIBERACAO] Carga {load} < threshold {RELEASE_THRESHOLD}."
              f" Agendando devolucao de {len(to_release)} worker(s)...")

        with release_pending_lock, borrowed_workers_lock:
            for wid in to_release:
                if wid in borrowed_workers and wid not in release_pending:
                    release_pending[wid] = {
                        "original_master_address": borrowed_workers[wid]["original_master_address"]
                    }


def notify_worker_returned(origin_address, worker_id):
    """Notifica o Master de origem (via TCP) que um Worker foi devolvido."""
    parts = origin_address.rsplit(':', 1)
    if len(parts) != 2:
        print(f"[LIBERACAO] Endereco de origem invalido: {origin_address}")
        return

    o_ip, o_port_str = parts
    try:
        o_port = int(o_port_str)
    except ValueError:
        return

    rid = str(uuid.uuid4())
    try:
        sock = socket.create_connection((o_ip, o_port), timeout=5)
        msg = make_m2m_msg("notify_worker_returned", rid, {"worker_id": worker_id})
        send_message(sock, msg)
        sock.close()
        log_m2m('->', 'notify_worker_returned', rid, f"{o_ip}:{o_port}")
        print(f"[LIBERACAO] Master {origin_address} notificado: worker {worker_id} devolvido.")
    except Exception as e:
        print(f"[LIBERACAO] Falha ao notificar {origin_address}: {e}")


# ── Sprint 3: Handlers M2M (mensagens recebidas de outros Masters) ─────────

def handle_m2m(conn, addr, msg):
    """Dispatcher central para mensagens Master-to-Master."""
    t = msg.get("type", "")
    rid = msg.get("request_id", str(uuid.uuid4()))
    log_m2m('<-', t, rid, addr[0])

    if t == "request_help":
        handle_request_help(conn, addr, msg)
    elif t == "register_temporary_worker":
        handle_register_temporary_worker(conn, addr, msg)
    elif t == "notify_worker_returned":
        handle_notify_worker_returned(conn, addr, msg)
    else:
        print(f"[M2M] Tipo desconhecido '{t}' de {addr[0]} — logado e ignorado.")


def handle_request_help(conn, addr, msg):
    """Avalia pedido de ajuda de outro Master e responde com workers ou recusa."""
    payload = msg.get("payload", {})
    rid = msg.get("request_id", str(uuid.uuid4()))
    requester_id = payload.get("master_id", "?")
    requester_address = payload.get("master_address", f"{addr[0]}:{PORT}")
    workers_needed = max(1, int(payload.get("workers_needed", 1)))

    own_load = task_queue.qsize()
    print(f"[M2M] Pedido de ajuda de '{requester_id}' ({requester_address})."
          f" Precisa de {workers_needed} worker(s). Nossa carga: {own_load}.")

    # Recusar se nossa carga propria for alta
    if own_load > RELEASE_THRESHOLD:
        response = make_m2m_msg("response_rejected", rid, {"reason": "high_load"})
        send_message(conn, response)
        log_m2m('->', 'response_rejected', rid)
        return

    # Selecionar workers disponiveis (vistos nos ultimos 60s, nao cedidos, nao em redirect)
    now = time.time()
    with known_workers_lock:
        all_known = dict(known_workers)
    with lent_workers_lock:
        currently_lent = set(lent_workers)
    with redirect_targets_lock:
        already_targeted = set(redirect_targets.keys())

    candidates = [
        wid for wid, wdata in all_known.items()
        if now - wdata.get("last_seen", 0) < 60
        and wid not in currently_lent
        and wid not in already_targeted
    ]

    if not candidates:
        response = make_m2m_msg("response_rejected", rid, {"reason": "no_workers_available"})
        send_message(conn, response)
        log_m2m('->', 'response_rejected', rid)
        print(f"[M2M] Nenhum worker disponivel para oferecer a '{requester_id}'.")
        return

    selected_count = min(len(candidates), workers_needed)
    selected = candidates[:selected_count]

    # Registrar como cedidos
    with lent_workers_lock:
        for wid in selected:
            lent_workers.add(wid)

    # Agendar command_redirect para esses workers especificos
    with redirect_targets_lock:
        for wid in selected:
            redirect_targets[wid] = {"new_master_address": requester_address}

    # Montar worker_details para a resposta
    worker_details = []
    with known_workers_lock:
        for wid in selected:
            wdata = known_workers.get(wid, {})
            worker_details.append({"id": wid, "address": wdata.get("addr", "?")})

    response = make_m2m_msg("response_accepted", rid, {
        "workers_offered": selected_count,
        "worker_details": worker_details,
    })
    send_message(conn, response)
    log_m2m('->', 'response_accepted', rid)
    print(f"[M2M] Ofertando {selected_count} worker(s) para '{requester_id}': "
          f"{[w['id'] for w in worker_details]}")
    print_workers_state()


def handle_register_temporary_worker(conn, addr, msg):
    """Registra um Worker emprestado que se reportou a este Master."""
    payload = msg.get("payload", {})
    worker_id = payload.get("worker_id")
    original_address = payload.get("original_master_address", "?")

    if not worker_id:
        print(f"[M2M] register_temporary_worker sem worker_id de {addr[0]} — ignorado.")
        return

    with borrowed_workers_lock:
        borrowed_workers[worker_id] = {
            "original_master_address": original_address,
            "since": time.time(),
        }

    print(f"[M2M] Worker emprestado registrado: {worker_id} (origem: {original_address})")
    print_workers_state()
    # Sem resposta necessaria: worker operara via Sprint 02 (ALIVE com SERVER_UUID)


def handle_notify_worker_returned(conn, addr, msg):
    """Recebe notificacao de que um Worker cedido foi devolvido pelo Master que o emprestou."""
    payload = msg.get("payload", {})
    worker_id = payload.get("worker_id", "?")

    with lent_workers_lock:
        lent_workers.discard(worker_id)

    print(f"[M2M] Worker '{worker_id}' retornou ao nosso controle (notificacao recebida).")
    print_workers_state()


# ── Handler de cada conexao TCP (Workers + M2M) ────────────────────────────

def handle_worker(conn, addr):
    try:
        message = recv_message(conn)
        if not message:
            return

        # Sprint 3: M2M possuem campo 'type' (minusculo), Sprint 1/2 usam 'TYPE'/'TASK'/'WORKER'
        if "type" in message and "TYPE" not in message:
            handle_m2m(conn, addr, message)
            return

        type_field   = message.get('TYPE',   '').upper()
        task_field   = message.get('TASK',   '').upper()
        worker_field = message.get('WORKER', '').upper()

        # Sprint 2.1: Confirmacao de eleicao de master
        if type_field == 'ELECTION_ACK':
            worker_uuid = message.get('WORKER_UUID', '?')
            selected = message.get('SELECTED_MASTER', '?')
            print(f"[ELECTION] Worker {worker_uuid} confirmou eleicao de {selected}")
            send_message(conn, {
                "TYPE": "ELECTION_ACK",
                "STATUS": "ACCEPTED",
                "MASTER_NAME": MASTER_NAME,
            })

        # Sprint 1: Heartbeat
        elif task_field == 'HEARTBEAT':
            worker_id = message.get('SERVER_UUID', '?')
            print(f" [+] Heartbeat de: {worker_id} [{addr[0]}]")
            with stats_lock:
                stats['heartbeats'] += 1
            send_message(conn, {
                "SERVER_UUID": MASTER_UUID,
                "TASK": "HEARTBEAT",
                "RESPONSE": "ALIVE",
            })

        # Sprint 2: Pedido de tarefa / apresentacao do Worker
        elif worker_field == 'ALIVE':
            worker_uuid = message.get('WORKER_UUID')
            if not worker_uuid:
                print(f" [!] ALIVE sem WORKER_UUID de {addr} — ignorado.")
                return

            server_uuid = message.get('SERVER_UUID')  # preenchido se emprestado

            # Sprint 3: Atualizar registro de workers conhecidos
            with known_workers_lock:
                known_workers[worker_uuid] = {
                    "last_seen": time.time(),
                    "addr": addr[0],
                }

            if server_uuid:
                print(f" [+] Worker EMPRESTADO {worker_uuid} (de {server_uuid}) [{addr[0]}]")
            else:
                print(f" [+] Worker {worker_uuid} [{addr[0]}]")

            # Sprint 3: Verificar command_release pendente para este worker
            with release_pending_lock:
                if worker_uuid in release_pending:
                    release_data = release_pending.pop(worker_uuid)
                    release_rid = str(uuid.uuid4())
                    cmd = make_m2m_msg("command_release", release_rid, {
                        "original_master_address": release_data["original_master_address"]
                    })
                    send_message(conn, cmd)
                    log_m2m('->', 'command_release', release_rid)
                    print(f" [RELEASE] command_release enviado para {worker_uuid}")

                    with borrowed_workers_lock:
                        borrowed_workers.pop(worker_uuid, None)

                    # Notificar master de origem em background
                    threading.Thread(
                        target=notify_worker_returned,
                        args=(release_data["original_master_address"], worker_uuid),
                        daemon=True,
                    ).start()
                    print_workers_state()
                    return

            # Sprint 3: Verificar command_redirect pendente para este worker
            with redirect_targets_lock:
                if worker_uuid in redirect_targets:
                    target = redirect_targets.pop(worker_uuid)
                    redirect_rid = str(uuid.uuid4())
                    cmd = make_m2m_msg("command_redirect", redirect_rid, {
                        "new_master_address": target["new_master_address"]
                    })
                    send_message(conn, cmd)
                    log_m2m('->', 'command_redirect', redirect_rid)
                    print(f" [REDIRECT] command_redirect enviado para {worker_uuid}"
                          f" -> {target['new_master_address']}")
                    return

            # Sprint 3: Recuperacao — verificar se este worker tinha tarefa em execucao
            with in_flight_lock:
                interrupted_task = in_flight_tasks.pop(worker_uuid, None)
            if interrupted_task:
                task_queue.put(interrupted_task)
                with task_enqueue_lock:
                    task_enqueue_times.append(time.time())
                print(f" [RECOVERY] Worker {worker_uuid} reconectou com tarefa pendente."
                      f" Tarefa '{interrupted_task.get('USER')}' recolocada na fila."
                      f" Nova fila: {task_queue.qsize()}")

            # Sprint 2: Distribuicao normal de tarefas
            task_data = None
            try:
                task_data = task_queue.get_nowait()
                with task_enqueue_lock:
                    if task_enqueue_times:
                        task_enqueue_times.popleft()

                # Registrar como em execucao ANTES de enviar (fault tolerance)
                with in_flight_lock:
                    in_flight_tasks[worker_uuid] = task_data

                origin_tag = f" (emprestado de {server_uuid})" if server_uuid else ""
                print(f" [FILA] Tarefa '{task_data['USER']}' -> {worker_uuid}{origin_tag}."
                      f" Restam: {task_queue.qsize()}")
                send_message(conn, task_data)

                status_report = recv_message(conn)
                if not status_report:
                    # Conexao perdida apos envio — recolocar na fila garantindo continuidade
                    with in_flight_lock:
                        in_flight_tasks.pop(worker_uuid, None)
                    task_queue.put(task_data)
                    with task_enqueue_lock:
                        task_enqueue_times.append(time.time())
                    print(f" [RECOVERY] Conexao com {worker_uuid} perdida durante tarefa."
                          f" Tarefa '{task_data.get('USER')}' recolocada na fila."
                          f" Fila: {task_queue.qsize()}")
                    return

                # Status recebido com sucesso — remover do in-flight
                with in_flight_lock:
                    in_flight_tasks.pop(worker_uuid, None)

                status        = status_report.get('STATUS', '').upper()
                reported_uuid = status_report.get('WORKER_UUID', worker_uuid)

                if status == 'OK':
                    print(f" [OK] {reported_uuid} concluiu{' (emprestado)' if server_uuid else ''}.")
                    with stats_lock:
                        stats['concluidas'] += 1
                elif status == 'NOK':
                    print(f" [NOK] {reported_uuid} falhou na tarefa.")
                    with stats_lock:
                        stats['falhas'] += 1

                send_message(conn, {"STATUS": "ACK", "WORKER_UUID": reported_uuid})

            except queue.Empty:
                send_message(conn, {"TASK": "NO_TASK"})
            except Exception as inner_e:
                # Excecao inesperada: garantir que a tarefa nao seja perdida
                if task_data:
                    with in_flight_lock:
                        in_flight_tasks.pop(worker_uuid, None)
                    task_queue.put(task_data)
                    with task_enqueue_lock:
                        task_enqueue_times.append(time.time())
                    print(f" [RECOVERY] Excecao durante tarefa de {worker_uuid}."
                          f" Tarefa '{task_data.get('USER')}' recolocada na fila.")
                raise inner_e

        # Sprint 2: Requeue de tarefas do temp master
        elif task_field == 'REQUEUE':
            task_data = message.get('TASK_DATA')
            if task_data:
                task_queue.put(task_data)
                with task_enqueue_lock:
                    task_enqueue_times.append(time.time())
                send_message(conn, {"STATUS": "ACK"})
                print(f" [REQUEUE] Tarefa adicionada a fila: {task_data}")

        else:
            print(f" [!] Mensagem desconhecida de {addr}: {message}")

    except json.JSONDecodeError:
        print(f" [ERRO] JSON invalido de {addr}")
    except Exception as e:
        print(f" [ERRO] {addr}: {e}")
    finally:
        conn.close()


# ── Sprint 4: Metricas e supervisor ──────────────────────────────────────

_SEP = "=" * 75

def collect_metrics():
    """Coleta metricas no schema exato esperado pelo supervisor (sprint4-monitor)."""
    now = time.time()
    uptime_s = int(now - _start_time)

    if _PSUTIL_OK:
        cpu_pct  = round(_psutil.cpu_percent(interval=0.1), 2)
        cpu_log  = _psutil.cpu_count(logical=True)  or 1
        cpu_phy  = _psutil.cpu_count(logical=False) or 1
        mem      = _psutil.virtual_memory()
        dsk      = _psutil.disk_usage('/')
        try:
            load1, load5, _ = _psutil.getloadavg()
        except AttributeError:
            load1, load5 = 0.0, 0.0  # Windows nao tem getloadavg
    else:
        cpu_pct = cpu_log = cpu_phy = 0
        load1 = load5 = 0.0
        mem = type('M', (), {
            'total': 0, 'available': 0, 'used': 0, 'percent': 0.0
        })()
        dsk = type('D', (), {
            'total': 0, 'free': 0, 'percent': 0.0
        })()

    with known_workers_lock:
        nk = len(known_workers)
    with borrowed_workers_lock:
        nb_dict = dict(borrowed_workers)   # workers recebidos de outros masters
    with lent_workers_lock:
        nl_set  = set(lent_workers)        # workers cedidos a outros masters
    with in_flight_lock:
        nf = len(in_flight_tasks)
    with stats_lock:
        completed  = stats['concluidas']
        failures   = stats['falhas']

    nb = len(nb_dict)
    nl = len(nl_set)
    q_size    = task_queue.qsize()
    with task_enqueue_lock:
        oldest_age = round(now - task_enqueue_times[0], 1) if task_enqueue_times else 0.0

    # lista borrowed_workers: "in" = recebidos, "out" = cedidos
    bw_list = []
    for wid, wdata in nb_dict.items():
        bw_list.append({"direction": "in",  "peer_uuid": wdata.get("original_master_address", wid)})
    for wid in nl_set:
        bw_list.append({"direction": "out", "peer_uuid": wid})

    # lista de vizinhos com status
    with neighbor_status_lock:
        nb_status = dict(neighbor_status)
    neighbors_list = [
        {
            "server_uuid":     n.get("master_id", entry_id),
            "status":          n.get("status", "unknown"),
            "last_heartbeat":  n.get("last_heartbeat", ""),
        }
        for entry_id, n in nb_status.items()
    ]
    # incluir vizinhos configurados que ainda nao responderam
    seen_ids = {n["server_uuid"] for n in neighbors_list}
    for neighbor in NEIGHBORS:
        mid = neighbor.get("master_id", "")
        if mid and mid not in seen_ids:
            neighbors_list.append({"server_uuid": mid, "status": "unknown", "last_heartbeat": ""})

    ts = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    return {
        "server_uuid":     MASTER_UUID,
        "hostname":        MASTER_NAME,
        "role":            "master",
        "task":            "performance_report",
        "timestamp":       ts,
        "message_id":      str(uuid.uuid4()),
        "payload_version": "sprint4-monitor",
        "performance": {
            "system": {
                "uptime_seconds":  uptime_s,
                "load_average_1m": round(load1, 2),
                "load_average_5m": round(load5, 2),
                "cpu": {
                    "usage_percent":   cpu_pct,
                    "count_logical":   cpu_log,
                    "count_physical":  cpu_phy,
                },
                "memory": {
                    "total_mb":     round(mem.total     / 1_048_576, 1),
                    "available_mb": round(mem.available / 1_048_576, 1),
                    "percent_used": round(mem.percent, 2),
                    "memory_used":  round(mem.used      / 1_048_576, 1),
                },
                "disk": {
                    "total_gb":    round(dsk.total / 1_073_741_824, 2),
                    "free_gb":     round(dsk.free  / 1_073_741_824, 2),
                    "percent_used": round(dsk.percent, 2),
                },
            },
            "farm_state": {
                "workers": {
                    "total_registered":         nk,
                    "workers_utilization":       nf,
                    "workers_alive":             nk,
                    "workers_idle":              max(0, nk - nf),
                    "workers_borrowed":          nl,
                    "workers_received":          nb,
                    "workers_failed":            0,
                    "workers_home":              max(0, nk - nb),
                    "workers_available_capacity": max(0, CAPACITY - q_size),
                    "borrowed_workers":          bw_list,
                },
                "tasks": {
                    "tasks_pending":      q_size,
                    "tasks_running":      nf,
                    "tasks_completed":    completed,
                    "tasks_failed":       failures,
                    "oldest_task_age_s":  oldest_age,
                },
            },
            "config_thresholds": {
                "max_task":             CAPACITY,
                "warn_cpu_percent":     85,
                "warn_memory_percent":  85,
                "release_task":         RELEASE_THRESHOLD,
            },
            "neighbors": neighbors_list,
        },
    }


def send_to_supervisor(payload, host=None, port=None):
    """Envia payload ao supervisor via TCP simples (porta 8000, sem TLS).
    Apenas conecta, transmite e fecha — sem recv, sem HTTP.
    """
    h = host if host is not None else SUPERVISOR_HOST
    p = port if port is not None else SUPERVISOR_PORT
    sock = socket.create_connection((h, p), timeout=10)
    try:
        sock.sendall((json.dumps(payload) + "\n").encode("utf-8"))
    finally:
        sock.close()


def supervisor_reporter():
    """Thread: envia performance_report ao supervisor via TCP/8000 a cada 10s."""
    while True:
        time.sleep(10)
        try:
            payload  = collect_metrics()
            perf     = payload["performance"]
            sys_     = perf["system"]
            farm     = perf["farm_state"]
            cpu_pct  = sys_["cpu"]["usage_percent"]
            q_size   = farm["tasks"]["tasks_pending"]
            workers  = farm["workers"]["total_registered"]
            summary  = (f"{payload['timestamp']} | "
                        f"CPU: {cpu_pct}% | "
                        f"Fila: {q_size} | "
                        f"Workers: {workers}")
            send_to_supervisor(payload)
            print(f"[SUPERVISOR/TCP/{SUPERVISOR_PORT}] Enviado — {summary}")
        except Exception as e:
            print(f"[SUPERVISOR/TCP/{SUPERVISOR_PORT}] Falha: {e}")


def print_dashboard():
    """Exibe dashboard estruturado com metricas atuais da farm e do sistema."""
    now_str = time.strftime("%Y-%m-%d %H:%M:%S")
    uptime_s = int(time.time() - _start_time)
    h, rem = divmod(uptime_s, 3600)
    m, s   = divmod(rem, 60)
    up_str = f"{h}:{m:02d}:{s:02d}"

    if _PSUTIL_OK:
        cpu = round(_psutil.cpu_percent(interval=None), 1)
        mem = _psutil.virtual_memory()
        dsk = _psutil.disk_usage('/')
        sys_line = (f"CPU: {cpu}%  |  "
                    f"RAM: {round(mem.used/1_048_576)}MB/{round(mem.total/1_048_576)}MB"
                    f" ({mem.percent}%)  |  Disco: {dsk.percent}%")
    else:
        sys_line = "CPU: n/d  |  RAM: n/d  |  Disco: n/d  (psutil ausente)"

    q_size = task_queue.qsize()
    with in_flight_lock:
        nf = len(in_flight_tasks)
    with known_workers_lock:
        nk = len(known_workers)
    with borrowed_workers_lock:
        nb = len(borrowed_workers)
    with lent_workers_lock:
        nl = len(lent_workers)
    with stats_lock:
        c  = stats['concluidas']
        f  = stats['falhas']
        hb = stats['heartbeats']
    with task_enqueue_lock:
        oldest = int(time.time() - task_enqueue_times[0]) if task_enqueue_times else 0

    sat_pct    = round(q_size / CAPACITY * 100, 1) if CAPACITY else 0.0
    sat_status = "SATURADO" if q_size > CAPACITY else "NORMAL"

    title = f" {MASTER_NAME} ({MASTER_UUID})"
    right = f"{now_str}   Up: {up_str} "
    pad   = max(0, 75 - len(title) - len(right))

    print(_SEP)
    print(title + " " * pad + right)
    print(_SEP)
    print(f" [Info Atual ] {sys_line}")
    print(f" [Fila       ] Pendentes: {q_size:<5}| Em execucao: {nf:<4}| + antiga: {oldest}s")
    print(f" [Workers    ] Conhecidos: {nk:<4}| Emprestados: {nb:<4}| Cedidos: {nl}")
    print(f" [Tarefas    ] OK: {c:<7}| Falhas: {f:<6}| Heartbeats: {hb}")
    print(f" [Masters    ] Vizinhos: {len(NEIGHBORS):<4}| Saturacao: {sat_pct}% ({q_size}/{CAPACITY}) | {sat_status}")
    print(_SEP)


# ── Entrada manual de tarefas ─────────────────────────────────────────────

def input_loop():
    """Thread de input do usuario: aceita comandos para gerenciar tarefas manualmente."""
    print("\n[CMD] Comandos: 'test' (adicionar tarefa) | 'status' | 'sair'")
    while True:
        try:
            cmd = input("> ").strip().lower()
        except EOFError:
            break

        if cmd == 'test':
            user = f"task-{uuid.uuid4().hex[:6].upper()}"
            task_queue.put({'TASK': 'QUERY', 'USER': user})
            with task_enqueue_lock:
                task_enqueue_times.append(time.time())
            print(f"  [FILA] Tarefa '{user}' adicionada. Fila: {task_queue.qsize()}")

        elif cmd in ('status', 's'):
            print_dashboard()

        elif cmd in ('sair', 'quit', 'q', 'exit'):
            print("[CMD] Use Ctrl+C para encerrar o master.")

        elif cmd == '':
            continue

        else:
            print(f"  [?] Comando desconhecido: '{cmd}'")
            print("  [CMD] Disponiveis: 'test' (adicionar tarefa), 'status', 'sair'")


# ── Inicializacao ─────────────────────────────────────────────────────────

def start_master():
    global _my_address
    my_ip = get_my_ip()
    _my_address = f"{my_ip}:{PORT}"

    threading.Thread(target=discovery_listener,  daemon=True).start()
    threading.Thread(target=input_loop,          daemon=True).start()
    threading.Thread(target=announce_online,     daemon=True).start()
    threading.Thread(target=saturation_monitor,  daemon=True).start()
    threading.Thread(target=release_monitor,     daemon=True).start()
    threading.Thread(target=supervisor_reporter, daemon=True).start()

    server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

    try:
        server_sock.bind((HOST, PORT))
        server_sock.listen()

        print(f"\n=== Master {MASTER_UUID} ({MASTER_NAME}) Online ===")
        print(f"IP: {my_ip} | TCP: {PORT} | UDP discovery: {DISCOVERY_PORT}")
        print(f"Saturacao: >{CAPACITY} tarefas | Liberacao: <{RELEASE_THRESHOLD} tarefas")
        if NEIGHBORS:
            print(f"Vizinhos: {[n['master_id'] + '@' + n['address'] for n in NEIGHBORS]}")
        print("Aguardando conexoes...\n")

        while True:
            conn, addr = server_sock.accept()
            threading.Thread(
                target=handle_worker, args=(conn, addr), daemon=True
            ).start()

    except KeyboardInterrupt:
        print("\n[!] Master encerrado pelo usuario.")
        with stats_lock:
            print(f"    Heartbeats : {stats['heartbeats']}")
            print(f"    Concluidas : {stats['concluidas']}")
            print(f"    Falhas     : {stats['falhas']}")
    except Exception as e:
        print(f"[ERRO FATAL] {e}")
    finally:
        server_sock.close()


def main():
    parser = argparse.ArgumentParser(
        description='Master P2P com descoberta UDP e negociacao M2M (Sprint 3)'
    )
    parser.add_argument('--master-name', dest='master_name', default=None,
                        help='Nome deste master para eleicao (ex: MASTER_1)')
    parser.add_argument('--port', type=int, default=None,
                        help='Porta TCP (default: 7011)')
    parser.add_argument('--capacity', type=int, default=None,
                        help='Threshold de saturacao em tarefas (default: 10)')
    parser.add_argument('--neighbors', nargs='*', default=None,
                        help='Masters vizinhos no formato MASTER_ID@ip:porta')
    args = parser.parse_args()

    global MASTER_NAME, PORT, CAPACITY, RELEASE_THRESHOLD, NEIGHBORS
    if args.master_name:
        MASTER_NAME = args.master_name
    if args.port:
        PORT = args.port
    if args.capacity:
        CAPACITY = args.capacity
        RELEASE_THRESHOLD = max(1, int(CAPACITY * 0.6))
    if args.neighbors:
        for entry in args.neighbors:
            if '@' in entry:
                mid, addr = entry.split('@', 1)
                NEIGHBORS.append({"master_id": mid.strip(), "address": addr.strip()})

    start_master()


if __name__ == '__main__':
    main()
