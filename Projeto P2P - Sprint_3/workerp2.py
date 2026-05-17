"""Standalone workerp2 — Worker com descoberta dinamica UDP (Sprint 2.1).

Usage:
    python workerp2.py                              # modo discovery (sem IP pre-configurado)
    python workerp2.py --master-host 192.168.1.10  # conexao direta (sem discovery)

Sprint 2.1: worker descobre masters via UDP broadcast, elege pelo menor MASTER_NAME
lexicografico e conecta via TCP com handshake ELECTION_ACK antes de iniciar heartbeat.
"""
import socket
import json
import uuid
import time
import threading
import random
import queue as _queue
import argparse

try:
    import psutil
    HAS_PSUTIL = True
except Exception:
    HAS_PSUTIL = False

# ── Configuracao base ─────────────────────────────────────────────────
MASTER_IP = None        # None = modo discovery ativo; forneca --master-host para skip
MASTER_PORT = 7011

PEER_PORT = 5001
PEER_IPS = []

HEARTBEAT_INTERVAL = 30
TASK_INTERVAL = 5
MAX_HB_FAILURES = 3
ELECTION_WAIT = 5

# ── Configuracao de descoberta UDP (Sprint 2.1) ───────────────────────
DISCOVERY_PORT = 5000
DISCOVERY_MULTICAST = '239.255.255.250'
DISCOVERY_WAIT = 3      # segundos para coletar respostas DISCOVERY_REPLY

ST_NORMAL = "NORMAL"
ST_ELECTING = "ELECTING"
ST_TEMP_MASTER = "TEMP_MASTER"


def get_my_ip():
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(('8.8.8.8', 80))
        ip = s.getsockname()[0]
        s.close()
        return ip
    except Exception:
        return '127.0.0.1'


def compute_score():
    if HAS_PSUTIL:
        cpu_idle = 100 - psutil.cpu_percent(interval=0.5)
        mem_gb = psutil.virtual_memory().available / (1024 ** 3)
        score = cpu_idle * 0.4 + mem_gb * 10 * 0.6
        return round(score, 2)
    return round(random.uniform(10, 90), 2)


class Worker:
    def __init__(self, borrowed_from: str = None):
        self.uuid = f"WORKER-Enzio_worker-{uuid.uuid4().hex[:4].upper()}"
        self.borrowed_from = borrowed_from
        self.my_ip = get_my_ip()
        self.running = True

        self._state = ST_NORMAL
        self._state_lock = threading.Lock()

        self._master_ip = MASTER_IP
        self._master_port = MASTER_PORT
        self._master_lock = threading.Lock()

        self._hb_failures = 0
        self._hb_lock = threading.Lock()

        self._votes = {}
        self._votes_lock = threading.Lock()

        self._temp_queue = _queue.Queue()
        self._temp_server_sock = None

    @property
    def state(self):
        with self._state_lock:
            return self._state

    @state.setter
    def state(self, value):
        with self._state_lock:
            old = self._state
            self._state = value
        if old != value:
            print(f"[ESTADO] {old} -> {value}")

    @property
    def master_ip(self):
        with self._master_lock:
            return self._master_ip

    @property
    def master_port(self):
        with self._master_lock:
            return self._master_port

    def set_master(self, ip, port):
        with self._master_lock:
            self._master_ip = ip
            self._master_port = port

    def _recv(self, sock, timeout=5):
        sock.settimeout(timeout)
        data = b""
        while True:
            ch = sock.recv(1)
            if not ch:
                return None
            if ch == b'\n':
                break
            data += ch
        return json.loads(data.decode('utf-8'))

    def _send(self, sock, payload):
        sock.sendall((json.dumps(payload) + "\n").encode('utf-8'))

    def _send_to_peer(self, ip, message):
        try:
            sock = socket.create_connection((ip, PEER_PORT), timeout=3)
            self._send(sock, message)
            sock.close()
        except Exception as e:
            print(f"[PEER] Falha ao contactar {ip}:{PEER_PORT} — {e}")

    def _broadcast(self, message):
        for ip in PEER_IPS:
            threading.Thread(target=self._send_to_peer, args=(ip, message), daemon=True).start()

    def _heartbeat_loop(self):
        while self.running:
            if self.state in (ST_ELECTING, ST_TEMP_MASTER):
                target_ip = MASTER_IP
                target_port = MASTER_PORT
            else:
                target_ip = self.master_ip
                target_port = self.master_port

            self._heartbeat_once(target_ip, target_port)

            with self._hb_lock:
                failures = self._hb_failures

            backoff_multiplier = min(1.5 ** failures, 8)
            sleep_time = HEARTBEAT_INTERVAL * backoff_multiplier
            time.sleep(sleep_time)

    def _heartbeat_once(self, target_ip, target_port):
        sock = None
        try:
            sock = socket.create_connection((target_ip, target_port), timeout=5)
            self._send(sock, {"SERVER_UUID": self.uuid, "TASK": "HEARTBEAT"})
            response = self._recv(sock)

            if response and response.get("RESPONSE") == "ALIVE":
                with self._hb_lock:
                    self._hb_failures = 0

                if self.state == ST_NORMAL:
                    print(f"[HB] Master respondeu: ALIVE")
                elif self.state == ST_TEMP_MASTER:
                    print(f"\n[HB] Master original detectado online em {target_ip}!")
                    self._on_original_master_returned()

        except Exception as e:
            if self.state == ST_NORMAL:
                with self._hb_lock:
                    self._hb_failures += 1
                    failures = self._hb_failures

                print(f"[HB] Falha #{failures}: {e}")

                if failures >= MAX_HB_FAILURES:
                    with self._hb_lock:
                        self._hb_failures = 0
                    print(f"[HB] {MAX_HB_FAILURES} falhas consecutivas. Iniciando eleição...")
                    threading.Thread(target=self._start_election, daemon=True).start()
        finally:
            if sock:
                try:
                    sock.close()
                except Exception:
                    pass

    def _start_election(self):
        if self.state != ST_NORMAL:
            return

        self.state = ST_ELECTING
        my_score = compute_score()

        print(f"\n[ELEIÇÃO] Iniciando. Score desta máquina: {my_score}")

        with self._votes_lock:
            self._votes = {self.uuid: (my_score, self.my_ip)}

        self._broadcast({
            "ELECTION": "START",
            "WORKER_UUID": self.uuid,
            "SCORE": my_score,
            "IP": self.my_ip
        })

        time.sleep(ELECTION_WAIT)
        self._resolve_election()

    def _resolve_election(self):
        with self._votes_lock:
            votes = dict(self._votes)

        if not votes:
            print("[ELEIÇÃO] Nenhum peer respondeu. Este worker assume como master temporário.")
            winner_uuid = self.uuid
            winner_ip = self.my_ip
        else:
            winner_uuid, (winner_score, winner_ip) = max(votes.items(), key=lambda x: (x[1][0], x[0]))
            print(f"[ELEIÇÃO] Vencedor: {winner_uuid} | Score: {winner_score} | IP: {winner_ip}")

        self._broadcast({
            "ELECTION": "ELECTED",
            "MASTER_UUID": winner_uuid,
            "MASTER_IP": winner_ip,
            "MASTER_PORT": MASTER_PORT
        })

        if winner_uuid == self.uuid:
            self._become_temp_master()
        else:
            self.set_master(winner_ip, MASTER_PORT)
            self.state = ST_NORMAL
            print(f"[ELEIÇÃO] Conectando ao novo master temporário: {winner_ip}:{MASTER_PORT}")

    def _become_temp_master(self):
        self.state = ST_TEMP_MASTER
        print(f"\n[TEMP MASTER] Este worker assumiu como master temporário!")
        print(f"[TEMP MASTER] Escutando em {self.my_ip}:{MASTER_PORT}")

        for user in ["Michel", "Julia", "Carlos"]:
            self._temp_queue.put({"TASK": "QUERY", "USER": user})

        threading.Thread(target=self._temp_master_server, daemon=True).start()

    def _temp_master_server(self):
        temp_uuid = f"TEMP-{self.uuid}"
        srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._temp_server_sock = srv

        try:
            srv.bind(('0.0.0.0', MASTER_PORT))
            srv.listen()
            srv.settimeout(1)
            print(f"[TEMP MASTER] Servidor {temp_uuid} iniciado.")

            while self.state == ST_TEMP_MASTER:
                try:
                    conn, addr = srv.accept()
                    threading.Thread(target=self._handle_as_master, args=(conn, addr, temp_uuid), daemon=True).start()
                except socket.timeout:
                    continue

        except Exception as e:
            print(f"[TEMP MASTER] Erro no servidor: {e}")
        finally:
            srv.close()
            self._temp_server_sock = None
            print("[TEMP MASTER] Servidor encerrado.")

    def _handle_as_master(self, conn, addr, temp_uuid):
        try:
            message = self._recv(conn)
            if not message:
                return

            task_field = message.get("TASK", "").upper()
            worker_field = message.get("WORKER", "").upper()

            if task_field == "HEARTBEAT":
                worker_id = message.get("SERVER_UUID", "?")
                print(f"[TEMP MASTER] Heartbeat de {worker_id}")
                self._send(conn, {"SERVER_UUID": temp_uuid, "TASK": "HEARTBEAT", "RESPONSE": "ALIVE"})

            elif worker_field == "ALIVE":
                worker_uuid = message.get("WORKER_UUID", "?")
                try:
                    task_data = self._temp_queue.get_nowait()
                    print(f"[TEMP MASTER] Tarefa '{task_data['USER']}' -> {worker_uuid}")
                    self._send(conn, task_data)

                    status_report = self._recv(conn)
                    if status_report:
                        status = status_report.get("STATUS", "").upper()
                        print(f"[TEMP MASTER] Status de {worker_uuid}: {status}")
                        self._send(conn, {"STATUS": "ACK", "WORKER_UUID": worker_uuid})
                except _queue.Empty:
                    self._send(conn, {"TASK": "NO_TASK"})

        except Exception as e:
            print(f"[TEMP MASTER] Erro ao processar {addr}: {e}")
        finally:
            conn.close()

    def _on_original_master_returned(self):
        print(f"\n[RETORNO] Master original online! Voltando ao modo NORMAL...")
        self.state = ST_NORMAL
        # Use host/name (MASTER_IP may be a hostname) and port
        self.set_master(MASTER_IP, MASTER_PORT)
        with self._hb_lock:
            self._hb_failures = 0

        if self._temp_server_sock:
            try:
                self._temp_server_sock.close()
            except Exception:
                pass

        try:
            self._requeue_tasks_to_master()
        except Exception as e:
            print(f"[RETORNO] Falha ao reencaminhar tarefas: {e}")
        # Broadcast the master identity using both name and resolved IP for compatibility
        try:
            resolved = socket.gethostbyname(MASTER_IP)
        except Exception:
            resolved = MASTER_IP

        self._broadcast({
            "MASTER": "ONLINE",
            "MASTER_NAME": MASTER_IP,
            "MASTER_IP": resolved,
            "MASTER_PORT": MASTER_PORT,
        })
        print(f"[RETORNO] Peers notificados. Reconectando ao master original.")

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

        # Uma conexão por tarefa — o master lê apenas uma mensagem por conexão.
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

    # ─────────────────────────────────────────────
    #  Sprint 2.1 — Descoberta Dinamica UDP
    # ─────────────────────────────────────────────

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

    def _elect_master(self, masters):
        """Elege master pelo menor MASTER_NAME lexicografico (determinístico sem comunicacao entre workers)."""
        if not masters:
            return None
        elected = sorted(masters, key=lambda m: m.get('MASTER_NAME', ''))[0]
        print(f"[ELECTION] Master eleito: {elected.get('MASTER_NAME')} "
              f"({elected.get('MASTER_IP')}:{elected.get('MASTER_PORT')})")
        return elected

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

    def _discovery_loop(self):
        """Descobre masters, elege e conecta com backoff exponencial. Retorna True quando conectado."""
        backoff = 1
        max_backoff = 60

        while self.running:
            print(f"\n[DISCOVERY] Iniciando descoberta de masters na rede...")
            masters = self._discover_masters()

            if not masters:
                print(f"[FALLBACK] NO_MASTER_FOUND. Aguardando {backoff}s antes de tentar novamente...")
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

    def _peer_server(self):
        srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        srv.bind(('0.0.0.0', PEER_PORT))
        srv.listen()
        srv.settimeout(1)
        print(f"[PEER] Servidor peer em {self.my_ip}:{PEER_PORT}")

        while self.running:
            try:
                conn, _ = srv.accept()
                threading.Thread(target=self._handle_peer, args=(conn,), daemon=True).start()
            except socket.timeout:
                continue

        srv.close()

    def _handle_peer(self, conn):
        try:
            msg = self._recv(conn, timeout=5)
            if not msg:
                return

            election = msg.get("ELECTION", "").upper()
            master = msg.get("MASTER", "").upper()

            if election == "START":
                sender_uuid = msg.get("WORKER_UUID")
                sender_score = msg.get("SCORE", 0)
                sender_ip = msg.get("IP", "")
                with self._votes_lock:
                    self._votes[sender_uuid] = (sender_score, sender_ip)
                print(f"[ELEIÇÃO] Voto recebido: {sender_uuid} | score={sender_score}")

                self._send_to_peer(sender_ip, {"ELECTION": "START", "WORKER_UUID": self.uuid, "SCORE": compute_score(), "IP": self.my_ip})

            elif election == "ELECTED":
                master_uuid = msg.get("MASTER_UUID")
                # Accept either MASTER_NAME or MASTER_IP
                master_host = msg.get("MASTER_NAME") or msg.get("MASTER_IP") or MASTER_IP
                master_port = msg.get("MASTER_PORT", MASTER_PORT)

                if master_uuid != self.uuid and self.state != ST_TEMP_MASTER:
                    self.set_master(master_host, master_port)
                    self.state = ST_NORMAL
                    print(f"[ELEIÇÃO] Master temporário eleito: {master_uuid} ({master_host})")

            elif master == "ONLINE":
                master_host = msg.get("MASTER_NAME") or msg.get("MASTER_IP", MASTER_IP)
                master_port = msg.get("MASTER_PORT", MASTER_PORT)
                print(f"\n[RETORNO] Master original em {master_host} voltou! Reconectando...")
                self.set_master(master_host, master_port)
                self.state = ST_NORMAL

        except Exception as e:
            print(f"[PEER] Erro ao processar mensagem: {e}")
        finally:
            conn.close()

    def _handle_command_redirect(self, msg):
        """Sprint 3: Processa command_redirect — muda master e registra no novo master."""
        payload = msg.get("payload", {})
        new_master_address = payload.get("new_master_address", "")

        if not new_master_address:
            print(f"[REDIRECT] Payload invalido (sem new_master_address): {msg}")
            return

        # Guardar endereco do master atual como origem
        old_address = f"{self.master_ip}:{self.master_port}"
        self.borrowed_from = old_address

        # Parsear novo endereco
        parts = new_master_address.rsplit(':', 1)
        if len(parts) != 2:
            print(f"[REDIRECT] Endereco invalido: {new_master_address}")
            return
        new_ip, new_port_str = parts
        try:
            new_port = int(new_port_str)
        except ValueError:
            print(f"[REDIRECT] Porta invalida em: {new_master_address}")
            return

        print(f"[REDIRECT] Redirecionando: {old_address} -> {new_master_address}")
        self.set_master(new_ip, new_port)

        # Registrar no novo master imediatamente
        self._register_temporary_worker(new_ip, new_port, old_address)

    def _handle_command_release(self, msg):
        """Sprint 3: Processa command_release — volta ao master original."""
        payload = msg.get("payload", {})
        original_address = payload.get("original_master_address", "")

        if not original_address:
            print(f"[RELEASE] Payload invalido (sem original_master_address): {msg}")
            return

        print(f"[RELEASE] Liberado! Voltando para master original: {original_address}")
        self.borrowed_from = None

        parts = original_address.rsplit(':', 1)
        if len(parts) == 2:
            orig_ip, orig_port_str = parts
            try:
                orig_port = int(orig_port_str)
                self.set_master(orig_ip, orig_port)
                print(f"[RELEASE] Master resetado para {original_address}.")
            except ValueError:
                print(f"[RELEASE] Porta invalida em: {original_address}")

    def _register_temporary_worker(self, ip, port, original_address):
        """Sprint 3: Abre conexao com novo master e envia register_temporary_worker."""
        try:
            sock = socket.create_connection((ip, port), timeout=5)
            reg_msg = {
                "type": "register_temporary_worker",
                "request_id": str(uuid.uuid4()),
                "payload": {
                    "worker_id": self.uuid,
                    "original_master_address": original_address,
                },
            }
            self._send(sock, reg_msg)
            sock.close()
            print(f"[REDIRECT] register_temporary_worker enviado para {ip}:{port}"
                  f" (origem: {original_address})")
        except Exception as e:
            print(f"[REDIRECT] Falha ao registrar no novo master {ip}:{port}: {e}")

    def _request_task(self):
        if self.state != ST_NORMAL:
            return

        sock = None
        try:
            sock = socket.create_connection((self.master_ip, self.master_port), timeout=5)

            presentation = {"WORKER": "ALIVE", "WORKER_UUID": self.uuid}
            if self.borrowed_from:
                presentation["SERVER_UUID"] = self.borrowed_from
            self._send(sock, presentation)
            print(f"[TAREFA] Apresentado em {self.master_ip}. Aguardando tarefa...")

            response = self._recv(sock)
            if not response:
                return

            # Sprint 3: Verificar comandos M2M antes dos tipos de tarefa normais
            msg_type = response.get("type", "")
            if msg_type == "command_redirect":
                print(f"[REDIRECT] command_redirect recebido de {self.master_ip}.")
                self._handle_command_redirect(response)
                return
            if msg_type == "command_release":
                print(f"[RELEASE] command_release recebido de {self.master_ip}.")
                self._handle_command_release(response)
                return

            task_type = response.get("TASK", "").upper()

            if task_type == "NO_TASK":
                print("[TAREFA] Sem tarefas. Aguardando proximo ciclo...")
                return

            if task_type == "QUERY":
                user = response.get("USER", "?")
                print(f"[TAREFA] Recebida: QUERY para '{user}'")

                proc_time = random.uniform(1, 4)
                print(f"[TAREFA] Processando... ({proc_time:.1f}s)")
                time.sleep(proc_time)

                success = random.random() > 0.1
                status = "OK" if success else "NOK"
                print(f"[TAREFA] Status: {status}")

                self._send(sock, {"STATUS": status, "TASK": "QUERY", "WORKER_UUID": self.uuid})

                ack = self._recv(sock)
                if ack and ack.get("STATUS", "").upper() == "ACK":
                    print("[TAREFA] ACK recebido. Ciclo concluido.\n")

        except socket.timeout:
            print("[TAREFA] Timeout — master demorou demais.")
        except ConnectionRefusedError:
            print("[TAREFA] Conexao recusada — master offline?")
        except Exception as e:
            print(f"[TAREFA] Erro: {e}")
        finally:
            if sock:
                sock.close()

    def run(self):
        print(f"\n=== Worker {self.uuid} Iniciado ===")
        print(f"    IP     : {self.my_ip}")
        if self.master_ip is None:
            print(f"    Master : nao configurado — modo discovery ativo")
        else:
            print(f"    Master : {self.master_ip}:{self.master_port}")
        print(f"    Peers  : {PEER_IPS if PEER_IPS else 'Nenhum configurado'}")
        if not HAS_PSUTIL:
            print("    [AVISO] psutil nao encontrado — score de eleicao sera aleatorio.")
        print()

        # Sprint 2.1: se nenhum master pre-configurado, descobrir na rede primeiro
        if self.master_ip is None:
            print("[DISCOVERY] Buscando masters na rede via UDP...")
            if not self._discovery_loop():
                print("[ERRO] Nenhum master encontrado. Encerrando.")
                return

        threading.Thread(target=self._peer_server, daemon=True).start()
        threading.Thread(target=self._heartbeat_loop, daemon=True).start()

        while self.running:
            self._request_task()
            time.sleep(TASK_INTERVAL)


def main():
    parser = argparse.ArgumentParser(description='Worker P2P com descoberta dinamica UDP')
    parser.add_argument('--master-host', '--master-ip', dest='master_host', default=None,
                        help='IP ou hostname do master (omitir para usar discovery automatico)')
    parser.add_argument('--master-port', type=int, default=None,
                        help='Porta TCP do master (default: 7011)')
    parser.add_argument('--peer-ips', nargs='*', default=None,
                        help='IPs de outros workers para eleicao de temp master')
    args = parser.parse_args()

    global MASTER_IP, MASTER_PORT, PEER_IPS
    if args.master_host:
        MASTER_IP = args.master_host
    if args.master_port:
        MASTER_PORT = args.master_port
    if args.peer_ips:
        PEER_IPS = args.peer_ips

    w = Worker()
    w.run()


if __name__ == '__main__':
    main()
