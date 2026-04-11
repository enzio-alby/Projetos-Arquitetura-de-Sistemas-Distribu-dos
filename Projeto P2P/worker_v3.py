import socket
import json
import uuid
import time
import threading
import random
import queue as _queue

try:
    import psutil
    HAS_PSUTIL = True
except ImportError:
    HAS_PSUTIL = False

# ── Configuração ─────────────────────────────────────────────────────
MASTER_IP   = '192.168.1.16'   # IP do master original
MASTER_PORT = 5000

PEER_PORT   = 5001             # porta para comunicação worker ↔ worker

# IPs dos outros workers — preencha antes de rodar
PEER_IPS = [
    # '192.168.1.17',
    # '192.168.1.18',
]

HEARTBEAT_INTERVAL = 30   # s entre cada heartbeat
TASK_INTERVAL      = 5    # s entre cada ciclo de pedido de tarefa
MAX_HB_FAILURES    = 3    # falhas antes de iniciar eleição
ELECTION_WAIT      = 5    # s para coletar votos dos peers

# ── Estados possíveis do worker ──────────────────────────────────────
ST_NORMAL      = "NORMAL"
ST_ELECTING    = "ELECTING"
ST_TEMP_MASTER = "TEMP_MASTER"


# ─────────────────────────────────────────────
#  Utilitários
# ─────────────────────────────────────────────

def get_my_ip():
    """Retorna o IP local real desta máquina."""
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(('8.8.8.8', 80))
        ip = s.getsockname()[0]
        s.close()
        return ip
    except Exception:
        return '127.0.0.1'


def compute_score():
    """
    Calcula a pontuação desta máquina para a eleição de master.
    Critérios: CPU ociosa (40%) + memória disponível em GB (60%).
    Maior score = melhor candidato a master temporário.
    """
    if HAS_PSUTIL:
        cpu_idle   = 100 - psutil.cpu_percent(interval=0.5)
        mem_gb     = psutil.virtual_memory().available / (1024 ** 3)
        score      = cpu_idle * 0.4 + mem_gb * 10 * 0.6
        return round(score, 2)
    # Sem psutil: valor aleatório (garante desempate mesmo sem métricas reais)
    return round(random.uniform(10, 90), 2)


# ─────────────────────────────────────────────
#  Classe Worker
# ─────────────────────────────────────────────

class Worker:
    def __init__(self, borrowed_from: str = None):
        """
        borrowed_from: SERVER_UUID do Master original, caso este Worker
                       tenha sido emprestado a outro Master (payload 2.1b).
        """
        self.uuid          = f"WORKER-Enzio_worker-{uuid.uuid4().hex[:4].upper()}"
        self.borrowed_from = borrowed_from
        self.my_ip         = get_my_ip()
        self.running       = True

        # ── Estado (thread-safe) ──────────────────────────────────────
        self._state      = ST_NORMAL
        self._state_lock = threading.Lock()

        # ── Endereço do master atual (muda durante eleição/retorno) ───
        self._master_ip   = MASTER_IP
        self._master_port = MASTER_PORT
        self._master_lock = threading.Lock()

        # ── Controle de heartbeat ─────────────────────────────────────
        self._hb_failures  = 0
        self._hb_lock      = threading.Lock()

        # ── Eleição: votos recebidos {worker_uuid: (score, ip)} ──────
        self._votes      = {}
        self._votes_lock = threading.Lock()

        # ── Fila de tarefas quando este worker for temp master ────────
        self._temp_queue      = _queue.Queue()
        self._temp_server_sock = None

    # ── Properties thread-safe ───────────────────────────────────────

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
            print(f"[ESTADO] {old} → {value}")

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
            self._master_ip   = ip
            self._master_port = port

    # ── Helpers de I/O ───────────────────────────────────────────────

    def _recv(self, sock, timeout=5):
        """Lê até \\n com timeout e retorna JSON parseado."""
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
        """Serializa payload como JSON + \\n e envia."""
        sock.sendall((json.dumps(payload) + "\n").encode('utf-8'))

    # ── Comunicação com peers ─────────────────────────────────────────

    def _send_to_peer(self, ip, message):
        try:
            sock = socket.create_connection((ip, PEER_PORT), timeout=3)
            self._send(sock, message)
            sock.close()
        except Exception as e:
            print(f"[PEER] Falha ao contactar {ip}:{PEER_PORT} — {e}")

    def _broadcast(self, message):
        """Envia message para todos os peers conhecidos (paralelo)."""
        for ip in PEER_IPS:
            threading.Thread(
                target=self._send_to_peer,
                args=(ip, message),
                daemon=True
            ).start()

    # ─────────────────────────────────────────────
    #  Sprint 1 — Heartbeat
    # ─────────────────────────────────────────────

    def _heartbeat_loop(self):
        """
        Thread: envia heartbeat periodicamente.
        - Estado NORMAL      → master atual
        - Estado ELECTING    → master original (para detectar retorno)
        - Estado TEMP_MASTER → master original (para detectar retorno)
        """
        while self.running:
            # Se em eleição ou temp master, tentamos o master ORIGINAL
            if self.state in (ST_ELECTING, ST_TEMP_MASTER):
                target_ip   = MASTER_IP
                target_port = MASTER_PORT
            else:
                target_ip   = self.master_ip
                target_port = self.master_port

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

                    # ── Retorno do master original detectado via heartbeat ──
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
                        threading.Thread(
                            target=self._start_election, daemon=True
                        ).start()
            finally:
                if sock:
                    sock.close()

            time.sleep(HEARTBEAT_INTERVAL)

    # ─────────────────────────────────────────────
    #  Eleição de Master Temporário
    # ─────────────────────────────────────────────

    def _start_election(self):
        """
        Inicia o protocolo de eleição:
        1. Calcula score local e registra como candidato
        2. Faz broadcast para todos os peers com seu score
        3. Aguarda ELECTION_WAIT segundos para coletar votos
        4. Determina o vencedor e age de acordo
        """
        if self.state != ST_NORMAL:
            return   # já está em eleição ou é temp master

        self.state   = ST_ELECTING
        my_score     = compute_score()

        print(f"\n[ELEIÇÃO] Iniciando. Score desta máquina: {my_score}")

        # Registra o próprio voto
        with self._votes_lock:
            self._votes = {self.uuid: (my_score, self.my_ip)}

        # Broadcast para peers
        self._broadcast({
            "ELECTION":    "START",
            "WORKER_UUID": self.uuid,
            "SCORE":       my_score,
            "IP":          self.my_ip
        })

        # Aguarda votos
        time.sleep(ELECTION_WAIT)
        self._resolve_election()

    def _resolve_election(self):
        """Determina o vencedor após coletar todos os votos."""
        with self._votes_lock:
            votes = dict(self._votes)

        if not votes:
            print("[ELEIÇÃO] Nenhum peer respondeu. Este worker assume como master temporário.")
            winner_uuid = self.uuid
            winner_ip   = self.my_ip
        else:
            winner_uuid, (winner_score, winner_ip) = max(
                votes.items(), key=lambda x: x[1][0]
            )
            print(f"[ELEIÇÃO] Vencedor: {winner_uuid} | Score: {winner_score} | IP: {winner_ip}")

        # Anuncia o eleito para todos
        self._broadcast({
            "ELECTION":    "ELECTED",
            "MASTER_UUID": winner_uuid,
            "MASTER_IP":   winner_ip,
            "MASTER_PORT": MASTER_PORT
        })

        if winner_uuid == self.uuid:
            self._become_temp_master()
        else:
            # Outro worker ganhou — atualiza master e volta ao normal
            self.set_master(winner_ip, MASTER_PORT)
            self.state = ST_NORMAL
            print(f"[ELEIÇÃO] Conectando ao novo master temporário: {winner_ip}:{MASTER_PORT}")

    # ─────────────────────────────────────────────
    #  Master Temporário
    # ─────────────────────────────────────────────

    def _become_temp_master(self):
        """Este worker assume o papel de master temporário."""
        self.state = ST_TEMP_MASTER
        print(f"\n[TEMP MASTER] Este worker assumiu como master temporário!")
        print(f"[TEMP MASTER] Escutando em {self.my_ip}:{MASTER_PORT}")

        # Pré-popula a fila do temp master
        for user in ["Michel", "Julia", "Carlos"]:
            self._temp_queue.put({"TASK": "QUERY", "USER": user})

        threading.Thread(target=self._temp_master_server, daemon=True).start()

    def _temp_master_server(self):
        """Servidor TCP do master temporário — espelha a lógica do master_v3."""
        temp_uuid = f"TEMP-{self.uuid}"
        srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._temp_server_sock = srv

        try:
            srv.bind(('0.0.0.0', MASTER_PORT))
            srv.listen()
            srv.settimeout(1)   # permite checar state periodicamente
            print(f"[TEMP MASTER] Servidor {temp_uuid} iniciado.")

            while self.state == ST_TEMP_MASTER:
                try:
                    conn, addr = srv.accept()
                    threading.Thread(
                        target=self._handle_as_master,
                        args=(conn, addr, temp_uuid),
                        daemon=True
                    ).start()
                except socket.timeout:
                    continue   # re-checa o state

        except Exception as e:
            print(f"[TEMP MASTER] Erro no servidor: {e}")
        finally:
            srv.close()
            self._temp_server_sock = None
            print("[TEMP MASTER] Servidor encerrado.")

    def _handle_as_master(self, conn, addr, temp_uuid):
        """Processa uma conexão de worker enquanto este nó é temp master."""
        try:
            message = self._recv(conn)
            if not message:
                return

            task_field   = message.get("TASK",   "").upper()
            worker_field = message.get("WORKER", "").upper()

            # HEARTBEAT
            if task_field == "HEARTBEAT":
                worker_id = message.get("SERVER_UUID", "?")
                print(f"[TEMP MASTER] Heartbeat de {worker_id}")
                self._send(conn, {
                    "SERVER_UUID": temp_uuid,
                    "TASK":        "HEARTBEAT",
                    "RESPONSE":    "ALIVE"
                })

            # Apresentação de Worker
            elif worker_field == "ALIVE":
                worker_uuid = message.get("WORKER_UUID", "?")
                try:
                    task_data = self._temp_queue.get_nowait()
                    print(f"[TEMP MASTER] Tarefa '{task_data['USER']}' → {worker_uuid}")
                    self._send(conn, task_data)

                    status_report = self._recv(conn)
                    if status_report:
                        status = status_report.get("STATUS", "").upper()
                        print(f"[TEMP MASTER] Status de {worker_uuid}: {status}")
                        self._send(conn, {
                            "STATUS":      "ACK",
                            "WORKER_UUID": worker_uuid
                        })
                except _queue.Empty:
                    self._send(conn, {"TASK": "NO_TASK"})

        except Exception as e:
            print(f"[TEMP MASTER] Erro ao processar {addr}: {e}")
        finally:
            conn.close()

    def _on_original_master_returned(self):
        """
        Chamado quando o master original é detectado online.
        Encerra o servidor temporário e notifica todos os peers.
        """
        print(f"\n[RETORNO] Master original online! Voltando ao modo NORMAL...")
        self.state = ST_NORMAL
        self.set_master(MASTER_IP, MASTER_PORT)
        with self._hb_lock:
            self._hb_failures = 0

        # Notifica peers para também reconectarem ao master original
        self._broadcast({
            "MASTER":      "ONLINE",
            "MASTER_IP":   MASTER_IP,
            "MASTER_PORT": MASTER_PORT
        })
        print(f"[RETORNO] Peers notificados. Reconectando ao master original.")

    # ─────────────────────────────────────────────
    #  Servidor Peer (Worker ↔ Worker)
    # ─────────────────────────────────────────────

    def _peer_server(self):
        """
        Thread: escuta mensagens de outros workers na PEER_PORT.
        Processa votos de eleição, resultados e anúncios de retorno.
        """
        srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        srv.bind(('0.0.0.0', PEER_PORT))
        srv.listen()
        srv.settimeout(1)
        print(f"[PEER] Servidor peer em {self.my_ip}:{PEER_PORT}")

        while self.running:
            try:
                conn, _ = srv.accept()
                threading.Thread(
                    target=self._handle_peer,
                    args=(conn,),
                    daemon=True
                ).start()
            except socket.timeout:
                continue

        srv.close()

    def _handle_peer(self, conn):
        """Processa uma mensagem vinda de outro worker."""
        try:
            msg = self._recv(conn, timeout=5)
            if not msg:
                return

            election = msg.get("ELECTION", "").upper()
            master   = msg.get("MASTER",   "").upper()

            # ── Voto de eleição recebido ──────────────────────────────
            if election == "START":
                sender_uuid  = msg.get("WORKER_UUID")
                sender_score = msg.get("SCORE", 0)
                sender_ip    = msg.get("IP", "")
                with self._votes_lock:
                    self._votes[sender_uuid] = (sender_score, sender_ip)
                print(f"[ELEIÇÃO] Voto recebido: {sender_uuid} | score={sender_score}")

                # Também envia o próprio score de volta ao solicitante
                # (para que o eleitor nos contabilize mesmo que não tenhamos iniciado)
                self._send_to_peer(sender_ip, {
                    "ELECTION":    "START",
                    "WORKER_UUID": self.uuid,
                    "SCORE":       compute_score(),
                    "IP":          self.my_ip
                })

            # ── Resultado da eleição ──────────────────────────────────
            elif election == "ELECTED":
                master_uuid = msg.get("MASTER_UUID")
                master_ip   = msg.get("MASTER_IP")
                master_port = msg.get("MASTER_PORT", MASTER_PORT)

                if master_uuid != self.uuid and self.state != ST_TEMP_MASTER:
                    self.set_master(master_ip, master_port)
                    self.state = ST_NORMAL
                    print(f"[ELEIÇÃO] Master temporário eleito: {master_uuid} ({master_ip})")

            # ── Master original voltou ────────────────────────────────
            elif master == "ONLINE":
                master_ip   = msg.get("MASTER_IP",   MASTER_IP)
                master_port = msg.get("MASTER_PORT", MASTER_PORT)
                print(f"\n[RETORNO] Master original em {master_ip} voltou! Reconectando...")
                self.set_master(master_ip, master_port)
                self.state = ST_NORMAL   # encerra temp master server se ativo

        except Exception as e:
            print(f"[PEER] Erro ao processar mensagem: {e}")
        finally:
            conn.close()

    # ─────────────────────────────────────────────
    #  Sprint 2 — Ciclo de Tarefas
    # ─────────────────────────────────────────────

    def _request_task(self):
        """Apresenta-se ao master atual e executa o ciclo completo de tarefa."""
        if self.state != ST_NORMAL:
            return   # não solicita tarefas durante eleição ou sendo temp master

        sock = None
        try:
            sock = socket.create_connection(
                (self.master_ip, self.master_port), timeout=5
            )

            # Apresentação (payload 2.1 / 2.1b)
            presentation = {"WORKER": "ALIVE", "WORKER_UUID": self.uuid}
            if self.borrowed_from:
                presentation["SERVER_UUID"] = self.borrowed_from
            self._send(sock, presentation)
            print(f"[TAREFA] Apresentado em {self.master_ip}. Aguardando tarefa...")

            response = self._recv(sock)
            if not response:
                return

            task_type = response.get("TASK", "").upper()

            # Fila vazia
            if task_type == "NO_TASK":
                print("[TAREFA] Sem tarefas. Aguardando próximo ciclo...")
                return

            # Tarefa recebida
            if task_type == "QUERY":
                user = response.get("USER", "?")
                print(f"[TAREFA] Recebida: QUERY para '{user}'")

                proc_time = random.uniform(1, 4)
                print(f"[TAREFA] Processando... ({proc_time:.1f}s)")
                time.sleep(proc_time)

                success = random.random() > 0.1
                status  = "OK" if success else "NOK"
                print(f"[TAREFA] {'✓' if success else '✗'} {status}")

                self._send(sock, {
                    "STATUS":      status,
                    "TASK":        "QUERY",
                    "WORKER_UUID": self.uuid
                })

                ack = self._recv(sock)
                if ack and ack.get("STATUS", "").upper() == "ACK":
                    print("[TAREFA] ACK recebido. Ciclo concluído.\n")

        except socket.timeout:
            print("[TAREFA] Timeout — master demorou demais.")
        except ConnectionRefusedError:
            print("[TAREFA] Conexão recusada — master offline?")
        except Exception as e:
            print(f"[TAREFA] Erro: {e}")
        finally:
            if sock:
                sock.close()

    # ─────────────────────────────────────────────
    #  Loop Principal
    # ─────────────────────────────────────────────

    def run(self):
        print(f"\n=== Worker {self.uuid} Iniciado ===")
        print(f"    IP     : {self.my_ip}")
        print(f"    Master : {MASTER_IP}:{MASTER_PORT}")
        print(f"    Peers  : {PEER_IPS if PEER_IPS else 'Nenhum configurado'}")
        if not HAS_PSUTIL:
            print("    [AVISO] psutil não encontrado — score de eleição será aleatório.")
            print("            Instale com: pip install psutil")
        print()

        # Servidor peer (comunicação worker ↔ worker)
        threading.Thread(target=self._peer_server, daemon=True).start()

        # Heartbeat ao master (Sprint 1 + detecção de falha/retorno)
        threading.Thread(target=self._heartbeat_loop, daemon=True).start()

        # Loop de tarefas (Sprint 2)
        while self.running:
            self._request_task()
            time.sleep(TASK_INTERVAL)


if __name__ == "__main__":
    # Para worker emprestado: Worker(borrowed_from="MASTER-Enzio-XXXX")
    w = Worker()
    w.run()
