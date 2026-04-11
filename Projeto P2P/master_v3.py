import socket
import json
import uuid
import threading
import queue
import time
import random

HOST        = '0.0.0.0'
PORT        = 5000
MASTER_UUID = f"MASTER-Enzio-{uuid.uuid4().hex[:4].upper()}"

# ── IPs dos workers (porta peer) para anunciar "master voltou" ────────
# Preencha com (IP, PEER_PORT) de cada worker antes de rodar.
WORKER_PEER_ADDRESSES = [
    # ('192.168.1.17', 5001),
    # ('192.168.1.18', 5001),
]

task_queue = queue.Queue()
stats      = {"concluidas": 0, "falhas": 0, "heartbeats": 0}
stats_lock = threading.Lock()


# ─────────────────────────────────────────────
#  Helpers de I/O (delimitador \n)
# ─────────────────────────────────────────────

def get_my_ip():
    """Retorna o IP real desta máquina (não 0.0.0.0)."""
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(('8.8.8.8', 80))
        ip = s.getsockname()[0]
        s.close()
        return ip
    except Exception:
        return '127.0.0.1'


def recv_message(conn):
    """Lê bytes até \\n e retorna o JSON parseado."""
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


# ─────────────────────────────────────────────
#  Anúncio de recuperação para os workers
# ─────────────────────────────────────────────

def announce_online():
    """
    Quando o master reinicia, avisa todos os workers (via porta peer)
    que o master original voltou. Workers em temp-master mode se rendem.
    """
    if not WORKER_PEER_ADDRESSES:
        print(" [ANUNCIO] Nenhum endereço peer configurado — workers se reconectarão via heartbeat.")
        return

    my_ip = get_my_ip()
    payload = {
        "MASTER":      "ONLINE",
        "MASTER_UUID": MASTER_UUID,
        "MASTER_IP":   my_ip,
        "MASTER_PORT": PORT
    }

    def _send(ip, peer_port):
        try:
            sock = socket.create_connection((ip, peer_port), timeout=3)
            sock.sendall((json.dumps(payload) + "\n").encode('utf-8'))
            sock.close()
            print(f" [ANUNCIO] Master online enviado para {ip}:{peer_port}")
        except Exception as e:
            print(f" [ANUNCIO] Falha ao anunciar para {ip}:{peer_port}: {e}")

    for ip, peer_port in WORKER_PEER_ADDRESSES:
        threading.Thread(target=_send, args=(ip, peer_port), daemon=True).start()


# ─────────────────────────────────────────────
#  Handler de cada Worker (sprint 1 + sprint 2)
# ─────────────────────────────────────────────

def handle_worker(conn, addr):
    try:
        message = recv_message(conn)
        if not message:
            return

        task_field   = message.get("TASK",   "").upper()
        worker_field = message.get("WORKER", "").upper()

        # ── Sprint 1: HEARTBEAT ──────────────────────────────────────
        if task_field == "HEARTBEAT":
            worker_id = message.get("SERVER_UUID", "?")
            print(f" [+] Heartbeat de: {worker_id} [{addr[0]}]")
            with stats_lock:
                stats["heartbeats"] += 1
            send_message(conn, {
                "SERVER_UUID": MASTER_UUID,
                "TASK":        "HEARTBEAT",
                "RESPONSE":    "ALIVE"
            })
            print(f" [->] ALIVE enviado para {worker_id}.")

        # ── Sprint 2: Apresentação do Worker ────────────────────────
        elif worker_field == "ALIVE":
            worker_uuid = message.get("WORKER_UUID")
            server_uuid = message.get("SERVER_UUID")   # só se emprestado

            if not worker_uuid:
                print(f" [!] Payload sem WORKER_UUID de {addr} — ignorado.")
                return

            if server_uuid:
                print(f" [+] Worker EMPRESTADO {worker_uuid} (de {server_uuid}) [{addr[0]}]")
            else:
                print(f" [+] Worker LOCAL {worker_uuid} [{addr[0]}]")

            try:
                task_data = task_queue.get_nowait()
                print(f" [FILA] Tarefa '{task_data['USER']}' → {worker_uuid}. "
                      f"Restam: {task_queue.qsize()}")
                send_message(conn, task_data)

                status_report = recv_message(conn)
                if not status_report:
                    return

                status        = status_report.get("STATUS", "").upper()
                reported_task = status_report.get("TASK",   "")
                reported_uuid = status_report.get("WORKER_UUID", worker_uuid)

                if status == "OK":
                    print(f" [OK]  {reported_uuid} concluiu '{reported_task}'.")
                    with stats_lock:
                        stats["concluidas"] += 1
                elif status == "NOK":
                    print(f" [NOK] {reported_uuid} falhou em '{reported_task}'.")
                    with stats_lock:
                        stats["falhas"] += 1

                send_message(conn, {"STATUS": "ACK", "WORKER_UUID": reported_uuid})
                print(f" [->] ACK enviado para {reported_uuid}.")

            except queue.Empty:
                print(f" [FILA] Sem tarefas para {worker_uuid}. Enviando NO_TASK.")
                send_message(conn, {"TASK": "NO_TASK"})

        else:
            print(f" [!] Mensagem desconhecida de {addr}: {message}")

    except json.JSONDecodeError:
        print(f" [ERRO] JSON inválido de {addr}")
    except Exception as e:
        print(f" [ERRO] {addr}: {e}")
    finally:
        conn.close()


# ─────────────────────────────────────────────
#  Gerador de tarefas simuladas
# ─────────────────────────────────────────────

def task_generator():
    users = ["Michel", "Ana", "Carlos", "Julia", "Pedro", "Mariana"]
    while True:
        time.sleep(15)
        user = random.choice(users)
        task_queue.put({"TASK": "QUERY", "USER": user})
        print(f" [GERADOR] Tarefa para '{user}' adicionada. Fila: {task_queue.qsize()}")


# ─────────────────────────────────────────────
#  Inicialização
# ─────────────────────────────────────────────

def start_master():
    for user in ["Michel", "Julia", "Carlos"]:
        task_queue.put({"TASK": "QUERY", "USER": user})
    print(f" [FILA] {task_queue.qsize()} tarefas iniciais carregadas.")

    threading.Thread(target=task_generator, daemon=True).start()

    # Anuncia aos workers que o master original está online novamente
    threading.Thread(target=announce_online, daemon=True).start()

    server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

    try:
        server_sock.bind((HOST, PORT))
        server_sock.listen()

        my_ip = get_my_ip()
        print(f"\n=== Master {MASTER_UUID} Online ===")
        print(f"IP real: {my_ip} | Porta: {PORT}")
        print("Aguardando Workers...\n")

        while True:
            conn, addr = server_sock.accept()
            threading.Thread(
                target=handle_worker, args=(conn, addr), daemon=True
            ).start()

    except KeyboardInterrupt:
        print("\n[!] Master encerrado pelo usuário.")
        with stats_lock:
            print(f"    Heartbeats  : {stats['heartbeats']}")
            print(f"    Concluídas  : {stats['concluidas']}")
            print(f"    Falhas      : {stats['falhas']}")
    except Exception as e:
        print(f"[ERRO FATAL] {e}")
    finally:
        server_sock.close()


if __name__ == "__main__":
    start_master()
