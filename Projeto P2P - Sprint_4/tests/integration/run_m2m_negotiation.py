"""Teste de integracao Sprint 3 — Negociacao M2M completa.

Simula o ciclo completo:
  1. Master A satura (tarefas > CAPACITY)
  2. Master A envia request_help para Master B (este script como servidor)
  3. Este script responde com response_accepted
  4. Master B envia command_redirect para Worker (verificado por log)
  5. Worker envia register_temporary_worker para Master A (este script como servidor)

Como executar:
    # Janela 1 — Master A (com vizinho apontando para este script):
    python masterp2.py --master-name MASTER_A --capacity 2 --neighbors MASTER_B@127.0.0.1:7099

    # Janela 2 — este script (faz papel de Master B + verifica):
    python tests/integration/run_m2m_negotiation.py

    # Janela 3 — Worker (se quiser ver o redirect na pratica):
    python workerp2.py --master-host 127.0.0.1 --master-port 7011
"""
import socket
import json
import threading
import time
import uuid
import sys
import os

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

LISTEN_PORT = 7099
TIMEOUT = 30  # segundos maximos aguardando request_help

received = {
    "request_help": None,
    "register_temporary_worker": None,
    "notify_worker_returned": None,
}
lock = threading.Lock()
events = {k: threading.Event() for k in received}


def recv_msg(conn):
    data = b""
    conn.settimeout(10)
    while True:
        ch = conn.recv(1)
        if not ch:
            return None
        if ch == b'\n':
            break
        data += ch
    return json.loads(data.decode('utf-8'))


def send_msg(conn, payload):
    conn.sendall((json.dumps(payload) + "\n").encode('utf-8'))


def handle_conn(conn, addr):
    try:
        msg = recv_msg(conn)
        if not msg:
            return

        t = msg.get("type", "")

        if t == "request_help":
            print(f"[MASTER-B-SIM] request_help recebido de {addr[0]}:")
            print(f"    master_id     : {msg['payload'].get('master_id')}")
            print(f"    current_load  : {msg['payload'].get('current_load')}")
            print(f"    workers_needed: {msg['payload'].get('workers_needed')}")
            print(f"    request_id    : {msg.get('request_id','?')[:8]}")

            with lock:
                received["request_help"] = msg

            # Responder com response_accepted
            rid = msg.get("request_id", str(uuid.uuid4()))
            response = {
                "type": "response_accepted",
                "request_id": rid,
                "payload": {
                    "workers_offered": 1,
                    "worker_details": [{"id": "W-SIM-01", "address": "127.0.0.1:5001"}],
                },
            }
            send_msg(conn, response)
            print(f"[MASTER-B-SIM] response_accepted enviado (rid={rid[:8]})")
            events["request_help"].set()

        elif t == "register_temporary_worker":
            print(f"[MASTER-B-SIM] register_temporary_worker recebido de {addr[0]}:")
            print(f"    worker_id        : {msg['payload'].get('worker_id')}")
            print(f"    original_address : {msg['payload'].get('original_master_address')}")
            with lock:
                received["register_temporary_worker"] = msg
            events["register_temporary_worker"].set()

        elif t == "notify_worker_returned":
            print(f"[MASTER-B-SIM] notify_worker_returned recebido de {addr[0]}:")
            print(f"    worker_id: {msg['payload'].get('worker_id')}")
            with lock:
                received["notify_worker_returned"] = msg
            events["notify_worker_returned"].set()

        else:
            print(f"[MASTER-B-SIM] Mensagem desconhecida '{t}' de {addr[0]} — ignorada.")

    except Exception as e:
        print(f"[MASTER-B-SIM] Erro ao processar {addr}: {e}")
    finally:
        conn.close()


def run_server():
    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind(('0.0.0.0', LISTEN_PORT))
    srv.listen()
    srv.settimeout(1)
    print(f"[MASTER-B-SIM] Servidor M2M escutando em porta {LISTEN_PORT}")

    deadline = time.time() + TIMEOUT
    while time.time() < deadline:
        try:
            conn, addr = srv.accept()
            threading.Thread(target=handle_conn, args=(conn, addr), daemon=True).start()
        except socket.timeout:
            continue
        except Exception:
            break

    srv.close()


def main():
    print("=" * 60)
    print("Teste de integracao Sprint 3 — Simulacao Master B")
    print("=" * 60)
    print(f"Aguardando request_help do Master A na porta {LISTEN_PORT}...")
    print(f"(Timeout: {TIMEOUT}s)")
    print()

    t = threading.Thread(target=run_server, daemon=True)
    t.start()

    # Aguardar request_help
    got_rh = events["request_help"].wait(timeout=TIMEOUT)
    if not got_rh:
        print("\n[FALHA] Nenhum request_help recebido dentro do timeout.")
        print("  Verifique se o Master A esta rodando com --neighbors MASTER_B@127.0.0.1:7099")
        sys.exit(1)

    print("\n[SUCESSO] request_help recebido e response_accepted enviado.")
    print(f"  master_id    : {received['request_help']['payload'].get('master_id')}")
    print(f"  request_id   : {received['request_help'].get('request_id','?')[:8]}...")
    rid = received['request_help'].get('request_id', '')
    print()

    # Aguardar optional register_temporary_worker (worker precisa estar rodando)
    print("Aguardando register_temporary_worker (necessita worker ativo)...")
    got_rtw = events["register_temporary_worker"].wait(timeout=15)
    if got_rtw:
        print(f"[SUCESSO] register_temporary_worker recebido!")
        print(f"  worker_id : {received['register_temporary_worker']['payload'].get('worker_id')}")
        print(f"  origem    : {received['register_temporary_worker']['payload'].get('original_master_address')}")
    else:
        print("[INFO] register_temporary_worker nao recebido (worker pode nao estar ativo).")

    print()
    print("=" * 60)
    print("Verificacao do payload request_help:")
    rh = received["request_help"]
    checks = [
        ("type == request_help",      rh.get("type") == "request_help"),
        ("request_id presente",        bool(rh.get("request_id"))),
        ("payload.master_id presente", bool(rh.get("payload", {}).get("master_id"))),
        ("payload.current_load int",   isinstance(rh.get("payload", {}).get("current_load"), int)),
        ("payload.capacity int",       isinstance(rh.get("payload", {}).get("capacity"), int)),
        ("payload.workers_needed >= 1",rh.get("payload", {}).get("workers_needed", 0) >= 1),
    ]
    all_ok = True
    for desc, result in checks:
        status = "OK" if result else "FALHA"
        print(f"  [{status}] {desc}")
        if not result:
            all_ok = False
    print("=" * 60)
    if all_ok:
        print("RESULTADO: PASSOU")
    else:
        print("RESULTADO: FALHOU")
        sys.exit(1)


if __name__ == "__main__":
    main()
