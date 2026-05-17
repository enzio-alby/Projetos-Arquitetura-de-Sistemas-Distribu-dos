"""Teste de integracao: worker descobre master via UDP, elege e faz handshake TCP.

Fluxo testado:
  Worker envia DISCOVERY UDP -> Fake master responde DISCOVERY_REPLY
  Worker elege MASTER_1 -> conecta TCP -> envia ELECTION_ACK -> recebe ACCEPTED
  Verifica: worker.master_ip aponta para o master eleito
"""
import os, sys
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

import socket, threading, time, json
import workerp2 as w_mod
from workerp2 import Worker

UDP_PORT = 15000    # porta UDP isolada para o teste
TCP_PORT = 17011    # porta TCP isolada para o teste
FAKE_MASTER_NAME = 'MASTER_1'
FAKE_MASTER_IP = '127.0.0.1'


def fake_udp_master(stop_event):
    """Fake master UDP: escuta DISCOVERY e responde com DISCOVERY_REPLY."""
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
    srv.settimeout(6)
    try:
        conn, _ = srv.accept()
        data = b''
        while True:
            ch = conn.recv(1)
            if not ch or ch == b'\n':
                break
            data += ch
        msg = json.loads(data.decode('utf-8'))
        results['type_ok'] = msg.get('TYPE') == 'ELECTION_ACK'
        results['selected'] = msg.get('SELECTED_MASTER')

        reply = {"TYPE": "ELECTION_ACK", "STATUS": "ACCEPTED", "MASTER_NAME": FAKE_MASTER_NAME}
        conn.sendall((json.dumps(reply) + '\n').encode('utf-8'))
        conn.close()
    except Exception as e:
        results['error'] = str(e)
    finally:
        srv.close()


def run():
    # Redirecionar constantes para portas isoladas
    w_mod.MASTER_IP = None
    w_mod.DISCOVERY_PORT = UDP_PORT
    w_mod.MASTER_PORT = TCP_PORT

    stop_event = threading.Event()
    results = {}

    threading.Thread(target=fake_udp_master, args=(stop_event,), daemon=True).start()
    threading.Thread(target=fake_tcp_master, args=(results, stop_event), daemon=True).start()
    time.sleep(0.3)

    w = Worker()
    connected = w._discovery_loop()

    time.sleep(0.5)
    stop_event.set()

    ok = (connected is True
          and results.get('type_ok') is True
          and results.get('selected') == FAKE_MASTER_NAME
          and w.master_ip == FAKE_MASTER_IP)

    print(f'run_discovery: {"PASS" if ok else "FAIL"}')
    print(f'  ELECTION_ACK recebido: {results.get("type_ok")}')
    print(f'  Master eleito: {results.get("selected")}')
    print(f'  worker.master_ip: {w.master_ip}:{w.master_port}')
    if 'error' in results:
        print(f'  Erro: {results["error"]}')


if __name__ == '__main__':
    run()
