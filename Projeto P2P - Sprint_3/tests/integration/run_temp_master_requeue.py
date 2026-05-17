"""Teste de integração: temp master reenfileira tarefas no master original.

Verifica que AMBAS as tarefas chegam ao master — uma conexão por mensagem,
fiel ao protocolo real do masterp2.py.
"""
import os
import sys

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

import socket
import threading
import json
import time

import workerp2 as w_mod
from workerp2 import Worker

TEST_PORT = 7011


def master_server(collected, stop_event):
    """Fake master: uma conexão por vez, lê UMA mensagem por conexão (como masterp2.py)."""
    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind(('0.0.0.0', TEST_PORT))
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


def run():
    collected = []
    stop_event = threading.Event()

    t = threading.Thread(target=master_server, args=(collected, stop_event), daemon=True)
    t.start()
    time.sleep(0.3)

    w_mod.MASTER_IP = '127.0.0.1'
    w_mod.MASTER_PORT = TEST_PORT

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
