"""Teste de integração: temp master serve tarefas a workers.

Verifica o ciclo completo: worker pede tarefa → temp master distribui →
worker reporta status → temp master responde ACK.
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

import worker as w_mod
from worker import Worker

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

        resp = send_recv(sock, {'WORKER': 'ALIVE', 'WORKER_UUID': 'TEST-WORKER-001'})
        results['task_received'] = resp.get('TASK') == 'QUERY' and resp.get('USER') == 'TestUser'

        ack = send_recv(sock, {'STATUS': 'OK', 'TASK': 'QUERY', 'WORKER_UUID': 'TEST-WORKER-001'})
        results['ack_received'] = ack.get('STATUS') == 'ACK'

        sock.close()
    except Exception as e:
        results['error'] = str(e)

    w.state = 'NORMAL'
    time.sleep(0.2)

    ok = results.get('task_received') and results.get('ack_received')
    print(f'run_temp_master_serve: {"PASS" if ok else "FAIL"} | detalhes: {results}')


if __name__ == '__main__':
    run()
