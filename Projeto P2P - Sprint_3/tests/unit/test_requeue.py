import socket
import pytest
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

    assert conn_count[0] == 2, f"Esperado 2 conexões separadas, obtido {conn_count[0]}"
