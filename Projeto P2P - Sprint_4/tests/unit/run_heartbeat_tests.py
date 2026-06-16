import os
import sys
import socket

# Adiciona a raiz do projeto ao path para permitir imports locais
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from worker.worker import Worker, MAX_HB_FAILURES


class DummySock:
    def sendall(self, data):
        pass

    def close(self):
        pass


def test_reset_on_alive():
    w = Worker()
    with w._hb_lock:
        w._hb_failures = 2

    orig_create = socket.create_connection
    orig_recv = Worker._recv

    try:
        socket.create_connection = lambda *a, **k: DummySock()
        Worker._recv = lambda self, sock, timeout=5: {'RESPONSE': 'ALIVE'}
        w._heartbeat_once('127.0.0.1', 5000)
        assert w._hb_failures == 0
        print('test_reset_on_alive: PASS')
    except AssertionError:
        print('test_reset_on_alive: FAIL')
    finally:
        socket.create_connection = orig_create
        Worker._recv = orig_recv


def test_increment_and_trigger_election_on_failure():
    w = Worker()
    with w._hb_lock:
        w._hb_failures = MAX_HB_FAILURES - 1

    orig_create = socket.create_connection
    orig_start = Worker._start_election

    def raise_conn(*a, **k):
        raise ConnectionError('conn failed')

    def fake_start(self):
        self._election_started = True

    try:
        socket.create_connection = raise_conn
        Worker._start_election = fake_start
        w._heartbeat_once('127.0.0.1', 5000)
        ok = getattr(w, '_election_started', False) is True and w._hb_failures == 0
        print('test_increment_and_trigger_election_on_failure: {}'.format('PASS' if ok else 'FAIL'))
    finally:
        socket.create_connection = orig_create
        Worker._start_election = orig_start


if __name__ == '__main__':
    test_reset_on_alive()
    test_increment_and_trigger_election_on_failure()
