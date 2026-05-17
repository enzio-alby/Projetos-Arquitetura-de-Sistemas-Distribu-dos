import socket
import pytest
import workerp2 as w_mod
from workerp2 import Worker

MAX_HB_FAILURES = w_mod.MAX_HB_FAILURES


class DummySock:
    def sendall(self, data):
        pass

    def close(self):
        pass


def test_heartbeat_resets_failures_on_alive(monkeypatch):
    w = Worker()
    with w._hb_lock:
        w._hb_failures = 2

    monkeypatch.setattr(socket, 'create_connection', lambda *a, **k: DummySock())
    monkeypatch.setattr(Worker, '_recv', lambda self, sock, timeout=5: {'RESPONSE': 'ALIVE'})

    w._heartbeat_once('127.0.0.1', 7011)

    assert w._hb_failures == 0


def test_heartbeat_increments_and_triggers_election_on_failure(monkeypatch):
    w = Worker()
    with w._hb_lock:
        w._hb_failures = MAX_HB_FAILURES - 1

    def raise_conn(*a, **k):
        raise ConnectionError("conn failed")

    monkeypatch.setattr(socket, 'create_connection', raise_conn)

    def fake_start(self):
        self._election_started = True

    monkeypatch.setattr(Worker, '_start_election', fake_start)

    w._heartbeat_once('127.0.0.1', 7011)

    assert getattr(w, '_election_started', False) is True
    assert w._hb_failures == 0
