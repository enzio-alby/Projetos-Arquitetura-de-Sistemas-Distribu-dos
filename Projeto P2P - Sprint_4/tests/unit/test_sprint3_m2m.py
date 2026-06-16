"""Testes unitarios Sprint 3 — Protocolo M2M e redirecionamento de Workers."""
import socket
import threading
import json
import uuid
import pytest
import worker as w_mod
from worker import Worker


# ─────────────────────────────────────────────────────────────────────────────
# Helpers compartilhados
# ─────────────────────────────────────────────────────────────────────────────

class FakeSock:
    """Socket falso para testes."""
    def __init__(self, responses=None):
        self._responses = list(responses or [])
        self.sent = []

    def sendall(self, data):
        self.sent.append(data)

    def settimeout(self, t):
        pass

    def recv(self, n):
        return b''

    def close(self):
        pass


# ─────────────────────────────────────────────────────────────────────────────
# Testes: make_m2m_msg (masterp2)
# ─────────────────────────────────────────────────────────────────────────────

def test_make_m2m_msg_structure():
    import master
    msg = master.make_m2m_msg("request_help", "rid-123", {"workers_needed": 2})
    assert msg["type"] == "request_help"
    assert msg["request_id"] == "rid-123"
    assert msg["payload"]["workers_needed"] == 2


def test_make_m2m_msg_generates_request_id_when_none():
    import master
    msg = master.make_m2m_msg("response_accepted")
    assert "request_id" in msg
    assert len(msg["request_id"]) > 0


# ─────────────────────────────────────────────────────────────────────────────
# Testes: Worker._handle_command_redirect
# ─────────────────────────────────────────────────────────────────────────────

def test_handle_command_redirect_sets_new_master():
    w = Worker()
    w.set_master("192.168.1.1", 7011)

    registered = []

    def fake_register(ip, port, original):
        registered.append((ip, port, original))

    w._register_temporary_worker = fake_register

    msg = {
        "type": "command_redirect",
        "request_id": str(uuid.uuid4()),
        "payload": {"new_master_address": "10.0.0.5:7012"},
    }
    w._handle_command_redirect(msg)

    assert w.master_ip == "10.0.0.5"
    assert w.master_port == 7012


def test_handle_command_redirect_sets_borrowed_from():
    w = Worker()
    w.set_master("192.168.1.1", 7011)

    w._register_temporary_worker = lambda ip, port, original: None

    msg = {
        "type": "command_redirect",
        "request_id": str(uuid.uuid4()),
        "payload": {"new_master_address": "10.0.0.5:7012"},
    }
    w._handle_command_redirect(msg)

    assert w.borrowed_from == "192.168.1.1:7011"


def test_handle_command_redirect_calls_register(monkeypatch):
    w = Worker()
    w.set_master("192.168.1.1", 7011)

    calls = []

    def fake_register(ip, port, original):
        calls.append({"ip": ip, "port": port, "original": original})

    w._register_temporary_worker = fake_register

    msg = {
        "type": "command_redirect",
        "request_id": str(uuid.uuid4()),
        "payload": {"new_master_address": "10.0.0.5:7012"},
    }
    w._handle_command_redirect(msg)

    assert len(calls) == 1
    assert calls[0]["ip"] == "10.0.0.5"
    assert calls[0]["port"] == 7012
    assert calls[0]["original"] == "192.168.1.1:7011"


def test_handle_command_redirect_invalid_address():
    """Endereco sem porta nao deve travar o worker."""
    w = Worker()
    w.set_master("192.168.1.1", 7011)
    original_ip = w.master_ip

    msg = {
        "type": "command_redirect",
        "request_id": str(uuid.uuid4()),
        "payload": {"new_master_address": "sem-porta"},
    }
    w._handle_command_redirect(msg)  # nao deve lancar excecao

    # Master nao deve ter mudado
    assert w.master_ip == original_ip


def test_handle_command_redirect_missing_payload():
    """Payload vazio nao deve travar o worker."""
    w = Worker()
    msg = {"type": "command_redirect", "request_id": "x", "payload": {}}
    w._handle_command_redirect(msg)  # nao deve lancar excecao


# ─────────────────────────────────────────────────────────────────────────────
# Testes: Worker._handle_command_release
# ─────────────────────────────────────────────────────────────────────────────

def test_handle_command_release_resets_borrowed_from():
    w = Worker()
    w.borrowed_from = "192.168.1.1:7011"
    w.set_master("10.0.0.5", 7012)

    msg = {
        "type": "command_release",
        "request_id": str(uuid.uuid4()),
        "payload": {"original_master_address": "192.168.1.1:7011"},
    }
    w._handle_command_release(msg)

    assert w.borrowed_from is None


def test_handle_command_release_restores_original_master():
    w = Worker()
    w.set_master("10.0.0.5", 7012)

    msg = {
        "type": "command_release",
        "request_id": str(uuid.uuid4()),
        "payload": {"original_master_address": "192.168.1.1:7011"},
    }
    w._handle_command_release(msg)

    assert w.master_ip == "192.168.1.1"
    assert w.master_port == 7011


def test_handle_command_release_missing_payload():
    """Payload sem original_master_address nao deve travar o worker.
    borrowed_from NAO e limpo porque sem endereco nao ha como reconectar."""
    w = Worker()
    w.borrowed_from = "192.168.1.1:7011"
    msg = {"type": "command_release", "request_id": "x", "payload": {}}
    w._handle_command_release(msg)  # nao deve lancar excecao
    # borrowed_from permanece: sem endereco nao sabemos para onde voltar
    assert w.borrowed_from == "192.168.1.1:7011"


# ─────────────────────────────────────────────────────────────────────────────
# Testes: Worker._request_task intercept de M2M
# ─────────────────────────────────────────────────────────────────────────────

def test_request_task_handles_command_redirect(monkeypatch):
    """_request_task deve chamar _handle_command_redirect ao receber tipo M2M."""
    w = Worker()
    w._master_ip = "127.0.0.1"
    w._master_port = 7011

    redirect_msg = {
        "type": "command_redirect",
        "request_id": str(uuid.uuid4()),
        "payload": {"new_master_address": "10.0.0.5:7012"},
    }

    monkeypatch.setattr(socket, 'create_connection', lambda *a, **k: FakeSock())
    monkeypatch.setattr(Worker, '_send', lambda self, sock, payload: None)
    monkeypatch.setattr(Worker, '_recv', lambda self, sock, timeout=5: redirect_msg)

    handled = []
    monkeypatch.setattr(Worker, '_handle_command_redirect', lambda self, msg: handled.append(msg))

    w._request_task()

    assert len(handled) == 1
    assert handled[0]["type"] == "command_redirect"


def test_request_task_handles_command_release(monkeypatch):
    """_request_task deve chamar _handle_command_release ao receber tipo M2M."""
    w = Worker()
    w._master_ip = "127.0.0.1"
    w._master_port = 7011

    release_msg = {
        "type": "command_release",
        "request_id": str(uuid.uuid4()),
        "payload": {"original_master_address": "192.168.1.1:7011"},
    }

    monkeypatch.setattr(socket, 'create_connection', lambda *a, **k: FakeSock())
    monkeypatch.setattr(Worker, '_send', lambda self, sock, payload: None)
    monkeypatch.setattr(Worker, '_recv', lambda self, sock, timeout=5: release_msg)

    handled = []
    monkeypatch.setattr(Worker, '_handle_command_release', lambda self, msg: handled.append(msg))

    w._request_task()

    assert len(handled) == 1
    assert handled[0]["type"] == "command_release"


def test_request_task_normal_query_unaffected(monkeypatch):
    """Mensagem QUERY normal nao deve ser confundida com M2M."""
    w = Worker()
    w._master_ip = "127.0.0.1"
    w._master_port = 7011

    responses = iter([
        {"TASK": "QUERY", "USER": "Test"},
        {"STATUS": "ACK", "WORKER_UUID": w.uuid},
    ])

    monkeypatch.setattr(socket, 'create_connection', lambda *a, **k: FakeSock())
    monkeypatch.setattr(Worker, '_send', lambda self, sock, payload: None)
    monkeypatch.setattr(Worker, '_recv', lambda self, sock, timeout=5: next(responses))

    redirect_called = []
    monkeypatch.setattr(Worker, '_handle_command_redirect', lambda self, msg: redirect_called.append(msg))

    import time as t_mod
    monkeypatch.setattr(t_mod, 'sleep', lambda s: None)

    w._request_task()

    assert len(redirect_called) == 0


# ─────────────────────────────────────────────────────────────────────────────
# Testes: ALIVE inclui SERVER_UUID quando borrowed_from definido
# ─────────────────────────────────────────────────────────────────────────────

def test_alive_includes_server_uuid_when_borrowed(monkeypatch):
    """Worker emprestado deve incluir SERVER_UUID no payload ALIVE."""
    w = Worker()
    w.borrowed_from = "192.168.1.1:7011"
    w._master_ip = "10.0.0.5"
    w._master_port = 7012

    sent_payloads = []

    def fake_send(self, sock, payload):
        sent_payloads.append(payload)

    monkeypatch.setattr(socket, 'create_connection', lambda *a, **k: FakeSock())
    monkeypatch.setattr(Worker, '_send', fake_send)
    monkeypatch.setattr(Worker, '_recv', lambda self, sock, timeout=5: {"TASK": "NO_TASK"})

    w._request_task()

    alive = sent_payloads[0]
    assert alive.get("WORKER") == "ALIVE"
    assert alive.get("SERVER_UUID") == "192.168.1.1:7011"


def test_alive_no_server_uuid_when_not_borrowed(monkeypatch):
    """Worker nao emprestado NAO deve incluir SERVER_UUID no payload ALIVE."""
    w = Worker()
    w.borrowed_from = None
    w._master_ip = "127.0.0.1"
    w._master_port = 7011

    sent_payloads = []

    def fake_send(self, sock, payload):
        sent_payloads.append(payload)

    monkeypatch.setattr(socket, 'create_connection', lambda *a, **k: FakeSock())
    monkeypatch.setattr(Worker, '_send', fake_send)
    monkeypatch.setattr(Worker, '_recv', lambda self, sock, timeout=5: {"TASK": "NO_TASK"})

    w._request_task()

    alive = sent_payloads[0]
    assert alive.get("WORKER") == "ALIVE"
    assert "SERVER_UUID" not in alive
