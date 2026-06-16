"""Testes unitarios Sprint 4 — payload performance_report e envio TLS/TCP."""
import json
import ssl
import socket
import collections
import datetime
import threading
import pytest
import master


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def _reset_master_state():
    """Limpa estado mutavel do master para isolar cada teste."""
    master.task_queue.queue.clear()
    with master.task_enqueue_lock:
        master.task_enqueue_times.clear()
    with master.stats_lock:
        master.stats['concluidas'] = 0
        master.stats['falhas']     = 0
        master.stats['heartbeats'] = 0
    with master.in_flight_lock:
        master.in_flight_tasks.clear()
    with master.known_workers_lock:
        master.known_workers.clear()
    with master.borrowed_workers_lock:
        master.borrowed_workers.clear()
    with master.lent_workers_lock:
        master.lent_workers.clear()


# ─────────────────────────────────────────────────────────────────────────────
# Testes: collect_metrics — estrutura do payload
# ─────────────────────────────────────────────────────────────────────────────

def test_collect_metrics_top_level_keys():
    _reset_master_state()
    m = master.collect_metrics()
    for key in ("type", "timestamp", "master_id", "master_uuid", "uptime_s", "system", "farm"):
        assert key in m, f"Chave ausente no payload: '{key}'"


def test_collect_metrics_type_field():
    _reset_master_state()
    m = master.collect_metrics()
    assert m["type"] == "performance_report"


def test_collect_metrics_timestamp_iso8601():
    """timestamp deve ser uma string ISO-8601 com informacao de timezone."""
    _reset_master_state()
    m = master.collect_metrics()
    ts = m["timestamp"]
    assert isinstance(ts, str)
    # datetime.fromisoformat aceita strings com offset (+00:00)
    dt = datetime.datetime.fromisoformat(ts)
    assert dt.tzinfo is not None, "timestamp deve incluir timezone info"


def test_collect_metrics_uptime_non_negative():
    _reset_master_state()
    m = master.collect_metrics()
    assert m["uptime_s"] >= 0


def test_collect_metrics_system_keys():
    _reset_master_state()
    m = master.collect_metrics()
    sys_keys = ("cpu_percent", "memory_used_mb", "memory_total_mb",
                "memory_percent", "disk_used_gb", "disk_total_gb", "disk_percent")
    for key in sys_keys:
        assert key in m["system"], f"Chave ausente em system: '{key}'"


def test_collect_metrics_farm_keys():
    _reset_master_state()
    m = master.collect_metrics()
    farm_keys = ("workers_known", "workers_borrowed", "workers_lent",
                 "tasks_queued", "tasks_in_flight", "tasks_completed",
                 "tasks_failed", "heartbeats", "oldest_task_age_s",
                 "neighbors_count", "saturation_pct", "capacity")
    for key in farm_keys:
        assert key in m["farm"], f"Chave ausente em farm: '{key}'"


def test_collect_metrics_tasks_queued_reflects_queue():
    _reset_master_state()
    master.task_queue.put({"TASK": "QUERY", "USER": "u1"})
    master.task_queue.put({"TASK": "QUERY", "USER": "u2"})
    with master.task_enqueue_lock:
        master.task_enqueue_times.append(0.0)
        master.task_enqueue_times.append(0.0)
    m = master.collect_metrics()
    assert m["farm"]["tasks_queued"] == 2
    _reset_master_state()


def test_collect_metrics_oldest_task_age_zero_when_empty():
    _reset_master_state()
    m = master.collect_metrics()
    assert m["farm"]["oldest_task_age_s"] == 0.0


def test_collect_metrics_oldest_task_age_positive_when_queued():
    import time
    _reset_master_state()
    with master.task_enqueue_lock:
        master.task_enqueue_times.append(time.time() - 30)   # simula tarefa com 30s na fila
    m = master.collect_metrics()
    assert m["farm"]["oldest_task_age_s"] >= 29.0
    _reset_master_state()


def test_collect_metrics_saturation_pct_zero_when_empty():
    _reset_master_state()
    m = master.collect_metrics()
    assert m["farm"]["saturation_pct"] == 0.0


def test_collect_metrics_saturation_pct_at_capacity():
    _reset_master_state()
    cap = master.CAPACITY
    for _ in range(cap):
        master.task_queue.put({"TASK": "QUERY", "USER": "x"})
    m = master.collect_metrics()
    assert m["farm"]["saturation_pct"] == 100.0
    _reset_master_state()


def test_collect_metrics_stats_reflected():
    _reset_master_state()
    with master.stats_lock:
        master.stats['concluidas'] = 7
        master.stats['falhas']     = 2
    m = master.collect_metrics()
    assert m["farm"]["tasks_completed"] == 7
    assert m["farm"]["tasks_failed"]    == 2
    _reset_master_state()


def test_collect_metrics_master_id_and_uuid():
    _reset_master_state()
    m = master.collect_metrics()
    assert m["master_id"]   == master.MASTER_NAME
    assert m["master_uuid"] == master.MASTER_UUID


# ─────────────────────────────────────────────────────────────────────────────
# Testes: send_to_supervisor — socket TLS puro, sem HTTP, sem recv
# ─────────────────────────────────────────────────────────────────────────────

class FakeTlsSock:
    """Simula um socket TLS — captura o que foi enviado."""
    def __init__(self):
        self.sent    = b""
        self.closed  = False
        self._recv_called = False

    def sendall(self, data):
        self.sent += data

    def recv(self, n):          # nunca deve ser chamado
        self._recv_called = True
        return b""

    def close(self):
        self.closed = True


def test_send_to_supervisor_sends_json_line(monkeypatch):
    """send_to_supervisor deve enviar JSON + newline via TLS."""
    fake_tls = FakeTlsSock()

    monkeypatch.setattr(socket, "create_connection",
                        lambda *a, **k: object())
    monkeypatch.setattr(ssl.SSLContext, "wrap_socket",
                        lambda self, raw, **k: fake_tls)

    payload = {"type": "performance_report", "master_id": "MASTER_TEST"}
    master.send_to_supervisor(payload)

    raw_sent = fake_tls.sent.decode("utf-8")
    assert raw_sent.endswith("\n"), "Payload deve terminar com newline"
    parsed = json.loads(raw_sent.strip())
    assert parsed["type"]      == "performance_report"
    assert parsed["master_id"] == "MASTER_TEST"


def test_send_to_supervisor_no_recv(monkeypatch):
    """send_to_supervisor NAO deve chamar recv (restricao critica)."""
    fake_tls = FakeTlsSock()

    monkeypatch.setattr(socket, "create_connection",
                        lambda *a, **k: object())
    monkeypatch.setattr(ssl.SSLContext, "wrap_socket",
                        lambda self, raw, **k: fake_tls)

    master.send_to_supervisor({"type": "performance_report"})

    assert not fake_tls._recv_called, "recv NAO deve ser chamado pelo supervisor"


def test_send_to_supervisor_closes_socket(monkeypatch):
    """send_to_supervisor deve fechar o socket TLS apos o envio."""
    fake_tls = FakeTlsSock()

    monkeypatch.setattr(socket, "create_connection",
                        lambda *a, **k: object())
    monkeypatch.setattr(ssl.SSLContext, "wrap_socket",
                        lambda self, raw, **k: fake_tls)

    master.send_to_supervisor({"type": "performance_report"})

    assert fake_tls.closed, "Socket TLS deve ser fechado apos o envio"


def test_send_to_supervisor_uses_sni(monkeypatch):
    """wrap_socket deve usar server_hostname = SUPERVISOR_HOST (SNI correto)."""
    captured = {}

    class FakeCtx:
        def wrap_socket(self, raw, server_hostname=None, **k):
            captured["sni"] = server_hostname
            return FakeTlsSock()

    monkeypatch.setattr(socket, "create_connection",
                        lambda *a, **k: object())
    monkeypatch.setattr(ssl, "create_default_context",
                        lambda: FakeCtx())

    master.send_to_supervisor({"type": "performance_report"})

    assert captured.get("sni") == master.SUPERVISOR_HOST, (
        f"SNI deve ser '{master.SUPERVISOR_HOST}', recebido: {captured.get('sni')}"
    )


def test_send_to_supervisor_connects_to_correct_host_port(monkeypatch):
    """create_connection deve usar SUPERVISOR_HOST e SUPERVISOR_PORT."""
    captured = {}

    def fake_connect(address, timeout=None):
        captured["address"] = address
        return object()

    monkeypatch.setattr(socket, "create_connection", fake_connect)
    monkeypatch.setattr(ssl.SSLContext, "wrap_socket",
                        lambda self, raw, **k: FakeTlsSock())

    master.send_to_supervisor({"type": "performance_report"})

    assert captured["address"] == (master.SUPERVISOR_HOST, master.SUPERVISOR_PORT)


def test_send_to_supervisor_no_http(monkeypatch):
    """O payload enviado nao deve conter verbos HTTP."""
    fake_tls = FakeTlsSock()

    monkeypatch.setattr(socket, "create_connection",
                        lambda *a, **k: object())
    monkeypatch.setattr(ssl.SSLContext, "wrap_socket",
                        lambda self, raw, **k: fake_tls)

    master.send_to_supervisor({"type": "performance_report", "v": 1})

    raw_sent = fake_tls.sent.decode("utf-8")
    for verb in ("GET ", "POST ", "HTTP/", "Host:"):
        assert verb not in raw_sent, f"Payload nao deve conter token HTTP: '{verb}'"
