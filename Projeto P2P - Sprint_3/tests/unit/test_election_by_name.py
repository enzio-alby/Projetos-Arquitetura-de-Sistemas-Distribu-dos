import pytest
from workerp2 import Worker


MASTER_1 = {"TYPE": "DISCOVERY_REPLY", "MASTER_NAME": "MASTER_1", "MASTER_IP": "10.0.0.1", "MASTER_PORT": 7011}
MASTER_2 = {"TYPE": "DISCOVERY_REPLY", "MASTER_NAME": "MASTER_2", "MASTER_IP": "10.0.0.2", "MASTER_PORT": 7011}
MASTER_10 = {"TYPE": "DISCOVERY_REPLY", "MASTER_NAME": "MASTER_10", "MASTER_IP": "10.0.0.10", "MASTER_PORT": 7011}


def test_elects_lexicographically_smallest_of_three():
    w = Worker()
    elected = w._elect_master([MASTER_2, MASTER_1, MASTER_10])
    assert elected["MASTER_NAME"] == "MASTER_1"


def test_elects_single_master():
    w = Worker()
    elected = w._elect_master([MASTER_2])
    assert elected["MASTER_NAME"] == "MASTER_2"


def test_elect_empty_list_returns_none():
    w = Worker()
    assert w._elect_master([]) is None


def test_master_10_between_1_and_2_lexicographically():
    """MASTER_1 < MASTER_10 < MASTER_2 na ordem lexicografica do Python."""
    w = Worker()
    elected = w._elect_master([MASTER_2, MASTER_10])
    assert elected["MASTER_NAME"] == "MASTER_10"


def test_connect_and_ack_sets_master(monkeypatch):
    """_connect_and_ack deve chamar set_master com o IP e porta corretos."""
    import socket
    from workerp2 import Worker

    class FakeSock:
        def sendall(self, d): pass
        def settimeout(self, t): pass
        def close(self): pass

    monkeypatch.setattr(socket, 'create_connection', lambda *a, **k: FakeSock())
    monkeypatch.setattr(Worker, '_send', lambda self, sock, payload: None)
    monkeypatch.setattr(Worker, '_recv', lambda self, sock, timeout=5: {
        'TYPE': 'ELECTION_ACK', 'STATUS': 'ACCEPTED', 'MASTER_NAME': 'MASTER_1'
    })

    w = Worker()
    master = {"MASTER_NAME": "MASTER_1", "MASTER_IP": "10.0.0.1", "MASTER_PORT": 7011}
    result = w._connect_and_ack(master)

    assert result is True
    assert w.master_ip == "10.0.0.1"
    assert w.master_port == 7011
