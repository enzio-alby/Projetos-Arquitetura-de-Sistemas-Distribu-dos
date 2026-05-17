import pytest
from workerp2 import Worker


def test_deterministic_election(monkeypatch):
    w1 = Worker()
    w2 = Worker()

    with w1._votes_lock:
        w1._votes = {w1.uuid: (50, w1.my_ip), w2.uuid: (40, w2.my_ip)}

    monkeypatch.setattr(Worker, '_broadcast', lambda self, msg: None)
    called = {}

    def fake_become(self):
        called['became'] = True

    monkeypatch.setattr(Worker, '_become_temp_master', fake_become)

    w1._resolve_election()

    assert called.get('became', False) is True
