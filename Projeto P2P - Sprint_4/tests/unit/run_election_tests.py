import os
import sys

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from worker.worker import Worker


def run():
    w1 = Worker()
    w2 = Worker()

    # set votes where w1 should win
    with w1._votes_lock:
        w1._votes = {w1.uuid: (50, w1.my_ip), w2.uuid: (40, w2.my_ip)}

    # stub out broadcast and _become_temp_master
    w1._broadcast = lambda msg: None

    def fake_become():
        w1._became = True

    w1._become_temp_master = fake_become

    w1._resolve_election()

    ok = getattr(w1, '_became', False) is True
    print('run_election_tests: {}'.format('PASS' if ok else 'FAIL'))


if __name__ == '__main__':
    run()
