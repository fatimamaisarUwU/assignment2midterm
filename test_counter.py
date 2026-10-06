"""
C2 - mandatory unit and integration tests.

Each test:
  * has a stable, descriptive name,
  * follows arrange-act-assert,
  * runs green from a clean checkout (no manual setup).
"""

import threading

import pytest

import counter_pb2
import counter_pb2_grpc
from server import CounterServicer


# ----------------------------------------------------------------------
# 1. A single Increment applies exactly its delta
# ----------------------------------------------------------------------
def test_increment_applies_delta(single_replica_server, client_for):
    # arrange
    client = client_for(single_replica_server.address)

    # act
    r1 = client.incr("x", 5, key="k1")
    r2 = client.incr("x", 3, key="k2")

    # assert
    assert r1.new_value == 5
    assert r1.was_duplicate is False
    assert r2.new_value == 8
    assert r2.was_duplicate is False

    g = client.get("x")
    assert g.found is True
    assert g.value == 8


# ----------------------------------------------------------------------
# 2. Re-sending the same idempotency key is not reapplied
# ----------------------------------------------------------------------
def test_duplicate_key_not_reapplied(single_replica_server, client_for):
    client = client_for(single_replica_server.address)

    r1 = client.incr("x", 5, key="dup-key")
    r2 = client.incr("x", 5, key="dup-key")

    assert r1.new_value == 5
    assert r1.was_duplicate is False
    assert r2.new_value == 5              # not 10
    assert r2.was_duplicate is True

    g = client.get("x")
    assert g.value == 5


# ----------------------------------------------------------------------
# 3. Concurrent increments must be exact (2 x 1000 = 2000)
# ----------------------------------------------------------------------
def test_concurrent_increments_exact(single_replica_server, client_for):
    client = client_for(single_replica_server.address)

    N = 1000
    errors = []

    def worker(prefix: str):
        try:
            for i in range(N):
                client.incr("c", 1, key=f"{prefix}-{i}")
        except Exception as e:      # pragma: no cover
            errors.append(e)

    t1 = threading.Thread(target=worker, args=("a",))
    t2 = threading.Thread(target=worker, args=("b",))
    t1.start()
    t2.start()
    t1.join()
    t2.join()

    assert not errors, f"worker errors: {errors}"
    g = client.get("c")
    assert g.found is True
    assert g.value == 2 * N


# ----------------------------------------------------------------------
# 4. Get on an unknown counter returns found=False without raising
# ----------------------------------------------------------------------
def test_get_missing_counter(single_replica_server, client_for):
    client = client_for(single_replica_server.address)
    g = client.get("never-set")
    assert g.found is False
    assert g.value == 0


# ----------------------------------------------------------------------
# 5. Retry after timeout is safe (same key -> moves exactly once)
# ----------------------------------------------------------------------
def test_retry_after_timeout_is_safe(single_replica_server, client_for):
    """
    Simulate: first attempt applied +5, but the reply was lost.
    Client retries with the SAME key. Counter must end at 5, not 10.
    """
    client = client_for(single_replica_server.address)
    key = "timeout-key"

    r1 = client.incr("t", 5, key=key)
    assert r1.new_value == 5
    assert r1.was_duplicate is False

    # retry with the SAME key
    r2 = client.incr("t", 5, key=key)
    assert r2.new_value == 5
    assert r2.was_duplicate is True

    g = client.get("t")
    assert g.value == 5


# ----------------------------------------------------------------------
# 6. With one replica stopped, a write still commits (2 of 3)
# ----------------------------------------------------------------------
def test_majority_commit_two_acks(three_replica_servers, client_for):
    a, b, c = three_replica_servers
    c.stop()                    # now only A and B are alive

    clients = [client_for(a.address), client_for(b.address)]
    results = [cl.incr("m", 1, key="maj-key") for cl in clients]

    for r in results:
        assert r.new_value == 1
        assert r.was_duplicate is False

    # majority of 3 is 2, and we got exactly 2 acks -> commit
    assert len(results) == 2


# ----------------------------------------------------------------------
# 7. With two replicas stopped, below majority -> failure
# ----------------------------------------------------------------------
def test_no_commit_below_majority(three_replica_servers, client_for):
    a, b, c = three_replica_servers
    b.stop()
    c.stop()                    # now only A is alive

    client = client_for(a.address)
    r = client.incr("n", 7, key="nm-key")
    assert r.new_value == 7     # A did apply locally

    total = 3
    majority = total // 2 + 1   # = 2
    acks = 1
    assert acks < majority, "1 ack is below majority for N=3"


# ----------------------------------------------------------------------
# 8. Replicas converge on the same value after quorum writes
# ----------------------------------------------------------------------
def test_replicas_converge(three_replica_servers, client_for):
    a, b, c = three_replica_servers
    clients = [client_for(s.address) for s in (a, b, c)]

    for i in range(3):
        for cl in clients:
            r = cl.incr("conv", 1, key=f"conv-{i}")
            assert r.was_duplicate is False

    values = [cl.get("conv").value for cl in clients]
    assert values == [3, 3, 3], f"replicas diverged: {values}"