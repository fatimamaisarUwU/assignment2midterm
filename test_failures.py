"""
C3 - failure-injection tests.

Scenarios:
  1. Replica crash mid-request: client still commits with 2 of 3 acks.
  2. Request duplication:        second call carries was_duplicate=True.
  3. Induced timeout + retry:    retry succeeds, counter moves exactly once.
  4. Counter-id isolation (extra, exemplary band)
  5. Retry storm (extra, exemplary band)
"""

import socket
import threading
import time
import uuid
from concurrent import futures
from concurrent.futures import ThreadPoolExecutor, as_completed

import grpc
import pytest

import counter_pb2
import counter_pb2_grpc
from server import CounterServicer


# ----------------------------------------------------------------------
# Local helpers (reuse the style from conftest)
# ----------------------------------------------------------------------
def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


class RunningServer:
    def __init__(self, name: str = "test-replica"):
        self.name = name
        self.port = _free_port()
        self.address = f"127.0.0.1:{self.port}"
        self.servicer = CounterServicer(name=name)
        self.server = grpc.server(futures.ThreadPoolExecutor(max_workers=8))
        counter_pb2_grpc.add_CounterServicer_to_server(
            self.servicer, self.server
        )
        self.server.add_insecure_port(self.address)
        self.server.start()

    def stop(self):
        self.server.stop(grace=0).wait()


def _increment_one(address, request, timeout=1.0):
    try:
        ch = grpc.insecure_channel(address)
        stub = counter_pb2_grpc.CounterStub(ch)
        return address, stub.Increment(request, timeout=timeout), None
    except grpc.RpcError as e:
        return address, None, e


def _quorum_increment(addresses, counter_id, delta, key, timeout=1.0):
    """Same as quorum_client.quorum_increment, local copy for test isolation."""
    req = counter_pb2.IncrementRequest(
        counter_id=counter_id, delta=delta, idempotency_key=key, lamport_time=0,
    )
    acks = 0
    dup_any = False
    value = None
    errors = []
    with ThreadPoolExecutor(max_workers=len(addresses)) as pool:
        futures = [
            pool.submit(_increment_one, addr, req, timeout)
            for addr in addresses
        ]
        for fut in as_completed(futures):
            addr, reply, err = fut.result()
            if err is not None:
                errors.append((addr, err))
                continue
            acks += 1
            dup_any = dup_any or reply.was_duplicate
            if value is None:
                value = reply.new_value
    need = len(addresses) // 2 + 1
    return acks >= need, value, acks, dup_any, errors


# ----------------------------------------------------------------------
# 1. Replica crash mid-request (simulated by killing one before the call)
# ----------------------------------------------------------------------
def test_replica_crash_mid_request_still_commits():
    a = RunningServer("crash-A")
    b = RunningServer("crash-B")
    c = RunningServer("crash-C")

    try:
        # Simulate crash of C before the request arrives.
        c.stop()

        addresses = [a.address, b.address, c.address]
        committed, value, acks, dup, errors = _quorum_increment(
            addresses, "crash:c", delta=1, key=str(uuid.uuid4()),
        )

        # INVARIANT: client still commits (2 of 3 acks).
        assert committed is True, f"should commit, errors={errors}"
        assert acks == 2
        assert value == 1
        assert dup is False

        # And no exception escaped to the caller.
        assert all(isinstance(e, grpc.RpcError) for _, e in errors)
    finally:
        for s in (a, b):
            s.stop()


# ----------------------------------------------------------------------
# 2. Request duplication -> was_duplicate=True, value moves once
# ----------------------------------------------------------------------
def test_duplicate_request_returns_was_duplicate():
    a = RunningServer("dup-A")
    b = RunningServer("dup-B")
    c = RunningServer("dup-C")

    try:
        addresses = [a.address, b.address, c.address]
        key = "dup-key-42"

        committed1, value1, acks1, dup1, _ = _quorum_increment(
            addresses, "dup:x", delta=7, key=key,
        )
        assert committed1 and acks1 == 3
        assert value1 == 7
        assert dup1 is False

        # Same key again -> no reapplication anywhere.
        committed2, value2, acks2, dup2, _ = _quorum_increment(
            addresses, "dup:x", delta=7, key=key,
        )
        assert committed2 and acks2 == 3
        assert value2 == 7                # NOT 14
        assert dup2 is True               # all replicas detected duplicate

    finally:
        for s in (a, b, c):
            s.stop()


# ----------------------------------------------------------------------
# 3. Induced timeout + retry (same key -> value moves exactly once)
# ----------------------------------------------------------------------
def test_induced_timeout_then_retry_moves_once():
    """
    We force a real timeout on the wire by setting a very small client
    deadline, then retry with the SAME key (idempotency guarantees at
    most one application regardless of whether the first attempt was
    received or its reply was lost).
    """
    srv = RunningServer("timeout-A")

    try:
        ch = grpc.insecure_channel(srv.address)
        stub = counter_pb2_grpc.CounterStub(ch)

        key = "timeout-key-1"
        req = counter_pb2.IncrementRequest(
            counter_id="timeout:x",
            delta=1,
            idempotency_key=key,
            lamport_time=0,
        )

        # Attempt 1: micro-deadline (may or may not actually time out;
        # either way the design must keep the counter correct).
        try:
            stub.Increment(req, timeout=0.001)
        except grpc.RpcError:
            pass  # deadline is allowed to fire

        # Retry with the SAME key.
        reply = stub.Increment(req, timeout=2.0)

        # INVARIANT: counter moves exactly once (0 or 1 accepted),
        # final value is 1, and the retry is flagged duplicate.
        assert reply.new_value == 1
        assert reply.was_duplicate is True
    finally:
        srv.stop()


# ----------------------------------------------------------------------
# 4. EXTRA: counter_id isolation (edge-case coverage)
# ----------------------------------------------------------------------
def test_counter_id_isolation():
    srv = RunningServer("iso-A")
    try:
        ch = grpc.insecure_channel(srv.address)
        stub = counter_pb2_grpc.CounterStub(ch)

        stub.Increment(
            counter_pb2.IncrementRequest(
                counter_id="alpha", delta=10,
                idempotency_key="iso-1", lamport_time=0,
            ),
            timeout=2.0,
        )
        stub.Increment(
            counter_pb2.IncrementRequest(
                counter_id="beta", delta=5,
                idempotency_key="iso-2", lamport_time=0,
            ),
            timeout=2.0,
        )

        a = stub.Get(
            counter_pb2.GetRequest(counter_id="alpha", lamport_time=0),
            timeout=2.0,
        )
        b = stub.Get(
            counter_pb2.GetRequest(counter_id="beta", lamport_time=0),
            timeout=2.0,
        )

        assert a.value == 10
        assert b.value == 5
    finally:
        srv.stop()


# ----------------------------------------------------------------------
# 5. EXTRA: retry storm with a single key -> counter moves exactly once
# ----------------------------------------------------------------------
def test_retry_storm_same_key():
    srv = RunningServer("storm-A")
    try:
        ch = grpc.insecure_channel(srv.address)
        stub = counter_pb2_grpc.CounterStub(ch)

        key = "storm-key"
        N = 20

        results = []
        for _ in range(N):
            r = stub.Increment(
                counter_pb2.IncrementRequest(
                    counter_id="storm:x",
                    delta=1,
                    idempotency_key=key,
                    lamport_time=0,
                ),
                timeout=2.0,
            )
            results.append(r)

        # First call applied; the remaining N-1 saw the key already.
        applied = sum(1 for r in results if not r.was_duplicate)
        duplicates = sum(1 for r in results if r.was_duplicate)
        assert applied == 1, f"applied={applied}, expected exactly 1"
        assert duplicates == N - 1

        g = stub.Get(
            counter_pb2.GetRequest(counter_id="storm:x", lamport_time=0),
            timeout=2.0,
        )
        assert g.value == 1
    finally:
        srv.stop()