"""
Pytest fixtures shared across all tests.

Key fixtures:
    single_replica_server - one in-process gRPC server (fresh per test)
    three_replica_servers - three independent servers (fresh per test)
    client_for            - factory that returns a thin RPC wrapper

Servers run in background threads inside the same pytest process, so
tests are fast and do not depend on manually-started processes.
"""

import socket
from concurrent import futures

import grpc
import pytest

import counter_pb2
import counter_pb2_grpc
from server import CounterServicer


def _free_port() -> int:
    """Ask the OS for a free TCP port."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


class RunningServer:
    """A gRPC server running in a background thread."""

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


@pytest.fixture
def single_replica_server():
    """One gRPC server, started fresh for each test."""
    srv = RunningServer("test-replica-A")
    try:
        yield srv
    finally:
        srv.stop()


@pytest.fixture
def three_replica_servers():
    """Three independent gRPC servers, each with its own state."""
    servers = [
        RunningServer(f"test-replica-{name}") for name in ("A", "B", "C")
    ]
    try:
        yield servers
    finally:
        for s in servers:
            s.stop()


class SimpleClient:
    """Thin wrapper around the generated stub."""

    def __init__(self, address: str):
        self.channel = grpc.insecure_channel(address)
        self.stub = counter_pb2_grpc.CounterStub(self.channel)

    def incr(self, counter_id: str, delta: int, key: str = "",
             timeout: float = 2.0):
        req = counter_pb2.IncrementRequest(
            counter_id=counter_id,
            delta=delta,
            idempotency_key=key,
            lamport_time=0,
        )
        return self.stub.Increment(req, timeout=timeout)

    def get(self, counter_id: str, timeout: float = 2.0):
        req = counter_pb2.GetRequest(counter_id=counter_id, lamport_time=0)
        return self.stub.Get(req, timeout=timeout)


@pytest.fixture
def client_for():
    """Factory fixture: client_for(address) -> SimpleClient."""
    def _make(address: str) -> SimpleClient:
        return SimpleClient(address)
    return _make