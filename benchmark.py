"""
C4 - performance benchmark.

Measures median and p95 Increment latency for four configurations:
  1) Single replica, 1 client
  2) Single replica, 16 clients
  3) Quorum (3 replicas), 1 client
  4) Quorum (3 replicas), 16 clients

At least 2000 requests per configuration.

p95 is computed as sorted[ceil(0.95 * n) - 1]  (0-based index).
"""

import math
import socket
import statistics
import threading
import time
import uuid
from concurrent import futures
from concurrent.futures import ThreadPoolExecutor, as_completed

import grpc

import counter_pb2
import counter_pb2_grpc
from server import CounterServicer


# ----------------------------------------------------------------------
# Server helper
# ----------------------------------------------------------------------
def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


class RunningServer:
    def __init__(self, name: str):
        self.name = name
        self.port = _free_port()
        self.address = f"127.0.0.1:{self.port}"
        self.servicer = CounterServicer(name=name)
        self.server = grpc.server(futures.ThreadPoolExecutor(max_workers=16))
        counter_pb2_grpc.add_CounterServicer_to_server(
            self.servicer, self.server
        )
        self.server.add_insecure_port(self.address)
        self.server.start()

    def stop(self):
        self.server.stop(grace=0).wait()


# ----------------------------------------------------------------------
# Measurement helpers
# ----------------------------------------------------------------------
def _p95(latencies):
    """Return 95th percentile latency (seconds) from a list of floats."""
    if not latencies:
        return float("nan")
    s = sorted(latencies)
    idx = math.ceil(0.95 * len(s)) - 1
    idx = max(0, min(idx, len(s) - 1))
    return s[idx]


def _one_thread_benchmark(address, n_ops, barrier, results, tid):
    """
    Run n_ops increments against one address.
    Each worker uses its own channel to avoid thread contention on the
    client side, and a unique counter_id so replicas never collide.
    """
    ch = grpc.insecure_channel(address)
    stub = counter_pb2_grpc.CounterStub(ch)
    counter_id = f"bench-{tid}"

    latencies = []
    barrier.wait()          # start all threads at the same time
    for i in range(n_ops):
        req = counter_pb2.IncrementRequest(
            counter_id=counter_id,
            delta=1,
            idempotency_key=str(uuid.uuid4()),
            lamport_time=0,
        )
        t0 = time.perf_counter()
        try:
            stub.Increment(req, timeout=5.0)
        except grpc.RpcError:
            # We don't count failed requests in latency; they'd skew
            # the percentiles. The test suite is responsible for
            # correctness; here we measure throughput behavior.
            continue
        t1 = time.perf_counter()
        latencies.append(t1 - t0)
    results[tid] = latencies


def run_single_replica(n_clients: int, total_requests: int):
    """All clients talk to one server."""
    srv = RunningServer("bench-A")
    try:
        per_thread = total_requests // n_clients
        results = [None] * n_clients
        barrier = threading.Barrier(n_clients)
        threads = [
            threading.Thread(
                target=_one_thread_benchmark,
                args=(srv.address, per_thread, barrier, results, tid),
            )
            for tid in range(n_clients)
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        all_lat = [x for sub in results for x in sub]
        return all_lat
    finally:
        srv.stop()


def run_quorum(n_clients: int, total_requests: int, n_replicas: int = 3):
    """
    Each client sends an Increment to all N replicas in parallel and
    waits for the last reply (the slowest replica) before counting the
    latency of that logical operation.
    """
    servers = [RunningServer(f"bench-{n}") for n in ("A", "B", "C")][:n_replicas]
    addresses = [s.address for s in servers]

    try:
        per_thread = total_requests // n_clients
        results = [None] * n_clients
        barrier = threading.Barrier(n_clients)

        def worker(tid):
            channel_stubs = []
            for addr in addresses:
                ch = grpc.insecure_channel(addr)
                channel_stubs.append(counter_pb2_grpc.CounterStub(ch))
            counter_id = f"qbench-{tid}"

            latencies = []
            barrier.wait()
            for i in range(per_thread):
                req = counter_pb2.IncrementRequest(
                    counter_id=counter_id,
                    delta=1,
                    idempotency_key=str(uuid.uuid4()),
                    lamport_time=0,
                )
                t0 = time.perf_counter()

                # fan out to all replicas, wait for all of them
                with ThreadPoolExecutor(max_workers=n_replicas) as pool:
                    futs = [
                        pool.submit(stub.Increment, req, timeout=5.0)
                        for stub in channel_stubs
                    ]
                    acks = 0
                    for fut in as_completed(futs):
                        try:
                            fut.result()
                            acks += 1
                        except grpc.RpcError:
                            pass

                t1 = time.perf_counter()
                # only count committed ops (majority)
                if acks >= n_replicas // 2 + 1:
                    latencies.append(t1 - t0)
            results[tid] = latencies

        threads = [
            threading.Thread(target=worker, args=(tid,))
            for tid in range(n_clients)
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        all_lat = [x for sub in results for x in sub]
        return all_lat
    finally:
        for s in servers:
            s.stop()


# ----------------------------------------------------------------------
# Report
# ----------------------------------------------------------------------
def _fmt(seconds):
    return f"{seconds * 1000:.3f}"


def main():
    TOTAL = 2000  # requests per configuration

    configs = [
        ("Single replica, 1 client",
         lambda: run_single_replica(1, TOTAL)),
        ("Single replica, 16 clients",
         lambda: run_single_replica(16, TOTAL)),
        ("Quorum (3 replicas), 1 client",
         lambda: run_quorum(1, TOTAL)),
        ("Quorum (3 replicas), 16 clients",
         lambda: run_quorum(16, TOTAL)),
    ]

    rows = []
    for name, fn in configs:
        print(f"Running: {name} ...", flush=True)
        lats = fn()
        n = len(lats)
        if n == 0:
            rows.append((name, "n/a", "n/a", 0))
            continue
        median = statistics.median(lats)
        p95 = _p95(lats)
        rows.append((name, _fmt(median), _fmt(p95), n))

    print()
    print(f"{'Configuration':<35}{'Median (ms)':>15}{'p95 (ms)':>12}{'Requests':>12}")
    print("-" * 74)
    for name, med, p95, n in rows:
        print(f"{name:<35}{med:>15}{p95:>12}{n:>12}")


if __name__ == "__main__":
    main()