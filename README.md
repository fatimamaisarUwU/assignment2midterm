# Replicated Counter Service

A fault-tolerant distributed counter built with gRPC in Python. Three stages:
single-node service with idempotent retries, Lamport-clock instrumentation,
and a three-replica deployment with 2-of-3 majority quorum commits.

---

### 1. Setup & Environment

    python -m venv .venv
    # On Windows:
    .venv\Scripts\Activate.ps1
    # On macOS/Linux:
    # source .venv/bin/activate

    pip install -r requirements.txt
    # or, if you prefer:
    # pip install grpcio grpcio-tools pytest

    python -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. counter.proto

The last command regenerates counter_pb2.py and counter_pb2_grpc.py from
counter.proto. These generated files must not be edited by hand.

---

### 2. Start Single Replica

    python server.py --port 50051 --name replica-A --log replica-A.log

Starts one gRPC server on port 50051. It runs until you press Ctrl+C.

---

### 3. Start Three Replicas (Cluster)

Open three terminals and run one command in each:

    python server.py --port 50051 --name replica-A --log replica-A.log
    python server.py --port 50052 --name replica-B --log replica-B.log
    python server.py --port 50053 --name replica-C --log replica-C.log

Each replica is an independent process with its own in-memory state and its
own deduplication map. Nothing is shared between them.

---

### 4. Run Client Commands

    # Increment counter
    python client.py incr likes:post-42 --by 1

    # Retry with the same idempotency key (second call must NOT re-apply)
    python client.py incr likes:post-42 --by 1 --key test-fixed-key

    # Get counter value
    python client.py get likes:post-42

    # Optional: use a named client for Lamport trace logs
    python client.py --client-name client-1 incr x --by 5

    # Quorum client (Part C): sends to all three replicas, commits when 2+ ack
    python quorum_client.py incr likes:post-42 --by 1
    python quorum_client.py get  likes:post-42

---

### 5. Run Test Suite

    # All tests together (13 passed expected)
    python -m pytest -v

    # Unit & Integration tests (Task C2)
    python -m pytest -v test_counter.py

    # Failure-injection tests (Task C3)
    python -m pytest -v test_failures.py

The tests start their own servers on ephemeral ports inside pytest fixtures,
so no manual setup is required. The suite runs green from a clean checkout.

---

### 6. Run Benchmark (Task C4)

    python benchmark.py

Runs four configurations (single replica and quorum, each with 1 and 16
concurrent clients, 2000 requests per configuration) and prints a table
with median and p95 latency.

---

### 7. Repository layout

    counter.proto             Protocol Buffers interface definition
    counter_pb2.py            Generated message classes (do not edit)
    counter_pb2_grpc.py       Generated stub/servicer classes (do not edit)
    server.py                 gRPC server (single replica; run 3x for cluster)
    client.py                 Part A/B CLI client (single replica)
    quorum_client.py          Part C CLI client (fan-out to 3 replicas)
    conftest.py               pytest fixtures (in-process servers)
    test_counter.py           8 mandated unit & integration tests
    test_failures.py          5 failure-injection and edge-case tests
    benchmark.py              Performance measurement script
    requirements.txt          pip freeze of the working environment
    pytest_output.txt         Captured output of a full green pytest run
    logs/                     Captured Lamport event traces from Task B2
    report.pdf                Full test report

---

### 8. Troubleshooting

If a port is already in use (WSA Error 10048), kill stale Python processes:

    Get-Process python -ErrorAction SilentlyContinue | Stop-Process -Force

Then verify ports are free:

    netstat -ano | Select-String "50051|50052|50053"

---

Repository: https://github.com/fatimamaisarUwU/assignment2midterm