"""
Part C quorum client.

Sends each Increment to ALL configured replicas in parallel, waits for
acknowledgements, and reports the write as committed iff a majority of
replicas acknowledged it.

Majority rule for N replicas: majority = N // 2 + 1.
For N = 3, majority = 2.

Usage:
    python quorum_client.py incr likes:post-42 --by 1
    python quorum_client.py get  likes:post-42
    python quorum_client.py --targets localhost:50051,localhost:50052,localhost:50053 incr x --by 5
"""

import argparse
import sys
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed

import grpc

import counter_pb2
import counter_pb2_grpc


# ----------------------------------------------------------------------
# Configuration
# ----------------------------------------------------------------------
DEFAULT_TARGETS = [
    "localhost:50051",
    "localhost:50052",
    "localhost:50053",
]


def majority(n: int) -> int:
    return n // 2 + 1


# ----------------------------------------------------------------------
# Single-replica RPC helper
# ----------------------------------------------------------------------
def _increment_one(target: str, request, timeout: float):
    """
    Send Increment to one replica. Returns (target, reply, error).
    On success: (target, reply, None).
    On failure: (target, None, exception).
    """
    try:
        channel = grpc.insecure_channel(target)
        stub = counter_pb2_grpc.CounterStub(channel)
        reply = stub.Increment(request, timeout=timeout)
        return target, reply, None
    except grpc.RpcError as e:
        return target, None, e


def _get_one(target: str, request, timeout: float):
    try:
        channel = grpc.insecure_channel(target)
        stub = counter_pb2_grpc.CounterStub(channel)
        reply = stub.Get(request, timeout=timeout)
        return target, reply, None
    except grpc.RpcError as e:
        return target, None, e


# ----------------------------------------------------------------------
# Quorum operations
# ----------------------------------------------------------------------
def quorum_increment(targets, counter_id, delta, key, timeout):
    """
    Send the SAME request (same key) to all targets in parallel.
    Return (committed, new_value, acks, total, dup_any).
    """
    request = counter_pb2.IncrementRequest(
        counter_id=counter_id,
        delta=delta,
        idempotency_key=key,
        lamport_time=0,  # not used for Part C demo
    )

    acks = 0
    dup_any = False
    best_value = None
    errors = []

    with ThreadPoolExecutor(max_workers=len(targets)) as pool:
        futures = [
            pool.submit(_increment_one, t, request, timeout)
            for t in targets
        ]
        for fut in as_completed(futures):
            target, reply, err = fut.result()
            if err is not None:
                errors.append((target, err))
                continue
            acks += 1
            if reply.was_duplicate:
                dup_any = True
            if best_value is None:
                best_value = reply.new_value

    need = majority(len(targets))
    committed = acks >= need
    return committed, best_value, acks, len(targets), dup_any, errors


# ----------------------------------------------------------------------
# CLI commands
# ----------------------------------------------------------------------
def cmd_incr(args):
    targets = args.targets.split(",")
    key = args.key or str(uuid.uuid4())

    committed, value, acks, total, dup, errors = quorum_increment(
        targets=targets,
        counter_id=args.counter,
        delta=args.by,
        key=key,
        timeout=args.timeout,
    )

    dup_str = "yes" if dup else "no"
    if committed:
        print(
            f"OK committed value = {value} "
            f"(replicas acked: {acks}/{total}, duplicate: {dup_str})"
        )
    else:
        print(
            f"FAIL not committed "
            f"(replicas acked: {acks}/{total}, need {majority(total)})"
        )
        for target, err in errors:
            print(f"  - {target}: {type(err).__name__}", file=sys.stderr)
        sys.exit(1)


def cmd_get(args):
    """
    Read from any single replica (first one that answers).
    """
    targets = args.targets.split(",")
    request = counter_pb2.GetRequest(counter_id=args.counter, lamport_time=0)

    with ThreadPoolExecutor(max_workers=len(targets)) as pool:
        futures = [
            pool.submit(_get_one, t, request, args.timeout)
            for t in targets
        ]
        for fut in as_completed(futures):
            target, reply, err = fut.result()
            if err is None:
                if reply.found:
                    print(f"value = {reply.value}")
                else:
                    print("value = <not found>")
                return
    print("FAIL no replica answered", file=sys.stderr)
    sys.exit(1)


def main():
    parser = argparse.ArgumentParser(description="Quorum client (Part C)")
    parser.add_argument(
        "--targets",
        default=",".join(DEFAULT_TARGETS),
        help="comma-separated host:port list",
    )
    parser.add_argument("--timeout", type=float, default=2.0)

    sub = parser.add_subparsers(dest="command", required=True)

    p_incr = sub.add_parser("incr")
    p_incr.add_argument("counter")
    p_incr.add_argument("--by", type=int, default=1)
    p_incr.add_argument("--key", default=None)
    p_incr.set_defaults(func=cmd_incr)

    p_get = sub.add_parser("get")
    p_get.add_argument("counter")
    p_get.set_defaults(func=cmd_get)

    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()