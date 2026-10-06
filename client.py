"""
Replicated Counter Service - Part A client.

Features:
    * 2-second deadline on every RPC
    * up to 3 bounded retries with exponential backoff
    * one idempotency key per logical operation (reused on every retry)
"""

import argparse
import sys
import time
import uuid

import grpc

import counter_pb2
import counter_pb2_grpc


# ----------------------------------------------------------------------
# Low-level helper: retry a gRPC call with the same idempotency key
# ----------------------------------------------------------------------
def _call_with_retry(stub, request, timeout=2.0, max_attempts=4):
    """
    Call stub.Increment(request) with deadline=timeout.

    Retries at most `max_attempts - 1` times on transient failures
    (DEADLINE_EXCEEDED / UNAVAILABLE), reusing the SAME request object so
    the idempotency key does not change.
    """
    delays = [0.2, 0.4, 0.8]  # exponential backoff
    last_error = None
    for attempt in range(max_attempts):
        try:
            return stub.Increment(request, timeout=timeout)
        except grpc.RpcError as e:
            code = e.code()
            if code not in (grpc.StatusCode.DEADLINE_EXCEEDED,
                            grpc.StatusCode.UNAVAILABLE):
                raise
            last_error = e
            if attempt < len(delays):
                time.sleep(delays[attempt])
    # All retries exhausted
    raise last_error


# ----------------------------------------------------------------------
# CLI
# ----------------------------------------------------------------------
def cmd_incr(args):
    channel = grpc.insecure_channel(args.target)
    stub = counter_pb2_grpc.CounterStub(channel)

    # ONE key per logical operation -- reused on every retry
    key = args.key or str(uuid.uuid4())

    request = counter_pb2.IncrementRequest(
        counter_id=args.counter,
        delta=args.by,
        idempotency_key=key,
    )

    reply = _call_with_retry(stub, request, timeout=args.timeout)
    dup = "yes" if reply.was_duplicate else "no"
    print(f"OK committed value = {reply.new_value} (duplicate: {dup})")


def cmd_get(args):
    channel = grpc.insecure_channel(args.target)
    stub = counter_pb2_grpc.CounterStub(channel)

    reply = stub.Get(
        counter_pb2.GetRequest(counter_id=args.counter),
        timeout=args.timeout,
    )
    if reply.found:
        print(f"value = {reply.value}")
    else:
        print("value = <not found>")


def main():
    parser = argparse.ArgumentParser(description="Counter client")
    parser.add_argument("--target", default="localhost:50051")
    parser.add_argument("--timeout", type=float, default=2.0)
    sub = parser.add_subparsers(dest="command", required=True)

    p_incr = sub.add_parser("incr")
    p_incr.add_argument("counter")
    p_incr.add_argument("--by", type=int, default=1)
    p_incr.add_argument("--key", default=None,
                        help="idempotency key (default: uuid4)")
    p_incr.set_defaults(func=cmd_incr)

    p_get = sub.add_parser("get")
    p_get.add_argument("counter")
    p_get.set_defaults(func=cmd_get)

    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()