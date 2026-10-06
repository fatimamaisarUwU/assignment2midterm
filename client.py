"""
Replicated Counter Service - Part B client with Lamport clock.
"""

import argparse
import logging
import sys
import time
import uuid

import grpc

import counter_pb2
import counter_pb2_grpc


# ----------------------------------------------------------------------
# Lamport clock (client side)
# ----------------------------------------------------------------------
class LamportClock:
    def __init__(self, name: str):
        self._name = name
        self._l = 0

    @property
    def value(self) -> int:
        return self._l

    def tick(self) -> int:
        self._l += 1
        return self._l

    def recv(self, received: int) -> int:
        self._l = max(self._l, received) + 1
        return self._l

    def log(self, event: str, extra: str = ""):
        logging.info("%s %s L=%d %s", self._name, event, self._l, extra)


# ----------------------------------------------------------------------
# Retry helper (Part A behaviour preserved)
# ----------------------------------------------------------------------
def _call_with_retry(stub, request, timeout=2.0, max_attempts=4):
    delays = [0.2, 0.4, 0.8]
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
    raise last_error


# ----------------------------------------------------------------------
# Commands
# ----------------------------------------------------------------------
def cmd_incr(args, clock: LamportClock):
    channel = grpc.insecure_channel(args.target)
    stub = counter_pb2_grpc.CounterStub(channel)

    key = args.key or str(uuid.uuid4())

    # ---- SEND event ----
    clock.tick()
    clock.log(
        "SEND Increment",
        f"counter={args.counter} delta={args.by}",
    )

    request = counter_pb2.IncrementRequest(
        counter_id=args.counter,
        delta=args.by,
        idempotency_key=key,
        lamport_time=clock.value,
    )

    reply = _call_with_retry(stub, request, timeout=args.timeout)

    # ---- RECV event ----
    clock.recv(reply.lamport_time)
    clock.log(
        "RECV IncrementReply",
        f"(new_value={reply.new_value}, duplicate={reply.was_duplicate}, "
        f"received L={reply.lamport_time})",
    )

    dup = "yes" if reply.was_duplicate else "no"
    print(f"OK committed value = {reply.new_value} (duplicate: {dup})")


def cmd_get(args, clock: LamportClock):
    channel = grpc.insecure_channel(args.target)
    stub = counter_pb2_grpc.CounterStub(channel)

    # ---- SEND event ----
    clock.tick()
    clock.log("SEND Get", f"counter={args.counter}")

    request = counter_pb2.GetRequest(
        counter_id=args.counter,
        lamport_time=clock.value,
    )
    reply = stub.Get(request, timeout=args.timeout)

    # ---- RECV event ----
    clock.recv(reply.lamport_time)
    clock.log(
        "RECV GetReply",
        f"(value={reply.value}, found={reply.found}, "
        f"received L={reply.lamport_time})",
    )

    if reply.found:
        print(f"value = {reply.value}")
    else:
        print("value = <not found>")


def main():
    parser = argparse.ArgumentParser(description="Counter client (Part B)")
    parser.add_argument("--target", default="localhost:50051")
    parser.add_argument("--timeout", type=float, default=2.0)
    parser.add_argument("--client-name", default="client-1",
                        help="client identifier used in Lamport logs")
    parser.add_argument("--log", default=None,
                        help="optional log file path")

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

    handlers = [logging.StreamHandler()]
    if args.log:
        handlers.append(logging.FileHandler(args.log, mode="a", encoding="utf-8"))
    logging.basicConfig(
        level=logging.INFO,
        format="%(message)s",
        handlers=handlers,
    )

    clock = LamportClock(args.client_name)
    args.func(args, clock)


if __name__ == "__main__":
    main()