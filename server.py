"""
Replicated Counter Service - Part B server with Lamport clock instrumentation.

Lamport rules implemented:
  * Before every local event (receive, apply, send) -> L = L + 1
  * On message receive -> L = max(L, received_L) + 1 before processing

Log format is exactly the one required by the assignment.
"""

import argparse
import logging
import threading
from concurrent import futures

import grpc

import counter_pb2
import counter_pb2_grpc


# ----------------------------------------------------------------------
# Lamport clock
# ----------------------------------------------------------------------
class LamportClock:
    def __init__(self, name: str):
        self._name = name
        self._l = 0

    @property
    def value(self) -> int:
        return self._l

    def tick(self) -> int:
        """Advance the clock before a local event (send/apply)."""
        self._l += 1
        return self._l

    def recv(self, received: int) -> int:
        """Merge on receive: L = max(L, received) + 1."""
        self._l = max(self._l, received) + 1
        return self._l

    def log(self, event: str, extra: str = ""):
        logging.info("%s %s L=%d %s", self._name, event, self._l, extra)


# ----------------------------------------------------------------------
# Counter servicer
# ----------------------------------------------------------------------
class CounterServicer(counter_pb2_grpc.CounterServicer):

    def __init__(self, name: str = "replica-A"):
        self._name = name
        self._lock = threading.Lock()
        self._values: dict[str, int] = {}
        self._seen: dict[str, tuple[str, int]] = {}
        self._clock = LamportClock(name)

    def Increment(self, request, context):
        # ---- RECV event ----
        self._clock.recv(request.lamport_time)
        self._clock.log(
            "RECV Increment",
            f"counter={request.counter_id} delta={request.delta} "
            f"(received L={request.lamport_time})",
        )

        with self._lock:
            key = request.idempotency_key

            # Idempotency check
            if key and key in self._seen:
                stored_counter, stored_value = self._seen[key]
                if stored_counter != request.counter_id:
                    context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
                    context.set_details(
                        "idempotency_key reused with a different counter_id"
                    )
                    return counter_pb2.IncrementReply(
                        new_value=0,
                        was_duplicate=False,
                        lamport_time=self._clock.value,
                    )

                # Duplicate path: still tick before sending reply
                self._clock.log(
                    "DUPLICATE",
                    f"counter={request.counter_id} key={key} "
                    f"-> value={stored_value}",
                )
                self._clock.tick()  # SEND event
                self._clock.log(
                    "SEND IncrementReply",
                    f"(new_value={stored_value}, duplicate=true)",
                )
                return counter_pb2.IncrementReply(
                    new_value=stored_value,
                    was_duplicate=True,
                    lamport_time=self._clock.value,
                )

            # ---- APPLY event ----
            self._clock.tick()
            current = self._values.get(request.counter_id, 0)
            new_value = current + request.delta
            self._values[request.counter_id] = new_value
            self._clock.log(
                "APPLY",
                f"counter={request.counter_id} -> {new_value}",
            )

            if key:
                self._seen[key] = (request.counter_id, new_value)

            # ---- SEND event ----
            self._clock.tick()
            self._clock.log(
                "SEND IncrementReply",
                f"(new_value={new_value}, duplicate=false)",
            )
            return counter_pb2.IncrementReply(
                new_value=new_value,
                was_duplicate=False,
                lamport_time=self._clock.value,
            )

    def Get(self, request, context):
        # ---- RECV event ----
        self._clock.recv(request.lamport_time)
        self._clock.log(
            "RECV Get",
            f"counter={request.counter_id} (received L={request.lamport_time})",
        )

        with self._lock:
            if request.counter_id in self._values:
                value = self._values[request.counter_id]
                found = True
            else:
                value = 0
                found = False

        # ---- SEND event ----
        self._clock.tick()
        self._clock.log(
            "SEND GetReply",
            f"(value={value}, found={found})",
        )
        return counter_pb2.GetReply(
            value=value,
            found=found,
            lamport_time=self._clock.value,
        )


def serve(port: int, name: str = "replica-A", log_file: str | None = None):
    handlers = [logging.StreamHandler()]
    if log_file:
        handlers.append(logging.FileHandler(log_file, mode="a", encoding="utf-8"))
    logging.basicConfig(
        level=logging.INFO,
        format="%(message)s",
        handlers=handlers,
    )

    server = grpc.server(futures.ThreadPoolExecutor(max_workers=8))
    counter_pb2_grpc.add_CounterServicer_to_server(
        CounterServicer(name=name), server
    )
    server.add_insecure_port(f"[::]:{port}")
    server.start()
    logging.info("%s listening on port %d", name, port)
    server.wait_for_termination()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, default=50051)
    parser.add_argument("--name", type=str, default="replica-A")
    parser.add_argument("--log", type=str, default=None,
                        help="optional log file path")
    args = parser.parse_args()
    serve(args.port, args.name, args.log)