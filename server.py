"""
Replicated Counter Service - Part A: single-node gRPC server with
idempotency (duplicate suppression) and thread-safe state.
"""

import argparse
import logging
import threading
from concurrent import futures

import grpc

import counter_pb2
import counter_pb2_grpc


logging.basicConfig(
    level=logging.INFO,
    format="[replica-%(name)s] %(message)s",
)


class CounterServicer(counter_pb2_grpc.CounterServicer):
    """
    Single-node counter service.

    State:
        _values: counter_id -> int (current value)
        _seen:   idempotency_key -> (counter_id, resulting_value)

    Both structures are guarded by a single lock, so the dedup check and
    the mutation happen atomically together.
    """

    def __init__(self, name: str = "A"):
        self._name = name
        self._lock = threading.Lock()
        self._values: dict[str, int] = {}
        self._seen: dict[str, tuple[str, int]] = {}

    # ------------------------------------------------------------------
    # Increment
    # ------------------------------------------------------------------
    def Increment(self, request, context):
        with self._lock:
            key = request.idempotency_key

            # 1) Idempotency check: have we already applied this key?
            if key and key in self._seen:
                stored_counter, stored_value = self._seen[key]
                # (Optional sanity: ensure the key belongs to the same counter)
                if stored_counter != request.counter_id:
                    context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
                    context.set_details(
                        "idempotency_key reused with a different counter_id"
                    )
                    return counter_pb2.IncrementReply()

                logging.info(
                    "DUPLICATE counter=%s key=%s -> value=%d",
                    request.counter_id, key, stored_value,
                )
                return counter_pb2.IncrementReply(
                    new_value=stored_value,
                    was_duplicate=True,
                )

            # 2) Apply the delta atomically
            current = self._values.get(request.counter_id, 0)
            new_value = current + request.delta
            self._values[request.counter_id] = new_value

            # 3) Remember the result so retries are safe
            if key:
                self._seen[key] = (request.counter_id, new_value)

            logging.info(
                "APPLY counter=%s delta=%d -> value=%d key=%s",
                request.counter_id, request.delta, new_value, key or "<none>",
            )

            return counter_pb2.IncrementReply(
                new_value=new_value,
                was_duplicate=False,
            )

    # ------------------------------------------------------------------
    # Get
    # ------------------------------------------------------------------
    def Get(self, request, context):
        with self._lock:
            if request.counter_id in self._values:
                value = self._values[request.counter_id]
                found = True
            else:
                value = 0
                found = False

        logging.info("GET counter=%s -> value=%d found=%s",
                     request.counter_id, value, found)
        return counter_pb2.GetReply(value=value, found=found)


def serve(port: int, name: str = "A"):
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=8))
    counter_pb2_grpc.add_CounterServicer_to_server(
        CounterServicer(name=name), server
    )
    server.add_insecure_port(f"[::]:{port}")
    server.start()
    logging.info("listening on port %d (name=%s)", port, name)
    server.wait_for_termination()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, default=50051)
    parser.add_argument("--name", type=str, default="A")
    args = parser.parse_args()
    serve(args.port, args.name)