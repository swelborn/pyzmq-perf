from typing import Type, TypeVar

from pydantic import BaseModel

import zmq

T = TypeVar("T", bound=BaseModel)


def validate_msg(msg: bytes, expected_type: Type[T]) -> T:
    return expected_type.model_validate_json(msg)


def req_poll(poller: zmq.Poller, socket: zmq.Socket, timeout_ms: int = 1000) -> bool:
    socks = dict(poller.poll(timeout_ms))
    if not socks:
        return False

    if socket not in socks:
        return False

    return True


def calculate_throughput(
    messages: int, size: int, start_time: float, end_time: float
) -> float:
    elapsed_time = end_time - start_time
    if elapsed_time <= 0:
        return 0.0
    return (messages * size * 8) / (elapsed_time * 1_000_000)  # Mbps
