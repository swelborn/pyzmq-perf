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
