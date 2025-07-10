import json
import logging
import time
from typing import TYPE_CHECKING, Callable

import numpy as np
from rich.logging import RichHandler

import zmq

from .callbacks import CallbackFactory
from .models import (
    CoordinationSignal,
    GroupSetupInfo,
    ReceiveCallback,
    Role,
    TestConfig,
    TestResult,
    WorkerCreate,
    WorkerState,
    WorkerUpdate,
)
from .utils import calculate_throughput

if TYPE_CHECKING:
    from .settings import BenchmarkSettings


logger = logging.getLogger("worker")


def get_worker_logger(worker_id: str, level: int = logging.INFO) -> logging.Logger:
    if logger.hasHandlers():
        logger.handlers.clear()
    handler = RichHandler(
        rich_tracebacks=True, show_time=False, show_path=False, markup=False
    )
    formatter = logging.Formatter(
        f"%(asctime)s [{worker_id}] %(message)s", datefmt="%Y-%m-%d %H:%M:%S"
    )
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.propagate = False
    logger.setLevel(level)
    return logger


def send_update(socket: zmq.Socket, worker_id: str, update: WorkerUpdate):
    socket.send(update.model_dump_json().encode())
    if not socket.recv_string() == "ACK":
        raise RuntimeError(
            f"[{worker_id}] did not receive ACK from coordinator after update."
        )
    logger.debug("Received ACK from coordinator after update.")


def retry_bind(socket: zmq.Socket, address: str, max_attempts: int = 10) -> None:
    """Keep trying to bind the socket to the address until successful or max attempts reached."""
    attempts = 0
    while attempts < max_attempts:
        try:
            socket.bind(address)
            logger.debug(f"Successfully bound to {address}.")
            return
        except zmq.ZMQError:
            logger.warning(f"Failed to bind to {address}. Retrying...")
            attempts += 1
            time.sleep(0.2)
    raise RuntimeError(f"Failed to bind to {address} after {max_attempts} attempts.")


def worker(
    role: Role,
    worker_id: str,
    settings: "BenchmarkSettings",
):
    logger = get_worker_logger(worker_id, settings.logging.get_level_int())
    ctx = zmq.Context()

    _worker_model = WorkerCreate(role=role, worker_id=worker_id)

    # Register with coordinator
    req_socket = ctx.socket(zmq.REQ)
    coordinator_url = f"tcp://{settings.network.coordinator_ip}:{settings.network.coordinator_router_port}"
    req_socket.connect(coordinator_url)
    req_socket.send(_worker_model.model_dump_json().encode())

    # Get peer info and sync address
    setup_info_raw = req_socket.recv()

    # Parse group setup info (always used now)
    group_setup_info = GroupSetupInfo.model_validate_json(setup_info_raw)
    logger.debug(
        f"Received group setup info: \n{group_setup_info.model_dump_json(indent=2)}"
    )

    sync_address = f"tcp://{settings.network.coordinator_ip}:{settings.network.coordinator_pub_port}"

    # Subscribe to sync signals
    sub_socket = ctx.socket(zmq.SUB)
    sub_socket.connect(sync_address)
    sub_socket.setsockopt(zmq.SUBSCRIBE, b"")
    time.sleep(settings.worker.setup_delay_s)

    logger.debug("connected to coordinator and sync channel.")

    update = WorkerUpdate(state=WorkerState.CONNECTED_TO_SYNC)
    send_update(req_socket, worker_id, update)
    logger.debug(f"updated state to {update.state}.")
    data_socket = None

    while True:
        # Wait for test config
        topic, config_json = sub_socket.recv_multipart()
        if topic.decode() == CoordinationSignal.FINISH.value:
            break
        if not topic.decode() == CoordinationSignal.CONFIG.value:
            continue

        config = TestConfig(**json.loads(config_json.decode()))
        logger.debug("Received config.")
        update = WorkerUpdate(
            state=WorkerState.RECEIVED_CONFIG, test_number=config.test_number
        )
        send_update(req_socket, worker_id, update)

        if isinstance(data_socket, zmq.Socket):
            data_socket.close()
            logger.debug("Closed previous data socket.")

        if role == Role.sender:
            data_socket = ctx.socket(zmq.PUB if config.pub else zmq.PUSH)
            data_socket.setsockopt(zmq.SNDHWM, config.sndhwm)
            data_socket.setsockopt(zmq.LINGER, 0)
            if settings.worker.sender_bind:
                # Sender binds to one port, all receivers connect to it
                addr = f"tcp://*:{group_setup_info.data_port}"
                retry_bind(data_socket, addr)
            else:
                # Sender connects to all receiver ports
                for i, receiver_port in enumerate(group_setup_info.receiver_ports):
                    addr = f"tcp://{settings.network.coordinator_ip}:{receiver_port}"
                    data_socket.connect(addr)
                    logger.debug(f"Connected to receiver {i} at {addr}.")
        else:
            data_socket = ctx.socket(zmq.SUB if config.pub else zmq.PULL)
            if config.pub:
                data_socket.setsockopt_string(zmq.SUBSCRIBE, "")
            data_socket.setsockopt(zmq.RCVHWM, config.rcvhwm)
            if settings.worker.sender_bind:
                # Receivers connect to sender's port
                addr = f"tcp://{settings.network.coordinator_ip}:{group_setup_info.data_port}"
                data_socket.connect(addr)
                logger.debug(f"Connected to sender at {addr}.")
            else:
                # Each receiver binds to its own port
                # Find this receiver's index in the group
                try:
                    my_port = group_setup_info.receiver_ports[group_setup_info.index]
                    addr = f"tcp://*:{my_port}"
                    retry_bind(data_socket, addr)
                except ValueError:
                    raise ValueError(f"{worker_id} not found in group setup")
        time.sleep(settings.worker.setup_delay_s)

        # Signal ready
        update = WorkerUpdate(
            state=WorkerState.READY_TO_TEST, test_number=config.test_number
        )
        send_update(req_socket, worker_id, update)

        # Wait for start signal
        if not sub_socket.recv_string() == CoordinationSignal.START.value:
            raise RuntimeError(f"Worker {worker_id} did not receive START signal.")

        logger.info(f"Starting test {config.test_number}.")

        # Run test
        result_data = run_test(
            role,
            config,
            data_socket,
            settings,
        )
        result = TestResult(
            worker_id=worker_id,
            role=role,
            config=config,
            **result_data,
        )

        # Send results
        update = WorkerUpdate(
            state=WorkerState.FINISHED_TEST,
            test_number=config.test_number,
            result=result,
        )
        send_update(req_socket, worker_id, update)
        logger.info(f"Finished test {config.test_number} and sent results.")

        # We need to send this END in a loop, because the receivers are not
        # guaranteed to receive it (PUSH/PULL does not always evenly distribute)
        # messages
        if role == Role.sender:
            logger.debug("Entering END message sending loop.")
            while True:
                try:
                    data_socket.send(b"END", flags=zmq.NOBLOCK)
                    time.sleep(0.001)
                except zmq.Again:
                    pass
                try:
                    signal, _ = sub_socket.recv_multipart(flags=zmq.NOBLOCK)
                    if signal.decode() == CoordinationSignal.STOP_END_LOOP.value:
                        logger.debug("Received STOP_END_LOOP signal.")
                        break
                except zmq.Again:
                    continue

        data_socket.close()

    logger.info("Shutting down.")
    ctx.destroy(linger=0)


def run_test(
    role: Role,
    config: TestConfig,
    data_socket: zmq.Socket,
    settings: "BenchmarkSettings",
):
    copy = not config.zero_copy

    def send():
        callback_to_msg: dict[ReceiveCallback, Callable[[int], bytes]] = {
            ReceiveCallback.NONE: lambda size: b" " * size,
            ReceiveCallback.WRITE_NPY: lambda size: np.random.randint(
                0, 255, size, dtype=np.uint8
            ).tobytes(),
        }
        msg = callback_to_msg[config.recv_callback](config.size)

        logger.debug(
            f"Starting to send {config.count} messages of size {config.size} bytes."
        )

        start_time = time.time()

        for _ in range(config.count):
            data_socket.send(msg, copy=copy)

        end_time = time.time()

        messages_sent = config.count
        throughput = calculate_throughput(
            messages_sent, config.size, start_time, end_time
        )
        return {
            "messages_sent": messages_sent,
            "throughput_mbps": throughput,
            "start_time": start_time,
            "end_time": end_time,
        }

    def receive():
        # Create callback using the factory
        callback = CallbackFactory.create_callback(config.recv_callback, settings)

        start_time = 0.0
        messages_received = 0
        first_message_received = False

        # Use END message protocol for many-to-one scenarios
        while True:
            msg = data_socket.recv(copy=copy)
            msg = msg.buffer if isinstance(msg, zmq.Frame) else msg
            if msg == b"END":
                logger.debug("Received END message, stopping reception.")
                break
            if not first_message_received:
                first_message_received = True
                # This is not "entirely" correct, but we count it as a rounding error
                # ... we tried signaling with a "START" message as well, but it is not
                # reliable because all the receivers may not get this START
                start_time = time.time()
            messages_received += 1
            callback(msg, messages_received, config)

        callback.finalize()

        end_time = time.time()
        throughput = calculate_throughput(
            messages_received, config.size, start_time, end_time
        )
        return {
            "messages_received": messages_received,
            "throughput_mbps": throughput,
            "start_time": start_time,
            "end_time": end_time,
        }

    method_map = {
        Role.sender: send,
        Role.receiver: receive,
    }

    return method_map[role]()
