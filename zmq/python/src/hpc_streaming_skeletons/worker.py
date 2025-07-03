import json
import logging
import time

# Import TYPE_CHECKING to avoid circular imports
from typing import TYPE_CHECKING

from rich.logging import RichHandler

import zmq
from hpc_streaming_skeletons.models import (
    CoordinationSignal,
    Role,
    SetupInfo,
    TestConfig,
    TestResult,
    WorkerCreate,
    WorkerState,
    WorkerUpdate,
)
from hpc_streaming_skeletons.utils import calculate_throughput

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

    setup_info = SetupInfo.model_validate_json(setup_info_raw)
    data_port = setup_info.data_port
    sync_address = f"tcp://{settings.network.coordinator_ip}:{settings.network.coordinator_pub_port}"
    logger.debug(f"Received setup info: \n{setup_info.model_dump_json(indent=2)}")

    # Subscribe to sync signals
    sub_socket = ctx.socket(zmq.SUB)
    sub_socket.connect(sync_address)
    sub_socket.setsockopt(zmq.SUBSCRIBE, b"")
    time.sleep(settings.worker.setup_delay_s)

    logger.debug("connected to coordinator and sync channel.")

    update = WorkerUpdate(state=WorkerState.CONNECTED_TO_SYNC)
    send_update(req_socket, worker_id, update)
    logger.debug(f"updated state to {update.state}.")

    while True:
        # Wait for test config
        topic, config_json = sub_socket.recv_multipart()
        if topic.decode() == CoordinationSignal.FINISH.value:
            break

        config = TestConfig(**json.loads(config_json.decode()))
        logger.debug("Received config.")
        update = WorkerUpdate(
            state=WorkerState.RECEIVED_CONFIG, test_number=config.test_number
        )
        send_update(req_socket, worker_id, update)

        # Setup data socket
        if role == Role.sender:
            data_socket = ctx.socket(zmq.PUB if config.pub else zmq.PUSH)
            data_socket.setsockopt(zmq.SNDHWM, config.sndhwm)
            if settings.worker.sender_bind:
                addr = f"tcp://*:{data_port}"
                data_socket.bind(addr)
                logger.debug(f"Bound to {addr}.")
            else:
                addr = f"tcp://{settings.network.coordinator_ip}:{data_port}"
                data_socket.connect(addr)
                logger.debug(f"Connected to {addr}.")
        else:
            data_socket = ctx.socket(zmq.SUB if config.pub else zmq.PULL)
            if config.pub:
                data_socket.setsockopt_string(zmq.SUBSCRIBE, "")
            data_socket.setsockopt(zmq.RCVHWM, config.rcvhwm)
            if not settings.worker.sender_bind:
                addr = f"tcp://*:{data_port}"
                data_socket.bind(addr)
                logger.debug(f"Bound to {addr}.")
            else:
                addr = f"tcp://{settings.network.coordinator_ip}:{data_port}"
                data_socket.connect(addr)
                logger.debug(f"Connected to {addr}.")
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
        result_data = run_test(role, config, data_socket)
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
        data_socket.close()
        logger.info(f"Finished test {config.test_number} and sent results.")

    logger.info("Shutting down.")
    ctx.destroy()


def run_test(role: Role, config: TestConfig, data_socket: zmq.Socket):
    data = b" " * config.size
    copy = not config.zero_copy

    def send():
        start_time = 0.0
        first_message_sent = False
        for _ in range(config.count):
            data_socket.send(data, copy=copy)
            if not first_message_sent:
                first_message_sent = True
                start_time = time.time()
        end_time = time.time()
        messages_sent = config.count - 1
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
        start_time = 0.0
        # start at -1 to account for the first message not being counted
        messages_received = -1
        first_message_received = False
        for _ in range(config.count):
            data_socket.recv(copy=copy)
            if not first_message_received:
                first_message_received = True
                start_time = time.time()
            messages_received += 1
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
