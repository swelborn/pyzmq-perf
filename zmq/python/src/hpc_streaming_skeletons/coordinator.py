import logging
import pathlib
from datetime import datetime

# Import TYPE_CHECKING to avoid circular imports
from typing import TYPE_CHECKING, Any

import pandas as pd
from rich.logging import RichHandler

import zmq
from hpc_streaming_skeletons.models import (
    CoordinationSignal,
    Role,
    SetupInfo,
    TestResult,
    Worker,
    WorkerCreate,
    WorkerState,
    WorkerUpdate,
)
from hpc_streaming_skeletons.utils import req_poll, validate_msg

from .models import TestConfig

if TYPE_CHECKING:
    from .settings import BenchmarkSettings


def get_coordinator_logger(level=logging.INFO) -> logging.Logger:
    logger = logging.getLogger("coordinator")
    if not logger.hasHandlers():
        handler = RichHandler(
            rich_tracebacks=True, show_time=False, show_path=False, markup=False
        )
        formatter = logging.Formatter(
            "%(asctime)s [coordinator] %(message)s", datefmt="%Y-%m-%d %H:%M:%S"
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)
    logger.propagate = False
    logger.setLevel(level)
    return logger


logger = get_coordinator_logger()


class WorkerRegistry(dict[bytes, Worker]):
    def register(self, worker: Worker):
        self[worker.id] = worker

    def check_all_state(self, state: WorkerState) -> bool:
        if not self:
            return False
        return all(worker.state == state for worker in self.values())

    def check_all_test_number(self, test_number: int) -> bool:
        if not self:
            return False
        return all(worker.test_number == test_number for worker in self.values())

    def pair(self, id1: bytes, id2: bytes):
        worker1 = self.get(id1)
        worker2 = self.get(id2)
        if not worker1:
            raise ValueError(f"Worker with id {id1} not found.")
        if not worker2:
            raise ValueError(f"Worker with id {id2} not found.")
        worker1.pair_id = id2
        worker2.pair_id = id1

    @property
    def unpaired_senders(self) -> list[Worker]:
        return [
            worker
            for worker in self.values()
            if worker.role == Role.sender and not worker.pair_id
        ]

    @property
    def unpaired_receivers(self) -> list[Worker]:
        return [
            worker
            for worker in self.values()
            if worker.role == Role.receiver and not worker.pair_id
        ]

    @property
    def paired_workers(self) -> list[Worker]:
        return [worker for worker in self.values() if worker.pair_id is not None]

    def able_to_pair(self) -> bool:
        unpaired_senders = self.unpaired_senders
        unpaired_receivers = self.unpaired_receivers
        return bool(unpaired_senders and unpaired_receivers)

    @property
    def num_pairs(self) -> int:
        return len(self.paired_workers) // 2

    @property
    def num_workers(self) -> int:
        return len(self)


def update_worker(
    id: bytes,
    msg_bytes: bytes,
    registry: WorkerRegistry,
    router_socket: zmq.Socket,
    test_results: list[TestResult] | None = None,
):
    _worker_update = validate_msg(msg_bytes, WorkerUpdate)
    _worker = registry.get(id)
    if not _worker:
        raise ValueError(f"Worker {id} not found for update.")
    _worker.state = _worker_update.state
    _worker.test_number = _worker_update.test_number
    logger.debug(
        f"Worker {_worker.worker_id} updated to state {_worker.state} for test number {_worker.test_number}"
    )
    if test_results is not None and _worker_update.result is not None:
        test_results.append(_worker_update.result)
    router_socket.send_multipart([id, b"", b"ACK"])


def register_worker(
    id: bytes,
    msg_bytes: bytes,
    registry: WorkerRegistry,
    router_socket: zmq.Socket,
    settings: "BenchmarkSettings",
):
    _worker = validate_msg(msg_bytes, WorkerCreate)
    _worker = Worker(id=id, **_worker.model_dump())
    logger.info(f"Registering worker {_worker.worker_id} as {str(_worker.role)}")
    registry[id] = _worker

    if not registry.able_to_pair():
        return

    data_port = settings.network.data_port_start + registry.num_pairs
    sender = registry.unpaired_senders[0]
    receiver = registry.unpaired_receivers[0]

    logger.debug(
        f"Pairing {sender.worker_id} with {receiver.worker_id} on port {data_port}"
    )
    registry.pair(sender.id, receiver.id)

    # Tell workers about their peer and connection info
    setup_info = SetupInfo(data_port=data_port)
    setup_info_msg = setup_info.model_dump_json().encode()

    # Send setup to both workers in the pair
    router_socket.send_multipart([sender.id, b"", setup_info_msg])
    router_socket.send_multipart([receiver.id, b"", setup_info_msg])


def wait_for_workers_state(
    expected_state: WorkerState,
    test_number: int,
    registry: WorkerRegistry,
    poller: zmq.Poller,
    router_socket: zmq.Socket,
    settings: "BenchmarkSettings",
    test_results: list[TestResult] | None = None,
):
    while not (
        registry.check_all_state(expected_state)
        and registry.check_all_test_number(test_number)
    ):
        if not req_poll(poller, router_socket):
            continue
        id, _, msg_bytes = router_socket.recv_multipart()
        update_worker(id, msg_bytes, registry, router_socket, test_results)


def save_results(results: list[dict[str, Any]], file: pathlib.Path):
    if not results:
        logger.info("No results to save.")
        return
    file.parent.mkdir(parents=True, exist_ok=True)
    df = pd.DataFrame(results)
    if file.exists():
        df.to_csv(file, mode="a", header=False, index=False)
    else:
        df.to_csv(file, mode="w", header=True, index=False)
    logger.info(f"Results saved to {file}")


def save_settings(settings: "BenchmarkSettings", file: pathlib.Path):
    file.parent.mkdir(parents=True, exist_ok=True)
    with file.open("w") as f:
        f.write(settings.model_dump_json(indent=2))
    logger.info(f"Settings saved to {file}")


def coordinator(settings: "BenchmarkSettings", test_matrix: list[dict]):
    logger = get_coordinator_logger(settings.logging.get_level_int())
    ctx = zmq.Context()

    router_socket = ctx.socket(zmq.ROUTER)
    router_socket.bind(f"tcp://*:{settings.network.coordinator_router_port}")

    pub_socket = ctx.socket(zmq.PUB)
    pub_socket.bind(f"tcp://*:{settings.network.coordinator_pub_port}")

    registry = WorkerRegistry()

    # Prepare output files
    results_file = settings.output.results_file
    config_file = settings.output.config_file
    if settings.output.add_date_time:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        results_file = results_file.with_name(f"{timestamp}_{results_file.name}")
        config_file = config_file.with_name(f"{timestamp}_{config_file.name}")

    # Save settings once
    save_settings(settings, file=config_file)

    logger.info(f"Waiting for {settings.num_pairs * 2} workers to register and pair...")

    poller = zmq.Poller()
    poller.register(router_socket, zmq.POLLIN)

    while registry.num_pairs < settings.num_pairs or not registry.check_all_state(
        WorkerState.CONNECTED_TO_SYNC
    ):
        if not req_poll(poller, router_socket):
            continue

        # The empty part is the delimiter in a ROUTER socket message
        id, _, msg_bytes = router_socket.recv_multipart()

        if id not in registry:
            register_worker(id, msg_bytes, registry, router_socket, settings)
            logger.info(
                f"Registered {registry.num_workers} workers, {registry.num_pairs} pairs."
            )
        else:
            update_worker(id, msg_bytes, registry, router_socket)

    logger.info("All workers have been paired.")

    logger.info("Starting test execution loop.")
    test_results: list[TestResult] = []

    for i, test_config_dict in enumerate(test_matrix):
        config = TestConfig(test_number=i, **test_config_dict)
        logger.info(f"Running test: \n{config.model_dump_json(indent=2)}")

        # Distribute config
        pub_socket.send_multipart(
            [
                CoordinationSignal.CONFIG.value.encode(),
                config.model_dump_json().encode(),
            ]
        )
        logger.info("Config sent to all workers.")

        wait_for_workers_state(
            WorkerState.RECEIVED_CONFIG,
            test_number=i,
            registry=registry,
            poller=poller,
            router_socket=router_socket,
            settings=settings,
        )
        logger.info("All workers have received the config.")

        wait_for_workers_state(
            WorkerState.READY_TO_TEST,
            test_number=i,
            registry=registry,
            poller=poller,
            router_socket=router_socket,
            settings=settings,
        )
        logger.info("All workers are ready for the test.")

        pub_socket.send_string(CoordinationSignal.START.value)

        test_results.clear()
        wait_for_workers_state(
            WorkerState.FINISHED_TEST,
            test_number=i,
            registry=registry,
            poller=poller,
            router_socket=router_socket,
            settings=settings,
            test_results=test_results,
        )
        logger.info("All workers have finished the test.")

        save_results([r.model_dump() for r in test_results], file=results_file)
        logger.info(f"Test complete. Collected and saved {len(test_results)} results.")

    # Signal end of tests
    logger.info("All tests complete. Shutting down workers.")
    pub_socket.send_multipart([CoordinationSignal.FINISH.value.encode(), b""])

    ctx.destroy()
