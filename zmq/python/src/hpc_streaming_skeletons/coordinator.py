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
    GroupSetupInfo,
    Role,
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
    def __init__(self):
        super().__init__()
        self._next_group_id = 0
        self._next_port_offset = 0

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

    def create_group(self, sender_id: bytes, receiver_ids: list[bytes]) -> int:
        """Create a group for any number of receivers per sender"""
        group_id = self._next_group_id
        self._next_group_id += 1

        # Assign group ID to sender
        sender = self.get(sender_id)
        if not sender:
            raise ValueError(f"Sender with id {sender_id} not found.")
        sender.group_id = group_id

        # Assign group ID to all receivers
        for receiver_id in receiver_ids:
            receiver = self.get(receiver_id)
            if not receiver:
                raise ValueError(f"Receiver with id {receiver_id} not found.")
            receiver.group_id = group_id

        return group_id

    def allocate_ports(
        self, num_receivers: int, sender_bind: bool
    ) -> tuple[int, list[int]]:
        """Allocate ports for a group based on sender_bind setting"""
        if sender_bind:
            # Sender binds to one port, all receivers connect to it
            data_port = self._next_port_offset
            receiver_ports = [data_port] * num_receivers
            self._next_port_offset += 1
        else:
            # Each receiver binds to its own port, sender connects to all
            data_port = self._next_port_offset
            receiver_ports = [self._next_port_offset + i for i in range(num_receivers)]
            self._next_port_offset += num_receivers

        return data_port, receiver_ports

    @property
    def unpaired_senders(self) -> list[Worker]:
        return [
            worker
            for worker in self.values()
            if worker.role == Role.sender and worker.group_id is None
        ]

    @property
    def unpaired_receivers(self) -> list[Worker]:
        return [
            worker
            for worker in self.values()
            if worker.role == Role.receiver and worker.group_id is None
        ]

    @property
    def grouped_workers(self) -> list[Worker]:
        return [worker for worker in self.values() if worker.group_id is not None]

    def able_to_group(self, receivers_per_sender: int) -> bool:
        unpaired_senders = self.unpaired_senders
        unpaired_receivers = self.unpaired_receivers
        return (
            len(unpaired_senders) > 0
            and len(unpaired_receivers) >= receivers_per_sender
        )

    @property
    def num_groups(self) -> int:
        group_ids = set()
        for worker in self.values():
            if worker.group_id is not None:
                group_ids.add(worker.group_id)
        return len(group_ids)

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

    # Always use group mode (pair mode is just group mode with 1 receiver)
    if not registry.able_to_group(settings.receivers_per_sender):
        return

    # Get one sender and required number of receivers
    sender = registry.unpaired_senders[0]
    receivers = registry.unpaired_receivers[: settings.receivers_per_sender]

    if len(receivers) < settings.receivers_per_sender:
        return  # Not enough receivers yet

    # Create group
    receiver_ids = [r.id for r in receivers]
    group_id = registry.create_group(sender.id, receiver_ids)

    # Allocate ports based on sender_bind setting
    port_offset, receiver_ports = registry.allocate_ports(
        len(receivers), settings.worker.sender_bind
    )
    data_port = settings.network.data_port_start + port_offset
    actual_receiver_ports = [
        settings.network.data_port_start + port for port in receiver_ports
    ]

    if settings.worker.sender_bind:
        logger.debug(
            f"Creating group {group_id}: sender {sender.worker_id} binds to port {data_port}, "
            f"{len(receivers)} receivers connect to it"
        )
    else:
        logger.debug(
            f"Creating group {group_id}: sender {sender.worker_id} connects to {len(receivers)} receivers "
            f"on ports {actual_receiver_ports}"
        )

    # Send setup to sender
    msg = GroupSetupInfo(
        data_port=data_port,
        receiver_ports=actual_receiver_ports,
        group_id=group_id,
        index=0,
    )
    _msg = msg.model_dump_json().encode()
    router_socket.send_multipart([sender.id, b"", _msg])

    # Send setup to all receivers
    for index, receiver in enumerate(receivers):
        msg.index = index
        _msg = msg.model_dump_json().encode()
        router_socket.send_multipart([receiver.id, b"", _msg])


def wait_for_workers_state(
    expected_state: WorkerState,
    test_number: int,
    registry: WorkerRegistry,
    poller: zmq.Poller,
    router_socket: zmq.Socket,
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

    # Calculate expected worker counts
    expected_workers = settings.num_pairs * (1 + settings.receivers_per_sender)
    expected_groups = settings.num_pairs

    logger.info(
        f"Waiting for {expected_workers} workers to register and form {expected_groups} groups..."
    )

    poller = zmq.Poller()
    poller.register(router_socket, zmq.POLLIN)

    # Wait for all workers to register and form groups
    while True:
        ready = registry.num_groups >= settings.num_pairs and registry.check_all_state(
            WorkerState.CONNECTED_TO_SYNC
        )

        if ready:
            break

        if not req_poll(poller, router_socket):
            continue

        # The empty part is the delimiter in a ROUTER socket message
        id, _, msg_bytes = router_socket.recv_multipart()

        if id not in registry:
            register_worker(id, msg_bytes, registry, router_socket, settings)
            logger.info(
                f"Registered {registry.num_workers} workers, {registry.num_groups} groups."
            )
        else:
            update_worker(id, msg_bytes, registry, router_socket)

    logger.info("All workers have been grouped.")

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
        )
        logger.info("All workers have received the config.")

        wait_for_workers_state(
            WorkerState.READY_TO_TEST,
            test_number=i,
            registry=registry,
            poller=poller,
            router_socket=router_socket,
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
            test_results=test_results,
        )
        logger.info("All workers have finished the test.")

        save_results([r.model_dump() for r in test_results], file=results_file)
        logger.info(f"Test complete. Collected and saved {len(test_results)} results.")

    # Signal end of tests
    logger.info("All tests complete. Shutting down workers.")
    pub_socket.send_multipart([CoordinationSignal.FINISH.value.encode(), b""])

    ctx.destroy()
