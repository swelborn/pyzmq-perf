from enum import Enum

from pydantic import BaseModel


class Role(str, Enum):
    sender = "sender"
    receiver = "receiver"


class WorkerCreate(BaseModel):
    worker_id: str
    role: Role


class CoordinationSignal(str, Enum):
    CONFIG = "CONFIG"
    START = "START"
    FINISH = "FINISH"
    STOP_END_LOOP = "STOP_END_LOOP"


class ReceiveCallback(str, Enum):
    NONE = "none"
    WRITE_NPY = "write_npy"


class TestConfigCreate(BaseModel):
    count: int  # number of messages to send
    size: int  # size of messages
    zero_copy: bool  # use pyzmq zero-copy
    pub: bool  # true, use pub/sub. false, use push/pull
    rcvhwm: int  # puller hwm
    sndhwm: int  # sender hwm
    recv_callback: ReceiveCallback


class TestConfig(TestConfigCreate):
    test_number: int


class TestResult(BaseModel):
    worker_id: str
    role: Role
    config: TestConfig
    messages_sent: int | None = None
    messages_received: int | None = None
    throughput_mbps: float
    start_time: float
    end_time: float


class WorkerState(str, Enum):
    CONNECTING_TO_COORDINATOR = "CONNECTING_TO_COORDINATOR"
    CONNECTED_TO_SYNC = "CONNECTED_TO_SYNC"
    RECEIVED_CONFIG = "RECEIVED_CONFIG"
    READY_TO_TEST = "READY_TO_TEST"
    RUNNING_TEST = "RUNNING_TEST"
    FINISHED_TEST = "FINISHED_TEST"

    def transition_allowed(self, next_state: "WorkerState") -> bool:
        allowed_transitions = {
            WorkerState.CONNECTING_TO_COORDINATOR: [WorkerState.CONNECTED_TO_SYNC],
            WorkerState.CONNECTED_TO_SYNC: [WorkerState.RECEIVED_CONFIG],
            WorkerState.RECEIVED_CONFIG: [WorkerState.READY_TO_TEST],
            WorkerState.READY_TO_TEST: [WorkerState.RUNNING_TEST],
            WorkerState.RUNNING_TEST: [WorkerState.FINISHED_TEST],
            WorkerState.FINISHED_TEST: [WorkerState.RECEIVED_CONFIG],
        }
        return next_state in allowed_transitions.get(self, [])


class Worker(WorkerCreate):
    id: bytes
    state: WorkerState = WorkerState.CONNECTING_TO_COORDINATOR
    group_id: int | None = None  # For group mode
    test_number: int | None = None


class WorkerUpdate(BaseModel):
    state: WorkerState
    test_number: int | None = None
    result: TestResult | None = None


class Mode(str, Enum):
    coordinator = "coordinator"
    worker = "worker"


class GroupSetupInfo(BaseModel):
    """Setup information for group mode"""

    receiver_ports: list[
        int
    ]  # Individual ports for each receiver (when sender_bind=False)
    data_port: int
    group_id: int
    index: int  # index of the worker in the group
