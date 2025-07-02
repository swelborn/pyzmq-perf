import multiprocessing
from itertools import product
from typing import Annotated

import typer

from hpc_streaming_skeletons.coordinator import coordinator as _coordinator
from hpc_streaming_skeletons.models import Role
from hpc_streaming_skeletons.worker import worker

app = typer.Typer()


def generate_test_matrix(short_test: bool = False):
    if short_test:
        counts = [100001]
        sizes = [64, 256]
        zero_copies = [False]
        pubs = [False]
        sndhwms = [100]
        rcvhwms = [100]
    else:
        counts = [100001]
        sizes = [64, 256, 1024, 4096, 16384, 65536, 262144, 1048576, 4194304, 10485760]
        max_size = 10 * 2**20  # 10 MiB
        sizes = [size for size in sizes if size <= max_size]  # threshold
        zero_copies = [True, False]
        pubs = [False]
        sndhwms = [100]
        rcvhwms = [100]

    test_combinations = product(counts, sizes, zero_copies, pubs, sndhwms, rcvhwms)

    test_matrix = [
        {
            "count": count,
            "size": size,
            "zero_copy": zero_copy,
            "pub": pub,
            "sndhwm": sndhwm,
            "rcvhwm": rcvhwm,
        }
        for count, size, zero_copy, pub, sndhwm, rcvhwm in test_combinations
    ]

    return test_matrix


RoleT = Annotated[Role, typer.Option(help="sender or receiver")]
CoordT = Annotated[bool, typer.Option(help="coordinator node or worker-only node")]
NumPairsT = Annotated[int, typer.Option(help="Number of sender/receiver pairs")]
SenderBindT = Annotated[bool, typer.Option(help="Senders bind, receivers connect")]
CoordinatorIpT = Annotated[str, typer.Option(help="IP address for the coordinator")]
ShortT = Annotated[bool, typer.Option(help="Use a few small sizes for quick tests")]
LogLevelT = Annotated[
    str, typer.Option(help="Logging level (DEBUG, INFO, WARNING, ERROR)")
]


@app.command()
def main(
    role: RoleT,
    coordinator: CoordT = False,
    num_pairs: NumPairsT = 1,
    sender_bind: SenderBindT = False,
    coordinator_ip: CoordinatorIpT = "127.0.0.1",
    short: ShortT = False,
    log_level: LogLevelT = "INFO",
):
    matrix = generate_test_matrix(short_test=short)
    processes = []

    if coordinator:
        coordinator_process = multiprocessing.Process(
            target=_coordinator, args=(num_pairs, matrix, log_level)
        )
        processes.append(coordinator_process)

    for i in range(num_pairs):
        worker_id = f"{role.value}-{i}"
        p = multiprocessing.Process(
            target=worker,
            args=(role, worker_id, coordinator_ip, sender_bind, log_level),
        )
        processes.append(p)

    for p in processes:
        p.start()

    for p in processes:
        p.join()

    print("Benchmark processes finished on this node.")
