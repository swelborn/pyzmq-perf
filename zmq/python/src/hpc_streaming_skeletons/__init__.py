import multiprocessing
from typing import Annotated, Optional

import typer
from rich.console import Console

from hpc_streaming_skeletons.coordinator import coordinator as _coordinator
from hpc_streaming_skeletons.models import Role
from hpc_streaming_skeletons.settings import BenchmarkSettings
from hpc_streaming_skeletons.worker import worker

from .plot import plot

app = typer.Typer(
    help="HPC Streaming Skeletons: High-performance ZeroMQ benchmarking tool",
    rich_markup_mode="rich",
)


# Type annotations for CLI options
RoleT = Annotated[Role, typer.Option(help="Worker role: sender or receiver")]
CoordT = Annotated[bool, typer.Option(help="Run coordinator on this node")]
NumPairsT = Annotated[
    Optional[int],
    typer.Option(help="Number of sender/receiver pairs"),
]
ReceiversPerSenderT = Annotated[
    Optional[int],
    typer.Option(help="Number of receivers per sender (1 = pair mode, >1 = many-to-one mode)"),
]
SenderBindT = Annotated[
    Optional[bool],
    typer.Option(help="Senders bind to ports, receivers connect"),
]
CoordinatorIpT = Annotated[
    Optional[str], typer.Option(help="IP address of the coordinator")
]
ShortT = Annotated[
    Optional[bool],
    typer.Option(help="Use reduced test matrix for quick testing"),
]
LogLevelT = Annotated[
    Optional[str],
    typer.Option(help="Logging level: DEBUG, INFO, WARNING, ERROR, CRITICAL"),
]
ConfigFileT = Annotated[
    Optional[str], typer.Option(help="Path to .env configuration file (default: .env)")
]


def run(
    role: Role,
    coordinator: bool = False,
    num_pairs: int | None = None,
    receivers_per_sender: int | None = None,
    sender_bind: bool | None = None,
    coordinator_ip: str | None = None,
    short: bool | None = None,
    log_level: str | None = None,
    config_file: str | None = None,
):
    """
    Run HPC Streaming Skeletons benchmark.

    This tool coordinates high-performance ZeroMQ benchmarks between sender and receiver workers.
    Configuration is loaded from environment variables, .env files, and can be overridden by CLI flags.

    Examples:

    Run coordinator with 2 pairs (traditional mode):
        pyzmq-bench sender --coordinator --num-pairs 2

    Run coordinator with 2 senders, 4 receivers per sender (many-to-one mode):
        pyzmq-bench sender --coordinator --num-pairs 2 --receivers-per-sender 4

    Run worker connecting to remote coordinator:
        pyzmq-bench receiver --coordinator-ip 192.168.1.100

    Quick test with custom log level:
        pyzmq-bench sender --coordinator --short --log-level DEBUG
    """

    # Load settings from environment/config file
    if config_file:
        # Create settings with custom env file
        from pydantic_settings import SettingsConfigDict

        class CustomSettings(BenchmarkSettings):
            model_config = SettingsConfigDict(
                env_file=config_file,
                env_file_encoding="utf-8",
                env_prefix="PYZMQ_BENCH_",
                env_nested_delimiter="__",
                extra="ignore",
            )

        settings = CustomSettings()
    else:
        # Load settings from default locations
        settings = BenchmarkSettings()

    # Override settings with CLI arguments (only if explicitly provided)
    overrides = {}
    if num_pairs is not None:
        overrides["num_pairs"] = num_pairs
    if receivers_per_sender is not None:
        overrides["receivers_per_sender"] = receivers_per_sender
    if short is not None:
        overrides["short_test"] = short
    if log_level is not None:
        overrides["logging"] = {"level": log_level}
    if coordinator_ip is not None:
        overrides["network"] = {"coordinator_ip": coordinator_ip}
    if sender_bind is not None:
        overrides["worker"] = {"sender_bind": sender_bind}

    # Apply CLI overrides if any were provided
    if overrides:
        # Create new settings instance with overrides
        current_settings = settings.model_dump()

        # Deep merge overrides
        for key, value in overrides.items():
            if isinstance(value, dict) and key in current_settings:
                current_settings[key].update(value)
            else:
                current_settings[key] = value

        settings = BenchmarkSettings(**current_settings)

    # Configure logging based on final settings
    settings.configure_logging()

    # Generate test matrix based on settings
    test_matrix = settings.get_test_matrix()

    console = Console()
    console.print(
        f"🚀 Starting benchmark with [bold cyan]{len(test_matrix)}[/bold cyan] test configurations"
    )
    console.print(
        f"📊 Using [bold cyan]{settings.num_pairs}[/bold cyan] sender/receiver group(s)"
    )
    if settings.receivers_per_sender > 1:
        console.print(
            f"🔗 Many-to-One mode: [bold cyan]{settings.receivers_per_sender}[/bold cyan] receivers per sender"
        )
    console.print(
        f"🌐 Coordinator: [bold cyan]{settings.network.coordinator_ip}:{settings.network.coordinator_router_port}[/bold cyan]"
    )

    processes = []

    if coordinator:
        console.print("🎯 Starting coordinator process...")
        coordinator_process = multiprocessing.Process(
            target=_coordinator, args=(settings, test_matrix)
        )
        processes.append(coordinator_process)

    # Calculate total number of workers needed
    if settings.receivers_per_sender == 1:
        # Traditional pair mode
        total_workers = settings.num_pairs
        console.print(
            f"👥 Starting [bold cyan]{total_workers}[/bold cyan] [bold cyan]{role.value}[/bold cyan] worker(s)..."
        )
        for i in range(total_workers):
            worker_id = f"{role.value}-{i}"
            p = multiprocessing.Process(
                target=worker,
                args=(role, worker_id, settings),
            )
            processes.append(p)
    else:
        # Many-to-one mode
        if role == Role.sender:
            total_workers = settings.num_pairs
            console.print(
                f"👥 Starting [bold cyan]{total_workers}[/bold cyan] [bold cyan]{role.value}[/bold cyan] worker(s)..."
            )
            for i in range(total_workers):
                worker_id = f"{role.value}-{i}"
                p = multiprocessing.Process(
                    target=worker,
                    args=(role, worker_id, settings),
                )
                processes.append(p)
        else:  # Role.receiver
            total_workers = settings.num_pairs * settings.receivers_per_sender
            console.print(
                f"👥 Starting [bold cyan]{total_workers}[/bold cyan] [bold cyan]{role.value}[/bold cyan] worker(s)..."
            )
            for i in range(total_workers):
                worker_id = f"{role.value}-{i}"
                p = multiprocessing.Process(
                    target=worker,
                    args=(role, worker_id, settings),
                )
                processes.append(p)

    # Start all processes
    for p in processes:
        p.start()

    try:
        # Wait for all processes to complete
        for p in processes:
            p.join()

        console.print(
            "✅ [bold green]Benchmark processes finished successfully[/bold green]"
        )

        if coordinator:
            console.print(
                f"📄 Results saved to: [bold cyan]{settings.output.results_file}[/bold cyan]"
            )

    except KeyboardInterrupt:
        console.print("🛑 [bold red]Benchmark interrupted by user[/bold red]")
        for p in processes:
            if p.is_alive():
                p.terminate()
                p.join()


@app.command()
def sender(
    coordinator: CoordT = False,
    num_pairs: NumPairsT = None,
    receivers_per_sender: ReceiversPerSenderT = None,
    sender_bind: SenderBindT = None,
    coordinator_ip: CoordinatorIpT = None,
    short: ShortT = None,
    log_level: LogLevelT = None,
    config_file: ConfigFileT = None,
):
    """Run a sender worker."""
    run(
        role=Role.sender,
        coordinator=coordinator,
        num_pairs=num_pairs,
        receivers_per_sender=receivers_per_sender,
        sender_bind=sender_bind,
        coordinator_ip=coordinator_ip,
        short=short,
        log_level=log_level,
        config_file=config_file,
    )


@app.command()
def receiver(
    coordinator: CoordT = False,
    num_pairs: NumPairsT = None,
    receivers_per_sender: ReceiversPerSenderT = None,
    sender_bind: SenderBindT = None,
    coordinator_ip: CoordinatorIpT = None,
    short: ShortT = None,
    log_level: LogLevelT = None,
    config_file: ConfigFileT = None,
):
    """Run a receiver worker."""
    run(
        role=Role.receiver,
        coordinator=coordinator,
        num_pairs=num_pairs,
        receivers_per_sender=receivers_per_sender,
        sender_bind=sender_bind,
        coordinator_ip=coordinator_ip,
        short=short,
        log_level=log_level,
        config_file=config_file,
    )


@app.command()
def config(
    config_file: ConfigFileT = None,
):
    """Show current configuration settings."""
    console = Console()

    if config_file:
        # Create settings with custom env file
        from pydantic_settings import SettingsConfigDict

        class CustomSettings(BenchmarkSettings):
            model_config = SettingsConfigDict(
                env_file=config_file,
                env_file_encoding="utf-8",
                env_prefix="PYZMQ_BENCH_",
                env_nested_delimiter="__",
                extra="ignore",
            )

        settings = CustomSettings()
    else:
        settings = BenchmarkSettings()

    console.print("📋 Current Configuration:")
    console.print("=" * 50)

    # Display settings in a formatted way
    config_dict = settings.model_dump()

    def print_section(name: str, data: dict, indent: int = 0):
        prefix = "  " * indent
        console.print(f"{prefix}[bold cyan]{name.upper()}[/bold cyan]")
        for key, value in data.items():
            if isinstance(value, dict):
                print_section(key, value, indent + 1)
            else:
                console.print(f"{prefix}  {key}: [green]{value}[/green]")
        if indent == 0:
            console.print()

    for section_name, section_data in config_dict.items():
        if isinstance(section_data, dict):
            print_section(section_name, section_data)
        else:
            console.print(
                f"[bold cyan]{section_name.upper()}[/bold cyan]: [green]{section_data}[/green]"
            )


app.command()(plot)
