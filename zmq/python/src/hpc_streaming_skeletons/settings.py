import logging
from itertools import product
from pathlib import Path
from typing import List

from pydantic import BaseModel, Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class LoggingSettings(BaseModel):
    level: str = Field(
        default="INFO",
        description="Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)",
    )
    format: str = Field(
        default="%(asctime)s [%(levelname)s] [%(name)s] %(message)s",
        description="Log message format string",
    )

    @field_validator("level")
    @classmethod
    def validate_log_level(cls, v: str) -> str:
        level_upper = v.upper()
        valid_levels = {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}
        if level_upper not in valid_levels:
            raise ValueError(f"Invalid log level: {v}. Must be one of {valid_levels}")
        return level_upper

    def get_level_int(self) -> int:
        return getattr(logging, self.level)


class NetworkSettings(BaseModel):
    coordinator_ip: str = Field(
        default="127.0.0.1", description="IP address of the coordinator node"
    )
    coordinator_router_port: int = Field(
        default=5555,
        description="Port for coordinator ROUTER socket (worker registration/updates)",
    )
    coordinator_pub_port: int = Field(
        default=5556,
        description="Port for coordinator PUB socket (test coordination signals)",
    )
    data_port_start: int = Field(
        default=6000,
        description="Starting port number for worker-to-worker data connections",
    )

    @field_validator(
        "coordinator_router_port", "coordinator_pub_port", "data_port_start"
    )
    @classmethod
    def validate_port(cls, v: int) -> int:
        if not (1024 <= v <= 65535):
            raise ValueError(f"Port must be between 1024 and 65535, got {v}")
        return v


class TestMatrixSettings(BaseModel):
    message_counts: List[int] = Field(
        default=[100001], description="List of message counts to test"
    )
    message_sizes: List[int] = Field(
        default=[64, 256, 1024, 4096, 16384, 65536, 262144, 1048576, 4194304, 10485760],
        description="List of message sizes in bytes to test",
    )
    max_message_size: int = Field(
        default=10 * 1024 * 1024,  # 10 MiB
        description="Maximum message size in bytes (sizes above this will be filtered out)",
    )
    zero_copy_options: List[bool] = Field(
        default=[True, False], description="Whether to test zero-copy mode"
    )
    pub_sub_options: List[bool] = Field(
        default=[False], description="Whether to test PUB/SUB pattern (vs PUSH/PULL)"
    )
    send_hwm_values: List[int] = Field(
        default=[100], description="High water mark values for send sockets"
    )
    recv_hwm_values: List[int] = Field(
        default=[100], description="High water mark values for receive sockets"
    )

    @field_validator(
        "message_counts", "message_sizes", "send_hwm_values", "recv_hwm_values"
    )
    @classmethod
    def validate_positive_lists(cls, v: List[int]) -> List[int]:
        if not v:
            raise ValueError("List cannot be empty")
        for val in v:
            if val <= 0:
                raise ValueError(f"All values must be positive, got {val}")
        return v

    @field_validator("max_message_size")
    @classmethod
    def validate_max_message_size(cls, v: int) -> int:
        if v <= 0:
            raise ValueError("Max message size must be positive")
        if v > 100 * 1024 * 1024:  # 100 MiB
            raise ValueError("Max message size cannot exceed 100 MiB")
        return v

    def get_filtered_message_sizes(self) -> List[int]:
        return [size for size in self.message_sizes if size <= self.max_message_size]


class WorkerSettings(BaseModel):
    sender_bind: bool = Field(
        default=False,
        description="Whether senders bind to ports (receivers connect) or vice versa",
    )
    setup_delay_s: float = Field(
        default=1.0,
        description="Delay in seconds after socket setup before signaling ready",
    )


class OutputSettings(BaseModel):
    add_date_time: bool = Field(
        default=True,
        description="Whether to add date/time prefix to output files",
    )
    results_file: Path = Field(
        default=Path("out/results.csv"),
        description="Path to save test results CSV file",
    )
    config_file: Path = Field(
        default=Path("out/config.json"),
        description="Path to save test configuration JSON file",
    )
    plot_show: bool = Field(
        default=False, description="Whether to display plots interactively"
    )
    plot_figure_size: tuple[float, float] = Field(
        default=(10, 6), description="Figure size for plots as (width, height)"
    )

    @field_validator("results_file")
    def validate_results_file(cls, v: Path) -> Path:
        if v.suffix != ".csv":
            raise ValueError("Results file must have .csv extension")
        return v


class BenchmarkSettings(BaseSettings):
    """
    Main settings configuration for HPC Streaming Skeletons benchmarking.

    Settings are loaded from:
    1. Default values defined in the nested models
    2. Environment variables (with PYZMQ_BENCH_ prefix)
    3. .env file in the current directory
    4. Explicit arguments passed to the constructor

    Environment variable examples:
    - PYZMQ_BENCH_LOGGING__LEVEL=DEBUG
    - PYZMQ_BENCH_NETWORK__COORDINATOR_IP=192.168.1.100
    - PYZMQ_BENCH_TEST_MATRIX__MESSAGE_COUNTS='[1000, 10000]'
    """

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        env_prefix="PYZMQ_BENCH_",
        env_nested_delimiter="__",
        extra="ignore",
    )

    # Nested configuration sections
    logging: LoggingSettings = Field(
        default_factory=LoggingSettings, description="Logging configuration"
    )
    network: NetworkSettings = Field(
        default_factory=NetworkSettings, description="Network and ZeroMQ configuration"
    )
    test_matrix: TestMatrixSettings = Field(
        default_factory=TestMatrixSettings,
        description="Test matrix generation settings",
    )
    worker: WorkerSettings = Field(
        default_factory=WorkerSettings, description="Worker behavior settings"
    )
    output: OutputSettings = Field(
        default_factory=OutputSettings, description="Output and results settings"
    )

    # Top-level benchmark settings
    num_pairs: int = Field(
        default=1,
        description="Number of groups to create (each group has 1 sender + N receivers)",
    )
    receivers_per_sender: int = Field(
        default=1,
        description="Number of receivers per sender (1 = one-to-one, >1 = one-to-many)",
    )
    short_test: bool = Field(
        default=False, description="Use a reduced test matrix for quick testing"
    )

    @field_validator("num_pairs")
    @classmethod
    def validate_num_pairs(cls, v: int) -> int:
        if v <= 0:
            raise ValueError("Number of groups must be positive")
        return v

    @field_validator("receivers_per_sender")
    @classmethod
    def validate_receivers_per_sender(cls, v: int) -> int:
        if v <= 0:
            raise ValueError("Number of receivers per sender must be positive")
        return v

    def get_test_matrix(self) -> List[dict]:
        if self.short_test:
            # Override for short test mode
            counts = [100001]
            sizes = [64, 256]
            zero_copies = [False]
            pubs = [False]
            sndhwms = [100]
            rcvhwms = [100]
        else:
            counts = self.test_matrix.message_counts
            sizes = self.test_matrix.get_filtered_message_sizes()
            zero_copies = self.test_matrix.zero_copy_options
            pubs = self.test_matrix.pub_sub_options
            sndhwms = self.test_matrix.send_hwm_values
            rcvhwms = self.test_matrix.recv_hwm_values

        test_combinations = product(counts, sizes, zero_copies, pubs, sndhwms, rcvhwms)

        return [
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

    def configure_logging(self) -> None:
        logging.basicConfig(
            level=self.logging.get_level_int(),
            format=self.logging.format,
            force=True,
        )


# Global settings instance - can be overridden by importing modules
settings = BenchmarkSettings()
