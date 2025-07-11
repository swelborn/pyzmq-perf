import uuid
from enum import Enum
from pathlib import Path
from typing import TYPE_CHECKING, Annotated, Optional, Protocol

import numpy as np
from pydantic import AfterValidator, BaseModel, Field

from .models import ReceiveCallback, TestConfig
from .validators import is_non_negative

if TYPE_CHECKING:
    from .settings import BenchmarkSettings


class NpyCallbackBufferFormat(str, Enum):
    BINARY = "binary"
    NPY = "npy"


class CallbackSettings(BaseModel):
    # Settings for WRITE_NPY callback
    npy_base_directory: Optional[Path] = Field(
        default=None,
        description="Base directory for numpy callback output files.",
    )
    npy_buffer_size_bytes: Annotated[int, AfterValidator(is_non_negative)] = Field(
        default=1024 * 1024,
        description="Buffer size threshold in bytes before writing to file. Set to 0 to disable buffering.",
    )
    npy_buffer_format: NpyCallbackBufferFormat = Field(
        default=NpyCallbackBufferFormat.BINARY,
        description="Format for buffered output: 'binary' (raw bytes) or 'npy' (numpy format).",
    )


class MessageCallback(Protocol):
    """Protocol for message callbacks"""

    def __call__(self, msg: bytes, message_number: int, config: TestConfig) -> None:
        """Process a received message"""
        ...

    def finalize(self) -> None:
        """Finalize the callback"""
        ...


class BufferedNpyCallback:
    """A stateful callback that buffers messages before writing to disk."""

    def __init__(
        self, base_dir: Path, buffer_size: int, buffer_format: NpyCallbackBufferFormat
    ):
        # Create worker-specific subdirectory
        self.output_dir = base_dir / str(uuid.uuid4())

        try:
            self.output_dir.mkdir(parents=True, exist_ok=True)
        except Exception as e:
            raise RuntimeError(
                f"Failed to create output directory {self.output_dir}: {e}"
            ) from e

        self.buffer_size = buffer_size
        self.buffer_format = buffer_format

        # State variables
        self.message_buffer = bytearray()
        self.buffer_start_message = 1
        self.file_counter = 0
        self.message_count_in_buffer = 0
        self._test_number: Optional[int] = None
        self._has_test_number = False

    def __call__(self, msg: bytes, message_number: int, config: TestConfig) -> None:
        # Store test number on first call since each callback handles only one test
        if not self._has_test_number:
            self._test_number = config.test_number
            self._has_test_number = True

        # Add message to buffer
        self.message_buffer.extend(msg)
        self.message_count_in_buffer += 1

        # Check if buffer should be flushed
        if self._should_flush_buffer():
            self._flush_buffer()
            self._reset_buffer()
            self.file_counter += 1
            self.buffer_start_message = message_number + 1

    def _should_flush_buffer(self) -> bool:
        return (self.buffer_size == 0) or (len(self.message_buffer) >= self.buffer_size)

    def _reset_buffer(self) -> None:
        self.message_buffer.clear()
        self.message_count_in_buffer = 0

    def _flush_buffer(self) -> None:
        if not self.message_buffer or not self._has_test_number:
            return
        # Calculate end message number
        end_message = self.buffer_start_message + self.message_count_in_buffer - 1

        # Generate filename
        if self.buffer_start_message == end_message:
            basename = (
                f"test_{self._test_number:03d}_message_{self.buffer_start_message:06d}"
            )
        else:
            basename = f"test_{self._test_number:03d}_messages_{self.buffer_start_message:06d}_to_{end_message:06d}"

        suffix_map = {
            NpyCallbackBufferFormat.BINARY: ".bin",
            NpyCallbackBufferFormat.NPY: ".npy",
        }
        suffix = suffix_map.get(self.buffer_format, ".bin")
        filename = f"{basename}_part_{self.file_counter:03d}{suffix}"
        filepath = self.output_dir / filename

        # Write file
        try:
            if self.buffer_format == NpyCallbackBufferFormat.NPY:
                np_array = np.frombuffer(self.message_buffer, dtype=np.uint8)
                np.save(filepath, np_array)
            else:
                with open(filepath, "wb") as f:
                    f.write(self.message_buffer)
        except Exception as e:
            raise RuntimeError(f"Failed to write buffer to {filepath}: {e}") from e

    def finalize(self) -> None:
        # Flush any remaining buffer using the stored test number
        if self._has_test_number and self.message_buffer:
            self._flush_buffer()
        self._reset_buffer()


class NoneCallback:
    def __call__(self, msg: bytes, message_number: int, config: TestConfig) -> None:
        pass

    def finalize(self) -> None:
        pass


class CallbackFactory:
    @staticmethod
    def create_callback(
        callback_type: ReceiveCallback,
        settings: "BenchmarkSettings",
    ) -> MessageCallback:
        if callback_type == ReceiveCallback.NONE:
            return CallbackFactory._create_none_callback()
        elif callback_type == ReceiveCallback.WRITE_NPY:
            return CallbackFactory._create_npy_callback(settings)
        else:
            raise ValueError(f"Unknown callback type: {callback_type}")

    @staticmethod
    def _create_none_callback() -> MessageCallback:
        return NoneCallback()

    @staticmethod
    def _create_npy_callback(settings: "BenchmarkSettings") -> MessageCallback:
        base_dir = settings.callbacks.npy_base_directory
        if base_dir is None:
            raise ValueError(
                "Numpy callback requires a base directory. Please set 'npy_base_directory' in settings."
            )

        # Create buffered callback (directory creation handled in constructor)
        callback = BufferedNpyCallback(
            base_dir=base_dir,
            buffer_size=settings.callbacks.npy_buffer_size_bytes,
            buffer_format=settings.callbacks.npy_buffer_format,
        )

        return callback
