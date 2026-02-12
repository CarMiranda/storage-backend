import pathlib
import time
from typing import BinaryIO

from opentelemetry import metrics, trace

from storage_backend.abc import StorageBackend

_tracer = trace.get_tracer("storage_backend")
_meter = metrics.get_meter("storage_backend")

_put_latency = _meter.create_histogram(
    "storage.local.put.duration",
    unit="s",
)
_get_latency = _meter.create_histogram(
    "storage.local.get.duration",
    unit="s",
)


class LocalStorage(StorageBackend):
    """Storage backend implementation for local filesystem."""

    def __init__(self, root: pathlib.Path):
        self.root = root
        self.root.mkdir(parents=True, exist_ok=True)

    async def put(self, key: str, file: BinaryIO) -> None:
        """Store a file in the local filesystem.

        Args:
            key: The key (path) to store the file under.
            file: A file-like object to store.
        """
        start = time.monotonic()
        with _tracer.start_as_current_span(
            "storage.local.put",
            attributes={
                "storage.bucket": str(self.root),
                "storage.key": key,
            },
        ):
            path = self.root / key
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(file.read())
        _put_latency.record(
            time.monotonic() - start, attributes={"operation": "storage.local.put"}
        )

    async def get(self, key: str) -> bytes:
        """Download a file from the local filesystem.

        Args:
            key: The key (path) to retrieve the file from.

        Returns:
            The contents of the file as bytes.
        """
        start = time.monotonic()
        with _tracer.start_as_current_span(
            "storage.local.get",
            attributes={
                "storage.bucket": str(self.root),
                "storage.key": key,
            },
        ):
            path = self.root / key
            if not path.exists():
                raise FileNotFoundError(key)
            out = path.read_bytes()
        _get_latency.record(
            time.monotonic() - start, attributes={"operation": "storage.local.get"}
        )
        return out

    def __repr__(self):
        return f"{self.__class__.__name__}(root={self.root!r})"
