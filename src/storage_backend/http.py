import asyncio
import time
from concurrent.futures import ThreadPoolExecutor
from typing import BinaryIO

import httpx
from opentelemetry import metrics, trace

from storage_backend.abc import StorageBackend

_tracer = trace.get_tracer("storage_backend")
_meter = metrics.get_meter("storage_backend")

_get_latency = _meter.create_histogram(
    "storage.http.get.duration",
    unit="s",
)


class HTTPStorage(StorageBackend):
    """Storage backend implementation for HTTP-based storage."""

    def __init__(self, base_url: str, headers: dict[str, str], max_concurrent: int = 1):
        self.base_url = base_url.rstrip("/")
        self.client = httpx.Client(headers=headers)
        self.headers = headers
        self._executor = ThreadPoolExecutor(max_workers=max_concurrent)
        self._semaphore = asyncio.Semaphore(max_concurrent)

    async def put(self, key: str, file: BinaryIO) -> None:
        """HTTP storage is read-only, so this method raises an error.

        Args:
            key: The key (path) to store the file under.
            file: A file-like object to upload.

        Raises:
            RuntimeError: Always raised since HTTP storage is read-only.
        """
        raise RuntimeError("HTTP backend is read-only")

    async def get(self, key: str) -> bytes:
        """Download a file from HTTP storage.

        Args:
            key: The key (path) to retrieve.

        Returns:
            The content of the file as bytes.
        """
        start = time.monotonic()
        url = f"{self.base_url}/{key}"
        with _tracer.start_as_current_span(
            "storage.http.get",
            attributes={
                "storage.key": key,
                "storage.base_url": str(self.base_url),
            },
        ):
            loop = asyncio.get_running_loop()
            async with self._semaphore:
                r = await loop.run_in_executor(self._executor, self.client.get, url)
            r.raise_for_status()
        _get_latency.record(
            time.monotonic() - start, attributes={"operation": "storage.http.get"}
        )
        return r.content

    def __repr__(self):
        return f"{self.__class__.__name__}(base_url={self.base_url!r}"
