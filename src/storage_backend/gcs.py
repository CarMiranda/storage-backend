import asyncio
import time
from concurrent.futures import ThreadPoolExecutor
from typing import BinaryIO

from google.cloud import storage
from opentelemetry import metrics, trace

from storage_backend.abc import StorageBackend

_tracer = trace.get_tracer("storage_backend")
_meter = metrics.get_meter("storage_backend")

_put_latency = _meter.create_histogram(
    "storage.gcs.put.duration",
    unit="s",
)
_get_latency = _meter.create_histogram(
    "storage.gcs.get.duration",
    unit="s",
)


class GCSStorage(StorageBackend):
    """Storage backend implementation for Google Cloud Storage."""

    def __init__(self, bucket: str, prefix: str = "", max_concurrent: int = 1):
        self.prefix = prefix.lstrip("/") + "/"
        client = storage.Client()
        self.bucket = client.bucket(bucket)
        self._executor = ThreadPoolExecutor(max_workers=max_concurrent)
        self._semaphore = asyncio.Semaphore(max_concurrent)

    async def put(self, key: str, file: BinaryIO) -> None:
        """Upload a file to GCS.

        Args:
            key: The key (path) to store the file under.
            file: A file-like object to upload.
        """
        start = time.monotonic()
        with _tracer.start_as_current_span(
            "storage.gcs.put",
            attributes={
                "storage.key": key,
                "storage.bucket": str(self.bucket),
            },
        ):
            blob = self.bucket.blob(self.prefix + key)
            loop = asyncio.get_running_loop()
            async with self._semaphore:
                await loop.run_in_executor(self._executor, blob.upload_from_file, file)

        _put_latency.record(
            time.monotonic() - start, attributes={"operation": "storage.gcs.put"}
        )

    async def get(self, key: str) -> bytes:
        """Download a file from GCS.

        Args:
            key: The key (path) of the file to download.

        Returns:
            The contents of the file as bytes.
        """
        start = time.monotonic()
        with _tracer.start_as_current_span(
            "storage.gcs.get",
            attributes={
                "storage.key": key,
                "storage.bucket": str(self.bucket),
            },
        ):
            blob = self.bucket.blob(self.prefix + key)
            loop = asyncio.get_running_loop()
            async with self._semaphore:
                out = await loop.run_in_executor(self._executor, blob.download_as_bytes)

        _get_latency.record(
            time.monotonic() - start, attributes={"operation": "storage.gcs.get"}
        )
        return out

    def __repr__(self):
        return (
            f"{self.__class__.__name__}(bucket={self.bucket!r}, prefix={self.prefix!r})"
        )
