import asyncio
import io
import time
from concurrent.futures import ThreadPoolExecutor
from typing import BinaryIO

import boto3
from opentelemetry import metrics, trace

from storage_backend.abc import StorageBackend

_tracer = trace.get_tracer("storage_backend")
_meter = metrics.get_meter("storage_backend")

_put_latency = _meter.create_histogram(
    "storage.s3.put.duration",
    unit="s",
)
_get_latency = _meter.create_histogram(
    "storage.s3.get.duration",
    unit="s",
)


class S3Storage(StorageBackend):
    """Storage backend implementation for Amazon S3."""

    def __init__(self, bucket: str, prefix: str = "", max_concurrent: int = 1):
        self.bucket = bucket.strip("/")
        self.prefix = prefix.strip("/") + "/"
        self.client = boto3.client("s3")
        self._executor = ThreadPoolExecutor(max_workers=max_concurrent)
        self._semaphore = asyncio.Semaphore(max_concurrent)

    async def put(self, key: str, file: BinaryIO) -> None:
        """Upload a file to S3.

        Args:
            key: The key (path) to store the file under.
            file: A file-like object to upload.
        """
        start = time.monotonic()
        with _tracer.start_as_current_span(
            "storage.s3.put",
            attributes={
                "storage.key": key,
                "storage.bucket": str(self.bucket),
            },
        ):
            loop = asyncio.get_running_loop()
            async with self._semaphore:
                await loop.run_in_executor(
                    self._executor,
                    self.client.upload_fileobj,
                    file,
                    self.bucket,
                    self.prefix + key.lstrip("/"),
                )

        _put_latency.record(
            time.monotonic() - start, attributes={"operation": "storage.s3.put"}
        )

    async def get(self, key: str) -> bytes:
        """Download a file from S3.

        Args:
            key: The key (path) of the file to download.

        Returns:
            The contents of the file as bytes.
        """
        start = time.monotonic()
        with _tracer.start_as_current_span(
            "storage.s3.get",
            attributes={
                "storage.key": key,
                "storage.bucket": str(self.bucket),
            },
        ):
            buf = io.BytesIO()
            loop = asyncio.get_running_loop()
            async with self._semaphore:
                await loop.run_in_executor(
                    self._executor,
                    self.client.download_fileobj,
                    self.bucket,
                    self.prefix + key.lstrip("/"),
                    buf,
                )
            out = buf.getvalue()
        _get_latency.record(
            time.monotonic() - start, attributes={"operation": "storage.s3.get"}
        )
        return out

    def __repr__(self):
        return (
            f"{self.__class__.__name__}(bucket={self.bucket!r}, prefix={self.prefix!r})"
        )
