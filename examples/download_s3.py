# /// script
# dependencies = [
#     "opentelemetry-exporter-otlp-proto-grpc>=1.39.1",
#     "opentelemetry-exporter-otlp-proto-http>=1.39.1",
#     "opentelemetry-exporter-prometheus>=0.60b1",
#     "opentelemetry-instrumentation>=0.60b1",
#     "opentelemetry-instrumentation-system-metrics>=0.60b1",
#     "opentelemetry-sdk>=1.39.1",
#     "prometheus-client>=0.15.0",
#     "storage-backend[s3]",
# ]
#
# [tool.uv.sources]
# storage-backend = { path = "../" }
# ///
import io
import pathlib
from typing import BinaryIO

from opentelemetry import metrics, trace
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.exporter.prometheus import PrometheusMetricReader
from opentelemetry.instrumentation.system_metrics import SystemMetricsInstrumentor
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.resources import SERVICE_NAME, Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import (
    BatchSpanProcessor,
)
from prometheus_client import start_http_server
from pydantic import computed_field
from pydantic_settings import BaseSettings

from storage_backend.abc import StorageBackend
from storage_backend.settings import (
    GCSSettings,
    HTTPSettings,
    LocalSettings,
    S3Settings,
    StorageSettings,
)

resource = Resource.create(attributes={SERVICE_NAME: "storage"})
provider = TracerProvider(resource=resource)
trace.set_tracer_provider(provider)
exporter = OTLPSpanExporter(endpoint="http://0.0.0.0:4317", insecure=True)
provider.add_span_processor(BatchSpanProcessor(exporter))
tracer = trace.get_tracer("storage_backend")
meter = metrics.get_meter("storage_backend")

metrics.set_meter_provider(MeterProvider(metric_readers=[PrometheusMetricReader()]))
SystemMetricsInstrumentor().instrument()


start_http_server(8000)


class Settings(BaseSettings):
    """Application settings."""

    storage_backend_config: pathlib.Path

    @computed_field
    @property
    def storage(self) -> StorageSettings:
        """Load storage settings from the specified JSON file."""
        with self.storage_backend_config.open() as fp:
            settings = StorageSettings.model_validate_json(fp.read())
        return settings


class PooledStorage:
    """A wrapper around a StorageBackend that limits the number of concurrent operations."""  # noqa: E501

    def __init__(self, backend: StorageBackend, max_concurrent: int):
        self.backend = backend
        self.semaphore = asyncio.Semaphore(max_concurrent)

    async def get(self, key: str) -> bytes:
        """Download a file from the backend, ensuring that no more than `max_concurrent` operations are in progress."""  # noqa: E501
        async with self.semaphore:
            return await self.backend.get(key)

    async def put(self, key: str, file: BinaryIO) -> None:
        """Upload a file to the backend, ensuring that no more than `max_concurrent` operations are in progress."""  # noqa: E501
        async with self.semaphore:
            await self.backend.put(key, file)


def build_storage(
    storage: LocalSettings | S3Settings | GCSSettings | HTTPSettings,
    max_concurrent: int = 1,
) -> StorageBackend:
    """Factory function to build a StorageBackend instance based on the provided settings.

    Args:
        storage: The storage backend settings.
        max_concurrent: The maximum number of concurrent operations allowed for the storage backend.

    Returns:
        An instance of StorageBackend configured according to the provided settings.
    """  # noqa: E501
    match storage.backend:
        case "local":
            assert storage.root_dir is not None

            from storage_backend.local import LocalStorage

            return LocalStorage(root=storage.root_dir)
        case "http":
            assert storage.base_url is not None

            from storage_backend.http import HTTPStorage

            return HTTPStorage(base_url=storage.base_url, headers=storage.headers)
        case "s3":
            assert storage.bucket is not None

            from storage_backend.s3 import S3Storage

            return S3Storage(
                bucket=storage.bucket,
                prefix=storage.prefix,
                max_concurrent=max_concurrent,
            )
        case "gcs":
            assert storage.bucket is not None

            from storage_backend.gcs import GCSStorage

            return GCSStorage(bucket=storage.bucket, prefix=storage.prefix)
        case _:
            raise NotImplementedError("")


async def tagged(key: str, coro):
    """Helper function to tag the result of a coroutine with its key for easier tracking in logs and metrics."""  # noqa: E501
    return key, await coro


async def download_images(
    images: list[str], input: StorageBackend, output: StorageBackend
):
    """Download a list of images from the input storage backend and upload them to the output storage backend.

    Args:
        images: A list of image keys to download and upload.
        input: The storage backend to download the images from.
        output: The storage backend to upload the images to.
    """  # noqa: E501
    tasks = [asyncio.create_task(tagged(image, input.get(image))) for image in images]

    for fut in asyncio.as_completed(tasks):
        image, result = await fut
        await output.put(image, io.BytesIO(result))


images = [f"file{i}.bin" for i in range(1, 11)] * 10

if __name__ == "__main__":
    import asyncio
    from datetime import datetime as dt

    settings = Settings()

    input_sb = settings.storage.root.get("images_source")
    output_sb = settings.storage.root.get("images_destination")

    if input_sb is None:
        raise RuntimeError("Missing `images_source` storage backend config.")
    if output_sb is None:
        raise RuntimeError("Missing `images_destination` storage backend config.")

    input_storage = build_storage(input_sb, max_concurrent=8)
    output_storage = build_storage(output_sb, max_concurrent=8)

    with tracer.start_as_current_span(f"run_{dt.now().strftime('%Y%m%d_%H%M%S')}"):
        loop = asyncio.new_event_loop()
        for _ in range(10):
            task = download_images(images, input_storage, output_storage)
            task = loop.create_task(task)
            loop.run_until_complete(asyncio.wait([task]))

    input("Done. Press Enter to exit...")
