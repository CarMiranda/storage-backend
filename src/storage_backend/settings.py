import pathlib
from typing import Annotated, Literal

from pydantic import BaseModel, Field, RootModel

StorageBackends = Literal["local", "http", "s3", "gcs"]


class LocalSettings(BaseModel):
    """Settings for local filesystem storage."""

    backend: Literal["local"] = "local"
    root_dir: pathlib.Path


class HTTPSettings(BaseModel):
    """Settings for HTTP storage."""

    backend: Literal["http"] = "http"
    base_url: str
    headers: dict[str, str] = Field(default_factory=dict)


class S3Settings(BaseModel):
    """Settings for Amazon S3 storage."""

    backend: Literal["s3"] = "s3"
    bucket: str
    prefix: str = ""


class GCSSettings(BaseModel):
    """Settings for Google Cloud Storage."""

    backend: Literal["gcs"] = "gcs"
    bucket: str
    prefix: str = ""


class StorageSettings(RootModel):
    """Settings for storage backends."""

    root: dict[
        str,
        (
            Annotated[
                LocalSettings | S3Settings | GCSSettings | HTTPSettings,
                Field(discriminator="backend"),
            ]
        ),
    ]

    def __repr__(self):
        storages = ",".join([f"{key}: {value}" for key, value in self.root.items()])
        return f"{self.__class__.__name__}(root={{{storages!r}}})"
