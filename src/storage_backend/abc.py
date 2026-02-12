import abc
from typing import BinaryIO


class StorageBackend(abc.ABC):
    """Abstract storage interface."""

    @abc.abstractmethod
    async def put(self, key: str, file: BinaryIO) -> None:
        """Upload a file to the storage backend.

        Args:
            key: The key (path) to store the file under.
            file: A file-like object to upload.
        """
        raise NotImplementedError

    @abc.abstractmethod
    async def get(self, key: str) -> bytes:
        """Download a file from the storage backend.

        Args:
            key: The key (path) of the file to download.

        Returns:
            The contents of the file as bytes.
        """
        raise NotImplementedError
