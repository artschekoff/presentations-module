from abc import ABC, abstractmethod


class FileStorage(ABC):
    """Abstract interface for file storage backends."""

    @abstractmethod
    def build_path(self, *parts: str) -> str:
        """Build a storage path or key from parts."""
        raise NotImplementedError

    @abstractmethod
    async def makedirs(self, path: str) -> None:
        """Ensure the given path/prefix exists. No-op for key-value stores."""
        raise NotImplementedError

    @abstractmethod
    async def save_bytes(self, path: str, data: bytes) -> str:
        """Save binary data and return the storage reference (path or URL)."""
        raise NotImplementedError

    @abstractmethod
    async def save_text(self, path: str, content: str, encoding: str = "utf-8") -> str:
        """Save text content and return the storage reference."""
        raise NotImplementedError

    @abstractmethod
    async def save_from_local_path(self, dest_path: str, local_path: str) -> str:
        """
        Move or upload a file from a local path to the given destination.
        Returns the storage reference. The local file may be consumed (moved).
        """
        raise NotImplementedError
