import os
import shutil

from .file_storage import FileStorage


class LocalFileStorage(FileStorage):
    """Stores files on the local filesystem, optionally rooted at base_dir."""

    def __init__(self, base_dir: str = "") -> None:
        self._base = os.path.abspath(base_dir) if base_dir else ""

    def _abs(self, path: str) -> str:
        if not self._base:
            return path
        rel = path.lstrip("/\\")
        return os.path.join(self._base, rel) if rel else self._base

    def build_path(self, *parts: str) -> str:
        joined = os.path.join(*parts) if parts else ""
        return self._abs(joined)

    async def makedirs(self, path: str) -> None:
        os.makedirs(self._abs(path), exist_ok=True)

    async def save_bytes(self, path: str, data: bytes) -> str:
        dest = self._abs(path)
        dest_dir = os.path.dirname(dest)
        if dest_dir:
            os.makedirs(dest_dir, exist_ok=True)
        with open(dest, "wb") as f:
            f.write(data)
        return dest

    async def save_text(self, path: str, content: str, encoding: str = "utf-8") -> str:
        dest = self._abs(path)
        dest_dir = os.path.dirname(dest)
        if dest_dir:
            os.makedirs(dest_dir, exist_ok=True)
        with open(dest, "w", encoding=encoding) as f:
            f.write(content)
        return dest

    async def save_from_local_path(self, dest_path: str, local_path: str) -> str:
        dest = self._abs(dest_path)
        dest_dir = os.path.dirname(dest)
        if dest_dir:
            os.makedirs(dest_dir, exist_ok=True)
        if local_path != dest:
            shutil.move(local_path, dest)
        return dest
