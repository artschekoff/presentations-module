import os
import shutil

from .file_storage import FileStorage


class LocalFileStorage(FileStorage):
    """Stores files on the local filesystem."""

    def build_path(self, *parts: str) -> str:
        return os.path.join(*parts)

    async def makedirs(self, path: str) -> None:
        os.makedirs(path, exist_ok=True)

    async def save_bytes(self, path: str, data: bytes) -> str:
        with open(path, "wb") as f:
            f.write(data)
        return path

    async def save_text(self, path: str, content: str, encoding: str = "utf-8") -> str:
        with open(path, "w", encoding=encoding) as f:
            f.write(content)
        return path

    async def save_from_local_path(self, dest_path: str, local_path: str) -> str:
        dest_dir = os.path.dirname(dest_path)
        if dest_dir:
            await self.makedirs(dest_dir)
        if local_path != dest_path:
            shutil.move(local_path, dest_path)
        return dest_path
