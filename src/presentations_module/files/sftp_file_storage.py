"""Async SFTP storage using Paramiko in a worker thread per operation."""

from __future__ import annotations

import asyncio
import os
import posixpath
from typing import Any
from urllib.parse import unquote, urlparse

from .file_storage import FileStorage

try:
    import paramiko
except ImportError as e:
    raise ImportError(
        "paramiko is required for SftpFileStorage. Install it with: pip install paramiko"
    ) from e


def _mkdir_p(sftp: "paramiko.SFTPClient", remote_path: str) -> None:
    """Create parent directories for a remote file path."""
    parent = posixpath.dirname(remote_path)
    if parent in ("", "/"):
        return
    cur = "/"
    for part in parent.strip("/").split("/"):
        cur = posixpath.join(cur, part) if cur != "/" else f"/{part}"
        try:
            sftp.stat(cur)
        except OSError:
            sftp.mkdir(cur)


class SftpFileStorage(FileStorage):
    """Store files on a remote path via SFTP (SSH + Paramiko)."""

    def __init__(
        self,
        host: str,
        port: int = 22,
        username: str = "",
        password: str | None = None,
        private_key_path: str | None = None,
        base_path: str = "",
        known_hosts_path: str | None = None,
    ) -> None:
        self._host = host.strip()
        self._port = port
        self._username = username
        self._password = password
        self._private_key_path = private_key_path
        self._base = base_path.rstrip("/")
        if self._base and not self._base.startswith("/"):
            self._base = "/" + self._base
        self._known_hosts = known_hosts_path

    @property
    def host(self) -> str:
        return self._host

    @staticmethod
    def parse_sftp_uri(uri: str) -> tuple[str, str]:
        """
        Return (host, remote_path) for
        sftp://hostname/absolute/path/to/file
        or sftp://user@hostname/absolute/...
        """
        parsed = urlparse(uri)
        if parsed.scheme != "sftp":
            msg = f"Not an sftp URI: {uri!r}"
            raise ValueError(msg)
        net = parsed.netloc
        if "@" in net:
            _user, host = net.rsplit("@", 1)
        else:
            host = net
        path = unquote(parsed.path) or "/"
        return host, path

    def _connect(self) -> paramiko.SFTPClient:
        client = paramiko.SSHClient()
        if self._known_hosts and os.path.isfile(self._known_hosts):
            client.load_host_keys(self._known_hosts)
        else:
            client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        key_arg: str | None = self._private_key_path if self._private_key_path and os.path.isfile(self._private_key_path) else None
        client.connect(
            self._host,
            port=self._port,
            username=self._username,
            password=self._password,
            key_filename=key_arg,
            look_for_keys=False,
            allow_agent=False,
        )
        sftp: paramiko.SFTPClient = client.open_sftp()
        sftp.ssh = client  # type: ignore[attr-defined]
        return sftp

    @staticmethod
    def _close(sftp: paramiko.SFTPClient) -> None:
        try:
            sftp.close()
        finally:
            client = getattr(sftp, "ssh", None)  # type: ignore[union-attr]
            if client is not None:
                try:
                    client.close()
                except (OSError, TypeError, AttributeError, RuntimeError):
                    pass

    def _abs_remote(self, key: str) -> str:
        rel = key.strip().lstrip("/")
        if not self._base:
            if not rel:
                return "/"
            return f"/{rel}" if not rel.startswith("/") else rel
        if not rel:
            return self._base
        combined = f"{self._base.rstrip('/')}/{rel}"
        return combined

    def build_path(self, *parts: str) -> str:
        segments: list[str] = []
        for p in parts:
            if p:
                for seg in p.replace("\\", "/").split("/"):
                    if seg:
                        segments.append(seg)
        return "/".join(segments)

    async def makedirs(self, path: str) -> None:
        if not path:
            return

        def _sync() -> None:
            full = self._abs_remote(path).rstrip("/")
            sftp = self._connect()
            try:
                cur = "/"
                for part in full.strip("/").split("/"):
                    cur = f"{cur}/{part}" if cur != "/" else f"/{part}"
                    try:
                        sftp.stat(cur)
                    except OSError:
                        sftp.mkdir(cur)
            finally:
                self._close(sftp)

        await asyncio.to_thread(_sync)

    async def save_bytes(self, path: str, data: bytes) -> str:
        full = self._abs_remote(path)

        def _sync() -> str:
            sftp = self._connect()
            try:
                _mkdir_p(sftp, full)
                with sftp.open(full, "wb") as remote_f:  # type: ignore[operator]
                    remote_f.write(data)
            finally:
                self._close(sftp)
            return f"sftp://{self._host}{full}"

        return await asyncio.to_thread(_sync)

    async def save_text(
        self, path: str, content: str, encoding: str = "utf-8"
    ) -> str:
        return await self.save_bytes(path, content.encode(encoding))

    async def save_from_local_path(self, dest_path: str, local_path: str) -> str:
        full = self._abs_remote(dest_path)

        def _sync() -> str:
            sftp = self._connect()
            try:
                _mkdir_p(sftp, full)
                sftp.put(local_path, full)
            finally:
                self._close(sftp)
            return f"sftp://{self._host}{full}"

        return await asyncio.to_thread(_sync)

    def sftp_path_from_uri(self, uri: str) -> str:
        host, rpath = self.parse_sftp_uri(uri)
        if host != self._host:
            msg = f"SFTP URI host {host!r} does not match storage host {self._host!r}"
            raise ValueError(msg)
        return rpath

    def get_client_for_download(self) -> paramiko.SFTPClient:
        """Sync SFTP client; caller must close (see _close)."""
        return self._connect()
