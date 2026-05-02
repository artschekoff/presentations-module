"""Tests for LocalFileStorage with base_dir."""
from __future__ import annotations

import os
import pytest
from presentations_module.files.local_file_storage import LocalFileStorage


@pytest.mark.asyncio
async def test_save_bytes_no_basedir(tmp_path):
    storage = LocalFileStorage()
    dest = str(tmp_path / "file.txt")
    result = await storage.save_bytes(dest, b"hello")
    assert result == dest
    assert open(dest, "rb").read() == b"hello"


@pytest.mark.asyncio
async def test_save_bytes_with_basedir(tmp_path):
    base = str(tmp_path / "base")
    storage = LocalFileStorage(base_dir=base)
    result = await storage.save_bytes("sub/file.txt", b"world")
    expected = os.path.join(base, "sub", "file.txt")
    assert result == expected
    assert open(expected, "rb").read() == b"world"


@pytest.mark.asyncio
async def test_save_from_local_path_with_basedir(tmp_path):
    base = str(tmp_path / "dest_base")
    src = tmp_path / "src.zip"
    src.write_bytes(b"zipdata")
    storage = LocalFileStorage(base_dir=base)
    result = await storage.save_from_local_path("uploads/src.zip", str(src))
    expected = os.path.join(base, "uploads", "src.zip")
    assert result == expected
    assert open(expected, "rb").read() == b"zipdata"


def test_build_path_no_basedir():
    storage = LocalFileStorage()
    assert storage.build_path("a", "b", "c") == os.path.join("a", "b", "c")


def test_build_path_with_basedir(tmp_path):
    base = str(tmp_path)
    storage = LocalFileStorage(base_dir=base)
    assert storage.build_path("a", "b") == os.path.join(base, "a", "b")


@pytest.mark.asyncio
async def test_makedirs_with_basedir(tmp_path):
    base = str(tmp_path / "root")
    storage = LocalFileStorage(base_dir=base)
    await storage.makedirs("deep/nested")
    assert os.path.isdir(os.path.join(base, "deep", "nested"))
