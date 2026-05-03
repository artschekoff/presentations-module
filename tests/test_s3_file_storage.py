"""
Real integration tests for S3FileStorage against Selectel S3.
Requires .env with S3_* credentials.

Run normally (objects deleted after each test):
    pytest tests/test_s3_file_storage.py -v

Keep objects in bucket for manual inspection:
    pytest tests/test_s3_file_storage.py -v --keep-s3
"""
import os
import tempfile
import uuid

import pytest
from dotenv import load_dotenv

load_dotenv()

from presentations_module.files.s3_file_storage import S3FileStorage


def make_storage() -> S3FileStorage:
    return S3FileStorage(
        bucket=os.environ["S3_BUCKET"],
        prefix=f"test-{uuid.uuid4().hex}",
        region_name=os.getenv("S3_REGION"),
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
        endpoint_url=os.getenv("S3_ENDPOINT_URL"),
        verify_ssl=os.getenv("S3_VERIFY_SSL", "true").lower() == "true",
    )


@pytest.fixture
def storage() -> S3FileStorage:
    return make_storage()


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------

async def _object_exists(storage: S3FileStorage, key: str) -> bool:
    """Return True if the key exists in the bucket."""
    try:
        async with storage._client() as s3:
            await s3.head_object(Bucket=storage.bucket, Key=key)
        return True
    except Exception:
        return False


async def _read_object(storage: S3FileStorage, key: str) -> bytes:
    async with storage._client() as s3:
        response = await s3.get_object(Bucket=storage.bucket, Key=key)
        return await response["Body"].read()


async def _maybe_delete(storage: S3FileStorage, key: str, keep: bool) -> None:
    if keep:
        print(f"\n  [kept] s3://{storage.bucket}/{key}")
        return
    async with storage._client() as s3:
        await s3.delete_object(Bucket=storage.bucket, Key=key)


# ---------------------------------------------------------------------------
# tests
# ---------------------------------------------------------------------------

class TestBuildPath:
    def test_joins_parts_with_slash(self, storage: S3FileStorage):
        assert storage.build_path("a", "b", "c") == f"{storage.prefix}/a/b/c"

    def test_strips_leading_trailing_slashes(self, storage: S3FileStorage):
        assert storage.build_path("/a/", "/b/") == f"{storage.prefix}/a/b"

    def test_filters_empty_parts(self, storage: S3FileStorage):
        assert storage.build_path("", "a", "") == f"{storage.prefix}/a"

    def test_empty_assets_dir(self):
        s = S3FileStorage(
            bucket="bucket",
            prefix="presentations",
            verify_ssl=False,
        )
        key = s.build_path("", "abc123", "file.pptx")
        assert key == "presentations/abc123/file.pptx"


class TestMakedirs:
    async def test_is_noop(self, storage: S3FileStorage):
        await storage.makedirs("any/path/here")
        await storage.makedirs("")


class TestSaveBytes:
    async def test_uploads_bytes(self, storage: S3FileStorage, keep_s3: bool):
        key = storage.build_path("save_bytes_test.bin")
        data = b"\x00\x01\x02\x03 hello s3"
        try:
            result = await storage.save_bytes(key, data)
            assert result == f"s3://{storage.bucket}/{key}"
            assert await _object_exists(storage, key)
            assert await _read_object(storage, key) == data
        finally:
            await _maybe_delete(storage, key, keep_s3)

    async def test_returns_s3_url(self, storage: S3FileStorage, keep_s3: bool):
        key = storage.build_path("url_test.bin")
        try:
            result = await storage.save_bytes(key, b"x")
            assert result.startswith("s3://")
            assert storage.bucket in result
            assert key in result
        finally:
            await _maybe_delete(storage, key, keep_s3)

    async def test_overwrites_existing(self, storage: S3FileStorage, keep_s3: bool):
        key = storage.build_path("overwrite_test.bin")
        try:
            await storage.save_bytes(key, b"first")
            await storage.save_bytes(key, b"second")
            assert await _read_object(storage, key) == b"second"
        finally:
            await _maybe_delete(storage, key, keep_s3)


class TestSaveText:
    async def test_uploads_utf8_text(self, storage: S3FileStorage, keep_s3: bool):
        key = storage.build_path("text_test.txt")
        content = "Hello, world! Hello S3."
        try:
            result = await storage.save_text(key, content)
            assert result == f"s3://{storage.bucket}/{key}"
            raw = await _read_object(storage, key)
            assert raw.decode("utf-8") == content
        finally:
            await _maybe_delete(storage, key, keep_s3)

    async def test_custom_encoding(self, storage: S3FileStorage, keep_s3: bool):
        key = storage.build_path("text_encoding_test.txt")
        content = "Hello"
        try:
            await storage.save_text(key, content, encoding="latin-1")
            raw = await _read_object(storage, key)
            assert raw.decode("latin-1") == content
        finally:
            await _maybe_delete(storage, key, keep_s3)


class TestSaveFromLocalPath:
    async def test_uploads_local_file(self, storage: S3FileStorage, keep_s3: bool):
        key = storage.build_path("local_upload_test.txt")
        payload = b"uploaded from local file"
        with tempfile.NamedTemporaryFile(delete=False, suffix=".txt") as f:
            f.write(payload)
            tmp_path = f.name
        try:
            result = await storage.save_from_local_path(key, tmp_path)
            assert result == f"s3://{storage.bucket}/{key}"
            assert await _object_exists(storage, key)
            assert await _read_object(storage, key) == payload
        finally:
            await _maybe_delete(storage, key, keep_s3)
            if os.path.exists(tmp_path):
                os.unlink(tmp_path)

    async def test_local_file_still_exists_after_upload(self, storage: S3FileStorage, keep_s3: bool):
        """S3 upload must NOT delete the source file (unlike local move)."""
        key = storage.build_path("exists_after_upload.txt")
        with tempfile.NamedTemporaryFile(delete=False, suffix=".txt") as f:
            f.write(b"keep me")
            tmp_path = f.name
        try:
            await storage.save_from_local_path(key, tmp_path)
            assert os.path.exists(tmp_path), "source file was deleted by S3 upload"
        finally:
            await _maybe_delete(storage, key, keep_s3)
            if os.path.exists(tmp_path):
                os.unlink(tmp_path)

    async def test_uploads_binary_file(self, storage: S3FileStorage, keep_s3: bool):
        key = storage.build_path("binary_upload_test.pptx")
        payload = bytes(range(256)) * 100  # 25.6 KB of binary data
        with tempfile.NamedTemporaryFile(delete=False, suffix=".pptx") as f:
            f.write(payload)
            tmp_path = f.name
        try:
            await storage.save_from_local_path(key, tmp_path)
            assert await _read_object(storage, key) == payload
        finally:
            await _maybe_delete(storage, key, keep_s3)
            if os.path.exists(tmp_path):
                os.unlink(tmp_path)
