from contextlib import AbstractAsyncContextManager
from typing import Any
from urllib.parse import urlparse

from .file_storage import FileStorage

try:
    import aioboto3
except ImportError as e:
    raise ImportError(
        "aioboto3 is required for S3FileStorage. Install it with: pip install aioboto3"
    ) from e


class S3FileStorage(FileStorage):
    """Stores files in Amazon S3 using aioboto3."""

    def __init__(
        self,
        bucket: str,
        prefix: str = "",
        region_name: str | None = None,
        aws_access_key_id: str | None = None,
        aws_secret_access_key: str | None = None,
        endpoint_url: str | None = None,
        verify_ssl: bool = True,
    ) -> None:
        self.bucket = bucket
        self.prefix = prefix.strip("/")
        self._region_name = region_name
        self._aws_access_key_id = aws_access_key_id
        self._aws_secret_access_key = aws_secret_access_key
        self._session = aioboto3.Session(
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            region_name=region_name,
        )
        self._endpoint_url = endpoint_url
        self._verify_ssl = verify_ssl

    def _client(self) -> AbstractAsyncContextManager[Any]:
        return self._session.client(  # type: ignore[return-value]
            "s3", endpoint_url=self._endpoint_url, verify=self._verify_ssl
        )

    def build_path(self, *parts: str) -> str:
        segments = [p.strip("/") for p in parts if p]
        if self.prefix:
            segments = [self.prefix, *segments]
        return "/".join(segments)

    async def makedirs(self, path: str) -> None:
        pass  # S3 has no directories

    async def save_bytes(self, path: str, data: bytes) -> str:
        async with self._client() as s3:
            await s3.put_object(Bucket=self.bucket, Key=path, Body=data)
        return f"s3://{self.bucket}/{path}"

    async def save_text(self, path: str, content: str, encoding: str = "utf-8") -> str:
        return await self.save_bytes(path, content.encode(encoding))

    async def save_from_local_path(self, dest_path: str, local_path: str) -> str:
        async with self._client() as s3:
            await s3.upload_file(local_path, self.bucket, dest_path)
        return f"s3://{self.bucket}/{dest_path}"

    def s3_presigned_redirect(self, s3_uri: str, expires_in: int = 3600) -> str:
        """Generate and return a presigned S3 URL."""
        import boto3

        parsed = urlparse(s3_uri)
        bucket = parsed.netloc
        key = parsed.path.lstrip("/")

        s3_client = boto3.client(
            "s3",
            endpoint_url=self._endpoint_url,
            aws_access_key_id=self._aws_access_key_id,
            aws_secret_access_key=self._aws_secret_access_key,
            region_name=self._region_name,
            verify=self._verify_ssl,
        )
        return s3_client.generate_presigned_url(
            "get_object",
            Params={"Bucket": bucket, "Key": key},
            ExpiresIn=expires_in,
        )
