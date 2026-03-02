import abc

from .download_format import DownloadFormat


class PresentationSource(abc.ABC):
    @abc.abstractmethod
    async def authenticate(
        self,
        login: str,
        password: str,
    ):
        """Authenticate with the presentation source."""

    @abc.abstractmethod
    async def generate_presentation(
        self,
        topic: str,
        language: str,
        slides_amount: int,
        grade: str,
        subject: str,
        author: str | None = None,
        style_id: str | None = None,
        formats_to_download: list[DownloadFormat] | None = None,
    ):
        """Retrieve a list of presentations."""
