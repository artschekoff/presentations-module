import abc
from typing import AsyncIterator

from .download_format import DownloadFormat
from ..core.progress_payload import ProgressPayload


class PresentationSource(abc.ABC):
    def __init__(self, generation_dir: str) -> None:
        self.generation_dir = generation_dir

    @abc.abstractmethod
    async def authenticate(
        self,
        login: str,
        password: str,
        generation_id: str,
    ) -> None:
        """Authenticate with the presentation source."""

    @abc.abstractmethod
    async def generate_presentation(
        self,
        generation_id: str,
        topic: str,
        language: str,
        slides_amount: int,
        grade: str,
        subject: str,
        author: str | None = None,
        style_id: str | None = None,
        formats_to_download: list[DownloadFormat] | None = None,
    ) -> AsyncIterator[ProgressPayload]:
        """Generate a presentation and stream progress updates."""
