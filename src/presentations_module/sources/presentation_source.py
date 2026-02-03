import abc


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
        audience: str,
        author: str | None = None,
        style_id: str | None = None,
    ):
        """Retrieve a list of presentations."""
