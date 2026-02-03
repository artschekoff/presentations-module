from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import datetime
from typing import Any


@dataclass
class PresentationDocument:
    topic: str
    language: str
    slides_amount: int
    audience: str
    author: str | None
    status: str = "pending"
    files: list[str] = field(default_factory=list)
    created_at: datetime = field(default_factory=datetime.utcnow)

    def payload(self) -> dict[str, Any]:
        return asdict(self)
