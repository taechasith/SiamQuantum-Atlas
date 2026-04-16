from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any


@dataclass(slots=True)
class RawMediaRecord:
    adapter: str
    platform: str
    media_type: str
    title: str
    description: str | None
    full_text: str | None
    url: str
    canonical_url: str
    published_at: datetime | None
    language_detected: str | None
    domain: str | None
    raw_payload: dict[str, Any] = field(default_factory=dict)
    views: float | None = None
    likes: float | None = None
    comments: float | None = None
    shares: float | None = None
    rank_proxy: float | None = None
    popularity_proxy: float | None = None


class SourceAdapter(ABC):
    name: str

    @abstractmethod
    def fetch(self, **kwargs: Any) -> list[RawMediaRecord]:
        raise NotImplementedError
