from __future__ import annotations

from typing import Protocol

from app.events.models import NewsItem


class NewsSource(Protocol):
    def fetch(self) -> list[NewsItem]:
        ...

