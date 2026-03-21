from __future__ import annotations

from app.events.models import NewsItem
from app.ingestion.base import NewsSource


class NewsIngestionService:
    def __init__(self, sources: list[NewsSource]) -> None:
        self.sources = sources

    def poll(self) -> list[NewsItem]:
        items: list[NewsItem] = []
        for source in self.sources:
            try:
                items.extend(source.fetch())
            except Exception:
                continue
        items.sort(key=lambda item: item.timestamp_utc)
        return items
