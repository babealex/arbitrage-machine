from __future__ import annotations

import hashlib
from datetime import datetime, timezone

import requests

from app.events.models import NewsItem


class NewsApiSource:
    def __init__(self, api_key: str, api_url: str, timeout_seconds: int = 8) -> None:
        self.api_key = api_key
        self.api_url = api_url
        self.timeout_seconds = timeout_seconds
        self.session = requests.Session()

    def fetch(self) -> list[NewsItem]:
        if not self.api_key:
            return []
        response = self.session.get(
            self.api_url,
            params={"category": "business", "language": "en", "pageSize": 25, "apiKey": self.api_key},
            timeout=self.timeout_seconds,
        )
        response.raise_for_status()
        payload = response.json()
        items: list[NewsItem] = []
        for article in payload.get("articles", []):
            headline = (article.get("title") or "").strip()
            if not headline:
                continue
            url = (article.get("url") or "").strip()
            published_at = _parse_timestamp(article.get("publishedAt"))
            external_id = hashlib.sha256(f"newsapi|{url}|{headline}".encode("utf-8")).hexdigest()
            items.append(
                NewsItem(
                    external_id=external_id,
                    timestamp_utc=published_at,
                    source=str((article.get("source") or {}).get("name") or "NewsAPI"),
                    headline=headline,
                    body=(article.get("description") or "") + "\n" + (article.get("content") or ""),
                    url=url,
                    metadata={"feed_type": "newsapi"},
                )
            )
        return items


def _parse_timestamp(raw: str | None) -> datetime:
    if not raw:
        return datetime.now(timezone.utc)
    try:
        parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        return parsed.astimezone(timezone.utc)
    except Exception:
        return datetime.now(timezone.utc)
