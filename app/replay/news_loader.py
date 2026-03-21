from __future__ import annotations

import csv
import json
from datetime import datetime, timezone
from pathlib import Path

import requests

from app.events.models import NewsItem


NEWSAPI_EVERYTHING_URL = "https://newsapi.org/v2/everything"


class HistoricalNewsLoader:
    def __init__(self, newsapi_key: str = "", timeout_seconds: int = 10) -> None:
        self.newsapi_key = newsapi_key
        self.timeout_seconds = timeout_seconds
        self.session = requests.Session()

    def load(
        self,
        json_path: str | None = None,
        csv_path: str | None = None,
        from_iso: str | None = None,
        to_iso: str | None = None,
        query: str = "markets OR inflation OR fed OR oil OR earnings",
        page_size: int = 100,
    ) -> list[NewsItem]:
        if json_path:
            return self._load_json(Path(json_path))
        if csv_path:
            return self._load_csv(Path(csv_path))
        if self.newsapi_key:
            return self._load_newsapi(from_iso, to_iso, query, page_size)
        raise ValueError("no_historical_news_source_configured")

    def _load_json(self, path: Path) -> list[NewsItem]:
        payload = json.loads(path.read_text(encoding="utf-8"))
        if isinstance(payload, dict):
            payload = payload.get("items", [])
        items = [self._news_item_from_mapping(row) for row in payload]
        return sorted(items, key=lambda item: item.timestamp_utc)

    def _load_csv(self, path: Path) -> list[NewsItem]:
        items: list[NewsItem] = []
        with path.open("r", encoding="utf-8", newline="") as handle:
            reader = csv.DictReader(handle)
            for row in reader:
                items.append(self._news_item_from_mapping(row))
        return sorted(items, key=lambda item: item.timestamp_utc)

    def _load_newsapi(self, from_iso: str | None, to_iso: str | None, query: str, page_size: int) -> list[NewsItem]:
        page = 1
        all_items: list[NewsItem] = []
        while True:
            response = self.session.get(
                NEWSAPI_EVERYTHING_URL,
                params={
                    "q": query,
                    "language": "en",
                    "sortBy": "publishedAt",
                    "from": from_iso,
                    "to": to_iso,
                    "pageSize": min(page_size, 100),
                    "page": page,
                    "apiKey": self.newsapi_key,
                },
                timeout=self.timeout_seconds,
            )
            response.raise_for_status()
            payload = response.json()
            articles = payload.get("articles", [])
            if not articles:
                break
            for article in articles:
                all_items.append(
                    self._news_item_from_mapping(
                        {
                            "timestamp": article.get("publishedAt"),
                            "headline": article.get("title"),
                            "source": (article.get("source") or {}).get("name"),
                            "body": "\n".join(filter(None, [article.get("description"), article.get("content")])),
                            "url": article.get("url"),
                            "external_id": article.get("url") or article.get("title"),
                        }
                    )
                )
            if len(articles) < min(page_size, 100):
                break
            page += 1
        return sorted(all_items, key=lambda item: item.timestamp_utc)

    def _news_item_from_mapping(self, row: dict) -> NewsItem:
        timestamp_value = (
            row.get("timestamp")
            or row.get("timestamp_utc")
            or row.get("published_at")
            or row.get("publishedAt")
        )
        headline = str(row.get("headline") or row.get("title") or "").strip()
        source = str(row.get("source") or row.get("provider") or "unknown").strip()
        body = str(row.get("body") or row.get("description") or row.get("content") or "")
        url = str(row.get("url") or "")
        external_id = str(row.get("external_id") or row.get("id") or f"{source}|{headline}|{timestamp_value}")
        return NewsItem(
            external_id=external_id,
            timestamp_utc=_parse_timestamp(timestamp_value),
            source=source,
            headline=headline,
            body=body,
            url=url,
            metadata={"replay_source": "historical_loader"},
        )


def _parse_timestamp(raw: str | None) -> datetime:
    if not raw:
        return datetime.now(timezone.utc)
    parsed = datetime.fromisoformat(str(raw).replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)
