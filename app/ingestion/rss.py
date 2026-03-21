from __future__ import annotations

import hashlib
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime

import requests

from app.events.models import NewsItem


class RSSNewsSource:
    def __init__(self, feed_urls: list[str], timeout_seconds: int = 8) -> None:
        self.feed_urls = feed_urls
        self.timeout_seconds = timeout_seconds
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": "arbitrage-machine-event-v1/0.1"})

    def fetch(self) -> list[NewsItem]:
        items: list[NewsItem] = []
        for url in self.feed_urls:
            try:
                response = self.session.get(url, timeout=self.timeout_seconds)
                response.raise_for_status()
                items.extend(self._parse_feed(response.text, url))
            except Exception:
                continue
        return items

    def _parse_feed(self, xml_text: str, source_url: str) -> list[NewsItem]:
        root = ET.fromstring(xml_text)
        found: list[NewsItem] = []
        for item in root.findall(".//item"):
            headline = (item.findtext("title") or "").strip()
            if not headline:
                continue
            body = (item.findtext("description") or "").strip()
            link = (item.findtext("link") or "").strip()
            published_raw = (item.findtext("pubDate") or "").strip()
            published_at = _parse_published_at(published_raw)
            external_id = hashlib.sha256(f"{source_url}|{link}|{headline}".encode("utf-8")).hexdigest()
            found.append(
                NewsItem(
                    external_id=external_id,
                    timestamp_utc=published_at,
                    source=source_url,
                    headline=headline,
                    body=body,
                    url=link,
                    metadata={"feed_type": "rss"},
                )
            )
        return found


def _parse_published_at(raw: str) -> datetime:
    if not raw:
        return datetime.now(timezone.utc)
    try:
        parsed = parsedate_to_datetime(raw)
        if parsed.tzinfo is None:
            return parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc)
    except Exception:
        return datetime.now(timezone.utc)
