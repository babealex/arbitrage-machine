from __future__ import annotations

from app.events.models import ClassifiedEvent, NewsItem, TradeIdea


def news_event_payload(item: NewsItem) -> dict:
    return {
        "external_id": item.external_id,
        "timestamp_utc": item.timestamp_utc.isoformat(),
        "source": item.source,
        "headline": item.headline,
        "url": item.url,
    }


def classified_event_payload(event: ClassifiedEvent) -> dict:
    return {
        "event_type": event.event_type,
        "region": event.region,
        "severity": event.severity,
        "directional_bias": event.directional_bias,
        "assets_affected": event.assets_affected,
        "tags": event.tags,
        "rationale": event.rationale,
    }


def trade_idea_payload(idea: TradeIdea) -> dict:
    return {
        "symbol": idea.symbol,
        "side": idea.side,
        "confidence": idea.confidence,
        "event_type": idea.event_type,
        "thesis": idea.thesis,
        "confirmation_needed": idea.confirmation_needed,
    }
