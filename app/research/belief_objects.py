from __future__ import annotations

from datetime import datetime, timezone

from app.models import Market
from app.strategies.cross_market_snapshot import midpoint_probability


def family_from_ticker(ticker: str, families: list[str]) -> str | None:
    for family in sorted(families, key=len, reverse=True):
        if ticker.startswith(family):
            return family
    return None


def build_kalshi_belief_snapshots(markets: list[Market], families: list[str]) -> list[dict]:
    timestamp = datetime.now(timezone.utc).isoformat()
    snapshots: list[dict] = []
    for market in markets:
        family = family_from_ticker(market.ticker, families)
        if family is None:
            continue
        probability = midpoint_probability(market)
        if probability is None:
            continue
        quote_diag = dict(market.metadata.get("quote_diagnostics", {}))
        snapshots.append(
            {
                "timestamp_utc": timestamp,
                "family": family,
                "event_ticker": market.event_ticker,
                "market_ticker": market.ticker,
                "source_type": "kalshi",
                "object_type": "mean_belief",
                "value": probability,
                "bid": market.yes_bid,
                "ask": market.yes_ask,
                "size_bid": int(quote_diag.get("best_yes_bid_size") or 0),
                "size_ask": int(quote_diag.get("best_yes_ask_size") or 0),
                "metadata_json": {
                    "minutes_to_event": minutes_to_event(market.expiry),
                    "session_hour_bucket": hour_bucket(),
                    "quote_context_verified": bool(quote_diag.get("orderbook_snapshot_exists")),
                },
            }
        )
    return snapshots


def minutes_to_event(expiry: datetime | None) -> int | None:
    if expiry is None:
        return None
    now = datetime.now(timezone.utc)
    delta = expiry - now
    return max(0, int(delta.total_seconds() // 60))


def hour_bucket(ts: datetime | None = None) -> str:
    current = ts or datetime.now(timezone.utc)
    return f"{current.hour:02d}:00"
