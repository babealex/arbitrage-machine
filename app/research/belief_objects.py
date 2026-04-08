from __future__ import annotations

from app.models import Market
from app.research.normalization import kalshi_market_belief


def family_from_ticker(ticker: str, families: list[str]) -> str | None:
    for family in sorted(families, key=len, reverse=True):
        if ticker.startswith(family):
            return family
    return None


def build_kalshi_belief_snapshots(markets: list[Market], families: list[str]) -> list[dict]:
    snapshots: list[dict] = []
    for market in markets:
        family = family_from_ticker(market.ticker, families)
        if family is None:
            continue
        belief = kalshi_market_belief(market, family)
        if belief is None:
            continue
        snapshots.append(belief.to_snapshot_row())
    return snapshots
