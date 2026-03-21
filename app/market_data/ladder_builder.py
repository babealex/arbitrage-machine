from __future__ import annotations

import re
from collections import defaultdict

from app.models import Market


STRIKE_PATTERN = re.compile(r"(-?\d+(?:\.\d+)?)")


def infer_strike(market: Market) -> float | None:
    for text in (market.subtitle, market.title, market.ticker):
        match = STRIKE_PATTERN.search(text or "")
        if match:
            return float(match.group(1))
    return None


def build_ladders(markets: list[Market]) -> dict[str, list[Market]]:
    ladders: dict[str, list[Market]] = defaultdict(list)
    for market in markets:
        market.strike = market.strike if market.strike is not None else infer_strike(market)
        ladder_key = market.metadata.get("ladder_key") or market.event_ticker
        ladders[ladder_key].append(market)
    for ladder in ladders.values():
        ladder.sort(key=lambda item: (item.strike is None, item.strike))
    return dict(ladders)
