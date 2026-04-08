from __future__ import annotations

from decimal import Decimal

from app.strategies.trend.models import DailyBar


def momentum_return(bars: list[DailyBar], lookback_bars: int) -> Decimal | None:
    if len(bars) <= lookback_bars:
        return None
    start = bars[-(lookback_bars + 1)].close
    end = bars[-1].close
    if start <= 0:
        return None
    return (end / start) - Decimal("1")
