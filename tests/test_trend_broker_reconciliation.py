from __future__ import annotations

import logging
from datetime import date, timedelta
from decimal import Decimal
from pathlib import Path

from app.brokers.ibkr_trend_adapter import IbkrTrendBrokerAdapter
from app.db import Database
from app.persistence.kalshi_repo import KalshiRepository
from app.persistence.trend_broker_repo import TrendBrokerRepository
from app.persistence.trend_repo import TrendRepository
from app.strategies.trend.models import DailyBar, TrendPosition
from tests.test_ibkr_trend_adapter import FakeIbkrClient, _config


def _bars(symbol: str, start: date, closes: list[str]) -> list[DailyBar]:
    return [
        DailyBar(symbol=symbol, bar_date=start + timedelta(days=index), close=Decimal(close))
        for index, close in enumerate(closes)
    ]


def test_matching_local_and_broker_state_passes_reconciliation(tmp_path: Path) -> None:
    db = Database(tmp_path / "reconcile_ok.db")
    trend_repo = TrendRepository(db)
    trend_repo.replace_positions(
        [TrendPosition(symbol="SPY", quantity=0, average_price_cents=50000, market_price_cents=50000, market_value=Decimal("0"))]
    )
    adapter = IbkrTrendBrokerAdapter(
        config=_config(),
        logger=logging.getLogger("ibkr"),
        trend_repo=trend_repo,
        signal_repo=KalshiRepository(db),
        broker_repo=TrendBrokerRepository(db),
        client=FakeIbkrClient(),
    )

    result = adapter.reconcile()

    assert result.is_consistent is True


def test_position_mismatch_fails_reconciliation(tmp_path: Path) -> None:
    db = Database(tmp_path / "reconcile_pos.db")
    trend_repo = TrendRepository(db)
    trend_repo.replace_positions(
        [TrendPosition(symbol="SPY", quantity=10, average_price_cents=50000, market_price_cents=50000, market_value=Decimal("5000"))]
    )
    adapter = IbkrTrendBrokerAdapter(
        config=_config(),
        logger=logging.getLogger("ibkr"),
        trend_repo=trend_repo,
        signal_repo=KalshiRepository(db),
        broker_repo=TrendBrokerRepository(db),
        client=FakeIbkrClient(),
    )

    result = adapter.reconcile()

    assert result.is_consistent is False
    assert "position_mismatch" in result.reasons
