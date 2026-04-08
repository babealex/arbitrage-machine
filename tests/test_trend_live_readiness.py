from __future__ import annotations

import logging
from pathlib import Path

from app.brokers.ibkr_trend_adapter import IbkrTrendBrokerAdapter, evaluate_trend_live_readiness
from app.db import Database
from app.persistence.kalshi_repo import KalshiRepository
from app.persistence.trend_broker_repo import TrendBrokerRepository
from app.persistence.trend_repo import TrendRepository
from tests.test_ibkr_trend_adapter import FakeIbkrClient, _config


def test_trend_live_readiness_false_when_account_snapshot_fails(tmp_path: Path) -> None:
    db = Database(tmp_path / "readiness_account.db")
    client = FakeIbkrClient()
    client.fail_account = True
    adapter = IbkrTrendBrokerAdapter(
        config=_config(),
        logger=logging.getLogger("ibkr"),
        trend_repo=TrendRepository(db),
        signal_repo=KalshiRepository(db),
        broker_repo=TrendBrokerRepository(db),
        client=client,
    )

    readiness = evaluate_trend_live_readiness(adapter)

    assert readiness.is_ready is False
    assert "account_unavailable" in readiness.reasons


def test_manual_mode_is_never_live_capable(tmp_path: Path) -> None:
    db = Database(tmp_path / "manual_mode.db")
    adapter = IbkrTrendBrokerAdapter(
        config=_config(trend_live_broker_mode="manual", trend_live_broker_enabled=False),
        logger=logging.getLogger("ibkr"),
        trend_repo=TrendRepository(db),
        signal_repo=KalshiRepository(db),
        broker_repo=TrendBrokerRepository(db),
        client=FakeIbkrClient(),
    )

    readiness = evaluate_trend_live_readiness(adapter)

    assert readiness.is_ready is False
    assert "trend_broker_mode_not_ibkr" in readiness.reasons


def test_reconnect_failure_marks_readiness_false(tmp_path: Path) -> None:
    db = Database(tmp_path / "reconnect.db")
    client = FakeIbkrClient()
    client.fail_connect = True
    adapter = IbkrTrendBrokerAdapter(
        config=_config(),
        logger=logging.getLogger("ibkr"),
        trend_repo=TrendRepository(db),
        signal_repo=KalshiRepository(db),
        broker_repo=TrendBrokerRepository(db),
        client=client,
    )

    readiness = evaluate_trend_live_readiness(adapter)

    assert readiness.is_ready is False
    assert TrendBrokerRepository(db).count_reconnect_events() >= 1
