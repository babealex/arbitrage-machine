from __future__ import annotations

from app.models import Market
from app.strategies.cross_market_snapshot import (
    CrossMarketSnapshotEngine,
    compute_disagreement_score,
    midpoint_probability,
)
from app.market_data.macro_fetcher import MacroPrices


def test_midpoint_probability_uses_yes_quotes() -> None:
    market = Market(
        ticker="KXECONSTATCPI-26MAR-T1.0",
        event_ticker="KXECONSTATCPI-26MAR",
        title="CPI month-over-month in Mar 2026?",
        subtitle="",
        status="open",
        yes_bid=20,
        yes_ask=30,
    )
    assert midpoint_probability(market) == 0.25


def test_compute_disagreement_score_is_absolute_distance() -> None:
    assert compute_disagreement_score(0.2, 0.8) == 0.6


def test_cross_market_snapshot_engine_builds_snapshots() -> None:
    engine = CrossMarketSnapshotEngine(["KXECONSTATCPI"])
    market = Market(
        ticker="KXECONSTATCPI-26MAR-T1.0",
        event_ticker="KXECONSTATCPI-26MAR",
        title="CPI month-over-month in Mar 2026?",
        subtitle="",
        status="open",
        yes_bid=20,
        yes_ask=30,
        metadata={"quote_diagnostics": {"best_yes_bid_size": 120, "best_yes_ask_size": 140}},
    )
    prices = MacroPrices(spy_price=500.0, qqq_price=400.0, tlt_price=90.0, btc_price=70000.0)
    snapshots, stats = engine.build_snapshots([market], prices)
    assert len(snapshots) == 1
    assert stats.snapshots_collected == 1
    assert snapshots[0]["event_ticker"] == "KXECONSTATCPI-26MAR"
    assert 0.0 <= snapshots[0]["derived_risk_score"] <= 1.0
