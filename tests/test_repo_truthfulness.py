from __future__ import annotations

from datetime import date, datetime, timezone
from decimal import Decimal

from app.config import Settings
from app.db import Database
from app.execution.router import ExecutionRouter
from app.persistence.kalshi_repo import KalshiRepository
from app.persistence.options_repo import OptionsRepository
from app.persistence.trend_repo import TrendRepository
from app.portfolio.orchestrator import PortfolioOrchestrator
from app.portfolio.runtime import OptionsCandidateEngine
from app.portfolio.state import PortfolioState
from app.risk.manager import RiskManager
from app.strategies.trend.models import TrendEquitySnapshot, TrendTarget


class DummyClient:
    data_only_mode = True

    def get_balance(self):
        return {}

    def get_positions(self):
        return []

    def list_orders(self):
        return []

    def get_fills(self):
        return []


def test_options_candidate_engine_respects_enable_flag(tmp_path) -> None:
    db = Database(tmp_path / "truth.db")
    engine = OptionsCandidateEngine(Settings(enable_options_sleeve=False), OptionsRepository(db))
    assert engine.get_candidates() == []


def test_trend_normalization_uses_current_portfolio_equity(tmp_path) -> None:
    db = Database(tmp_path / "orchestrator_truth.db")
    signal_repo = KalshiRepository(db)
    trend_repo = TrendRepository(db)
    trend_repo.persist_equity_snapshot(
        TrendEquitySnapshot(
            snapshot_date=date(2026, 1, 1),
            equity=Decimal("250000"),
            cash=Decimal("250000"),
            gross_exposure=Decimal("0"),
            net_exposure=Decimal("0"),
            drawdown=Decimal("0"),
            trade_count=0,
            positions={},
        )
    )
    router = ExecutionRouter(DummyClient(), signal_repo, PortfolioState(DummyClient()), RiskManager(0.02, 1.0, 1.0, 3), __import__("logging").getLogger("truth"), False)
    orchestrator = PortfolioOrchestrator(config=None, db=db, allocator=None, execution_router=router)
    target = TrendTarget(
        symbol="SPY",
        momentum_return=Decimal("0.05"),
        realized_volatility=Decimal("0.10"),
        target_weight=Decimal("0.20"),
    )

    candidate = orchestrator._normalize_trend_candidate(target, datetime(2026, 1, 2, tzinfo=timezone.utc))

    assert candidate.trade_candidate.notional_reference == Decimal("50000")


def test_settings_only_warn_missing_trend_broker_when_none(monkeypatch) -> None:
    monkeypatch.setenv("LIVE_TRADING", "true")
    monkeypatch.setenv("LIVE_ENABLED_SLEEVES", "trend")
    monkeypatch.setenv("TREND_LIVE_BROKER_MODE", "ibkr")
    monkeypatch.setenv("TREND_LIVE_BROKER_ENABLED", "true")
    monkeypatch.setenv("IBKR_ACCOUNT_ID", "DU123")

    settings = Settings()

    assert "trend_live_requested_without_supported_broker" not in settings.validate()
