from __future__ import annotations

from datetime import date, timedelta
from decimal import Decimal
from pathlib import Path

from app.db import Database
from app.execution.models import SignalLegIntent
from app.execution.router import ExecutionRouter
from app.instruments.models import InstrumentRef
from app.market_data.snapshots import MarketStateInput
from app.models import Signal, SignalEdge, StrategyName
from app.persistence.kalshi_repo import KalshiRepository
from app.persistence.market_state_repo import MarketStateRepository
from app.persistence.risk_repo import RiskRepository
from app.persistence.trend_repo import TrendRepository
from app.portfolio.state import PortfolioState
from app.replay_execution import ExecutionReplayEngine
from app.reporting_execution import build_execution_report
from app.risk.manager import RiskManager
from app.strategies.trend.models import DailyBar
from app.strategies.trend.portfolio import TrendPaperEngine


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


def _bars(symbol: str, start: date, closes: list[str]) -> list[DailyBar]:
    return [
        DailyBar(symbol=symbol, bar_date=start + timedelta(days=index), close=Decimal(close))
        for index, close in enumerate(closes)
    ]


def _seed_mixed_run(tmp_path: Path) -> tuple[KalshiRepository, TrendRepository]:
    db = Database(tmp_path / "replay.db")
    signal_repo = KalshiRepository(db)
    market_state_repo = MarketStateRepository(db)
    trend_repo = TrendRepository(db)
    risk = RiskManager(0.02, 1.0, 1.0, 3, event_recorder=RiskRepository(db).persist_transition)
    router = ExecutionRouter(DummyClient(), signal_repo, PortfolioState(DummyClient()), risk, __import__("logging").getLogger("replay"), False, market_state_repo=market_state_repo)

    kalshi_signal = Signal(
        strategy=StrategyName.CROSS_MARKET,
        ticker="FED-1",
        action="buy_yes",
        quantity=2,
        edge=SignalEdge.from_values(edge_before_fees=0.04, edge_after_fees=0.03, fee_estimate=0.01),
        source="cross_market_probability_map",
        confidence=0.8,
        legs=[SignalLegIntent(InstrumentRef("FED-1", contract_side="yes"), "buy", "limit", Decimal("0.40"), "IOC")],
    )
    assert router.execute_signal(kalshi_signal)

    start = date(2026, 1, 1)
    trend_repo.persist_bars(_bars("SPY", start, [str(500 + step) for step in range(25)]))
    engine = TrendPaperEngine(
        signal_repo=signal_repo,
        trend_repo=trend_repo,
        risk=risk,
        execution_router=router,
        universe=["SPY"],
        starting_capital=Decimal("100000"),
    )
    result = engine.run(start + timedelta(days=24))
    assert result.readiness.can_trade
    return signal_repo, trend_repo


def _seed_trend_only_run(tmp_path: Path) -> tuple[KalshiRepository, TrendRepository]:
    db = Database(tmp_path / "trend_only.db")
    signal_repo = KalshiRepository(db)
    market_state_repo = MarketStateRepository(db)
    trend_repo = TrendRepository(db)
    risk = RiskManager(0.02, 1.0, 1.0, 3)
    router = ExecutionRouter(DummyClient(), signal_repo, PortfolioState(DummyClient()), risk, __import__("logging").getLogger("replay-trend"), False, market_state_repo=market_state_repo)
    start = date(2026, 1, 1)
    trend_repo.persist_bars(_bars("SPY", start, [str(500 + step) for step in range(25)]))
    engine = TrendPaperEngine(
        signal_repo=signal_repo,
        trend_repo=trend_repo,
        risk=risk,
        execution_router=router,
        universe=["SPY"],
        starting_capital=Decimal("100000"),
    )
    result = engine.run(start + timedelta(days=24))
    assert result.readiness.can_trade
    return signal_repo, trend_repo


def test_replay_reconstructs_mixed_run_deterministically(tmp_path: Path) -> None:
    signal_repo, _trend_repo = _seed_mixed_run(tmp_path)
    replay_engine = ExecutionReplayEngine()

    first = replay_engine.replay(
        signals=signal_repo.load_signals(),
        order_intents=signal_repo.load_order_intents(),
        execution_outcomes=signal_repo.load_execution_outcomes(),
        market_state_inputs=MarketStateRepository(signal_repo.db).load_market_state_inputs(),
        starting_cash=Decimal("100000"),
    )
    second = replay_engine.replay(
        signals=signal_repo.load_signals(),
        order_intents=signal_repo.load_order_intents(),
        execution_outcomes=signal_repo.load_execution_outcomes(),
        market_state_inputs=MarketStateRepository(signal_repo.db).load_market_state_inputs(),
        starting_cash=Decimal("100000"),
    )

    assert first.snapshot.cash == second.snapshot.cash
    assert first.snapshot.equity == second.snapshot.equity
    assert first.snapshot.positions.keys() == second.snapshot.positions.keys()


def test_replay_pnl_matches_trend_snapshot(tmp_path: Path) -> None:
    signal_repo, trend_repo = _seed_trend_only_run(tmp_path)
    replay_engine = ExecutionReplayEngine()

    replay = replay_engine.replay(
        signals=signal_repo.load_signals(),
        order_intents=signal_repo.load_order_intents(),
        execution_outcomes=signal_repo.load_execution_outcomes(),
        market_state_inputs=MarketStateRepository(signal_repo.db).load_market_state_inputs(),
        starting_cash=Decimal("100000"),
    )
    trend_snapshot = trend_repo.load_equity_snapshots()[-1]

    assert replay.snapshot.cash == trend_snapshot.cash
    assert replay.snapshot.equity == trend_snapshot.equity


def test_execution_report_uses_outcomes_for_fill_and_slippage(tmp_path: Path) -> None:
    signal_repo, _trend_repo = _seed_mixed_run(tmp_path)
    report = build_execution_report(
        signals=signal_repo.load_signals(),
        order_intents=signal_repo.load_order_intents(),
        execution_outcomes=signal_repo.load_execution_outcomes(),
        market_state_inputs=MarketStateRepository(signal_repo.db).load_market_state_inputs(),
    )

    assert report.summary["orders"] >= 2
    assert "cross_market" in report.by_strategy
    assert "momentum" in report.by_strategy
    assert report.by_strategy["momentum"]["fill_rate"] >= 1.0


def test_replay_prefers_persisted_market_state_over_metadata(tmp_path: Path) -> None:
    signal_repo, _trend_repo = _seed_trend_only_run(tmp_path)
    market_state_repo = MarketStateRepository(signal_repo.db)
    existing = market_state_repo.load_market_state_inputs()
    instrument = existing[-1].instrument
    market_state_repo.persist_market_state_input(
        MarketStateInput(
            observed_at=existing[-1].observed_at + timedelta(days=1),
            instrument=instrument,
            source="test_mark",
            reference_kind="mark",
            reference_price=Decimal("999.99"),
            provenance="test",
            best_bid=Decimal("999.99"),
            best_ask=Decimal("1000.01"),
        )
    )
    replay = ExecutionReplayEngine().replay(
        signals=signal_repo.load_signals(),
        order_intents=signal_repo.load_order_intents(),
        execution_outcomes=signal_repo.load_execution_outcomes(),
        market_state_inputs=market_state_repo.load_market_state_inputs(),
        starting_cash=Decimal("100000"),
    )
    assert replay.snapshot.equity > Decimal("100000")
