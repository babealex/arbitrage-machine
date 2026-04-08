from __future__ import annotations

from datetime import date, timedelta
from decimal import Decimal
from pathlib import Path

from app.db import Database
from app.execution.equity_broker import EquityExecutionBroker
from app.execution.models import SignalLegIntent
from app.persistence.kalshi_repo import KalshiRepository
from app.persistence.reporting_repo import ReportingRepository
from app.persistence.risk_repo import RiskRepository
from app.persistence.trend_repo import TrendRepository
from app.execution.router import ExecutionRouter
from app.instruments.models import InstrumentRef
from app.models import Signal, SignalEdge, StrategyName
from app.portfolio.state import PortfolioState
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


class FakeLiveTrendBroker(EquityExecutionBroker):
    def is_live_capable(self) -> bool:
        return True

    def execute_order(self, intent, *, source_metadata, market_state):
        from app.execution.models import ExecutionOutcome

        return ExecutionOutcome(
            client_order_id=intent.order_id,
            instrument=intent.instrument,
            side=intent.side,
            action=intent.order_type,
            status="submitted",
            requested_quantity=int(intent.quantity),
            filled_quantity=0,
            price_cents=50000,
            time_in_force=intent.time_in_force,
            is_flatten=False,
            created_at=intent.created_at,
            metadata=source_metadata,
        )


def _bars(symbol: str, start: date, closes: list[str]) -> list[DailyBar]:
    return [
        DailyBar(symbol=symbol, bar_date=start + timedelta(days=index), close=Decimal(close))
        for index, close in enumerate(closes)
    ]


def test_trend_signal_execution_and_persistence_round_trip(tmp_path: Path) -> None:
    db = Database(tmp_path / "trend.db")
    signal_repo = KalshiRepository(db)
    reporting_repo = ReportingRepository(db)
    trend_repo = TrendRepository(db)
    risk = RiskManager(0.02, 1.0, 1.0, 3, event_recorder=RiskRepository(db).persist_transition)
    router = ExecutionRouter(DummyClient(), signal_repo, PortfolioState(DummyClient()), risk, __import__("logging").getLogger("trend"), False)
    start = date(2026, 1, 1)
    trend_repo.persist_bars(
        _bars("SPY", start, [str(500 + step) for step in range(25)])
        + _bars("QQQ", start, [str(400 + (step // 2)) for step in range(25)])
        + _bars("TLT", start, [str(100 - step) for step in range(25)])
    )
    engine = TrendPaperEngine(
        signal_repo=signal_repo,
        trend_repo=trend_repo,
        risk=risk,
        execution_router=router,
        universe=["SPY", "QQQ", "TLT"],
        starting_capital=Decimal("100000"),
    )

    result = engine.run(start + timedelta(days=24))

    loaded_signals = signal_repo.load_signals()
    loaded_outcomes = signal_repo.load_execution_outcomes()
    loaded_orders = reporting_repo.load_kalshi_reporting_bundle().orders
    loaded_positions = trend_repo.load_positions()
    loaded_snapshots = trend_repo.load_equity_snapshots()

    assert result.readiness.can_trade
    assert len(result.signals) >= 1
    assert any(signal.strategy.value == "momentum" for signal in loaded_signals)
    assert any(outcome.instrument.venue == "paper_equity" for outcome in loaded_outcomes)
    assert len(loaded_orders) == len(loaded_outcomes)
    assert loaded_positions
    assert loaded_snapshots[-1].trade_count == len(loaded_outcomes)


def test_signal_and_snapshot_decimal_boundaries_are_exact(tmp_path: Path) -> None:
    db = Database(tmp_path / "precision.db")
    signal_repo = KalshiRepository(db)
    trend_repo = TrendRepository(db)
    risk = RiskManager(0.02, 1.0, 1.0, 3)
    router = ExecutionRouter(DummyClient(), signal_repo, PortfolioState(DummyClient()), risk, __import__("logging").getLogger("trend"), False)
    start = date(2026, 1, 1)
    trend_repo.persist_bars(_bars("SPY", start, [str(500 + step) for step in range(25)]))
    engine = TrendPaperEngine(
        signal_repo=signal_repo,
        trend_repo=trend_repo,
        risk=risk,
        execution_router=router,
        universe=["SPY"],
        starting_capital=Decimal("100000.125"),
    )

    engine.run(start + timedelta(days=24))

    signal_row = db.fetchone("SELECT probability_edge_decimal FROM signals")
    snapshot_row = db.fetchone("SELECT equity_decimal, cash_decimal FROM trend_equity_snapshots")
    assert signal_row["probability_edge_decimal"] is not None
    assert Decimal(str(signal_row["probability_edge_decimal"])) == signal_repo.load_signals()[0].edge.edge_after_fees
    assert Decimal(str(snapshot_row["cash_decimal"])) == trend_repo.load_equity_snapshots()[0].cash


def test_gentle_score_weight_mode_reduces_equal_cap_saturation(tmp_path: Path) -> None:
    db = Database(tmp_path / "gentle_score.db")
    signal_repo = KalshiRepository(db)
    trend_repo = TrendRepository(db)
    risk = RiskManager(0.02, 1.0, 1.0, 3)
    router = ExecutionRouter(DummyClient(), signal_repo, PortfolioState(DummyClient()), risk, __import__("logging").getLogger("gentle_score"), False)
    start = date(2026, 1, 1)
    trend_repo.persist_bars(
        _bars("SPY", start, [str(500 + step) for step in range(25)])
        + _bars("QQQ", start, [str(400 + (step * 2)) for step in range(25)])
        + _bars("IWM", start, [str(200 + step) for step in range(25)])
        + _bars("EFA", start, [str(80 + (step / 2)) for step in range(25)])
        + _bars("EEM", start, [str(40 + (step / 3)) for step in range(25)])
        + _bars("TLT", start, [str(100 - step) for step in range(25)])
    )
    current_engine = TrendPaperEngine(
        signal_repo=signal_repo,
        trend_repo=trend_repo,
        risk=risk,
        execution_router=router,
        universe=["SPY", "QQQ", "IWM", "EFA", "EEM", "TLT"],
        weight_mode="current",
        starting_capital=Decimal("100000"),
    )
    gentle_engine = TrendPaperEngine(
        signal_repo=signal_repo,
        trend_repo=trend_repo,
        risk=risk,
        execution_router=router,
        universe=["SPY", "QQQ", "IWM", "EFA", "EEM", "TLT"],
        weight_mode="gentle_score",
        starting_capital=Decimal("100000"),
    )

    _readiness, _bars_by_symbol, current_targets = current_engine.get_targets(start + timedelta(days=24))
    _readiness, _bars_by_symbol, gentle_targets = gentle_engine.get_targets(start + timedelta(days=24))
    current_weights = {target.symbol: target.target_weight for target in current_targets}
    gentle_weights = {target.symbol: target.target_weight for target in gentle_targets}

    positive_symbols = ["SPY", "QQQ", "IWM", "EFA", "EEM"]
    assert len({current_weights[symbol] for symbol in positive_symbols}) == 1
    assert len({gentle_weights[symbol] for symbol in positive_symbols}) > 1
    assert max(gentle_weights[symbol] for symbol in positive_symbols) < Decimal("0.35")


def test_mixed_kalshi_and_trend_run_share_order_execution_contract(tmp_path: Path) -> None:
    db = Database(tmp_path / "mixed.db")
    signal_repo = KalshiRepository(db)
    reporting_repo = ReportingRepository(db)
    trend_repo = TrendRepository(db)
    risk = RiskManager(0.02, 1.0, 1.0, 3)
    router = ExecutionRouter(DummyClient(), signal_repo, PortfolioState(DummyClient()), risk, __import__("logging").getLogger("mixed"), False)

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

    orders = reporting_repo.load_kalshi_reporting_bundle().orders
    outcomes = signal_repo.load_execution_outcomes()
    venues = {row["venue"] for row in orders}
    outcome_venues = {outcome.instrument.venue for outcome in outcomes}

    assert result.readiness.can_trade
    assert "kalshi" in venues
    assert "paper_equity" in venues
    assert "kalshi" in outcome_venues
    assert "paper_equity" in outcome_venues


def test_trend_engine_uses_live_equity_venue_when_router_has_live_broker(tmp_path: Path) -> None:
    db = Database(tmp_path / "trend_live.db")
    signal_repo = KalshiRepository(db)
    trend_repo = TrendRepository(db)
    risk = RiskManager(0.02, 1.0, 1.0, 3)
    router = ExecutionRouter(
        DummyClient(),
        signal_repo,
        PortfolioState(DummyClient()),
        risk,
        __import__("logging").getLogger("trend_live"),
        True,
        equity_broker=FakeLiveTrendBroker(),
    )
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

    readiness, _bars_by_symbol, targets = engine.get_targets(start + timedelta(days=24))
    signal = engine._build_signal(targets[0], quantity=1, side="buy", close=Decimal("524"), as_of=start + timedelta(days=24))

    assert readiness.can_trade is True
    assert engine.live_capable is True
    assert signal.legs[0].instrument.venue == "live_equity"


def test_trend_snapshot_persists_truthful_diagnostics(tmp_path: Path) -> None:
    db = Database(tmp_path / "trend_diag.db")
    signal_repo = KalshiRepository(db)
    trend_repo = TrendRepository(db)
    risk = RiskManager(0.02, 1.0, 1.0, 3)
    router = ExecutionRouter(DummyClient(), signal_repo, PortfolioState(DummyClient()), risk, __import__("logging").getLogger("trend_diag"), False)
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

    snapshot = trend_repo.load_equity_snapshots()[-1]
    assert result.snapshot.diagnostics["equity_reference"] == "100000"
    assert snapshot.diagnostics["targets"][0]["symbol"] == "SPY"
    assert "raw_signal" in snapshot.diagnostics["targets"][0]
    assert "cap_binding" in snapshot.diagnostics["targets"][0]
