import logging
from decimal import Decimal

from app.db import Database
from app.execution.models import SignalLegIntent
from app.instruments.models import InstrumentRef
from app.models import PortfolioSnapshot, Position, Signal, SignalEdge, StrategyName
from app.persistence.risk_repo import RiskRepository
from app.risk.manager import RiskManager


def make_signal(quantity: int = 1) -> Signal:
    return Signal(
        strategy=StrategyName.MONOTONICITY,
        ticker="A",
        action="buy",
        quantity=quantity,
        edge=SignalEdge.from_values(edge_before_fees=0.12, edge_after_fees=0.1, fee_estimate=0.02),
        source="monotonicity_ladder",
        confidence=1.0,
        legs=[SignalLegIntent(InstrumentRef("A", contract_side="yes"), "buy", "limit", Decimal("0.40"), "IOC")],
    )


def test_daily_loss_kills_trading() -> None:
    risk = RiskManager(0.02, 0.30, 0.02, 2)
    snapshot = PortfolioSnapshot(equity=1000, cash=500, daily_pnl=-25, positions={})
    assert not risk.approve_signal(make_signal(), snapshot)
    assert risk.state.killed


def test_exposure_limit_rejects_signal() -> None:
    risk = RiskManager(0.02, 0.30, 0.02, 2)
    snapshot = PortfolioSnapshot(
        equity=1000,
        cash=500,
        daily_pnl=0,
        positions={"A": Position(ticker="A", yes_contracts=299, no_contracts=0, mark_price=0.5)},
    )
    assert not risk.approve_signal(make_signal(quantity=10), snapshot)


def test_risk_manager_persists_kill_transition(tmp_path) -> None:
    repo = RiskRepository(Database(tmp_path / "risk.db"))
    risk = RiskManager(0.02, 0.30, 0.02, 2, logger=logging.getLogger("test"), event_recorder=repo.persist_transition)
    risk.kill("stale_market_data", {"ticker": "A"})
    events = repo.load_transitions()
    assert len(events) == 1
    assert events[0].reason == "stale_market_data"
