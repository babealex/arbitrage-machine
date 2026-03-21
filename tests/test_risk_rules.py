from app.models import PortfolioSnapshot, Position, Signal, StrategyName
from app.risk.manager import RiskManager


def make_signal(quantity: int = 1) -> Signal:
    return Signal(
        strategy=StrategyName.MONOTONICITY,
        ticker="A",
        action="buy",
        probability_edge=0.1,
        expected_edge_bps=1000,
        quantity=quantity,
        legs=[],
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
