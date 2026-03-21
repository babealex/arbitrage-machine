import logging
from pathlib import Path

from app.db import Database
from app.execution.router import ExecutionRouter
from app.models import PortfolioSnapshot, Signal, StrategyName
from app.persistence.kalshi_repo import KalshiRepository
from app.persistence.reporting_repo import ReportingRepository
from app.portfolio.state import PortfolioState, ReconciliationResult
from app.risk.manager import RiskManager


class DummyClient:
    def __init__(self) -> None:
        self.calls = 0
        self.orders = []

    def create_order(self, payload):
        self.calls += 1
        self.orders.append(payload)
        if self.calls == 2:
            raise RuntimeError("second leg failed")
        return {"order_id": f"oid-{self.calls}", "status": "accepted", "order": {"order_id": f"oid-{self.calls}"}}

    def get_order(self, order_id):
        return {"order_id": order_id, "status": "filled"}

    def get_balance(self):
        return {"equity": 1000, "cash": 1000, "pnl_today": 0}

    def get_positions(self):
        return []

    def list_orders(self):
        return []

    def get_fills(self):
        return []


class DummyPortfolio(PortfolioState):
    def __init__(self, client):
        super().__init__(client)
        self.snapshot = PortfolioSnapshot(equity=1000, cash=1000, daily_pnl=0, positions={})

    def reconcile(self, db=None):
        return ReconciliationResult(snapshot=self.snapshot, success=True)


def test_router_attempts_flatten_on_partial_failure(tmp_path: Path) -> None:
    client = DummyClient()
    db = Database(tmp_path / "test.db")
    repo = KalshiRepository(db)
    reporting_repo = ReportingRepository(db)
    router = ExecutionRouter(client, repo, DummyPortfolio(client), RiskManager(0.02, 0.30, 0.02, 2, logger=logging.getLogger("test")), logging.getLogger("test"), True)
    signal = Signal(
        strategy=StrategyName.MONOTONICITY,
        ticker="PAIR",
        action="buy_lower_sell_higher",
        probability_edge=0.05,
        expected_edge_bps=500,
        quantity=1,
        legs=[
            {"ticker": "LOW", "action": "buy", "side": "yes", "price": 40, "tif": "FOK"},
            {"ticker": "HIGH", "action": "sell", "side": "yes", "price": 55, "tif": "IOC"},
        ],
    )
    assert not router.execute_signal(signal)
    kalshi_bundle = reporting_repo.load_kalshi_reporting_bundle()
    statuses = [row["status"] for row in kalshi_bundle.orders]
    event_statuses = [row["status"] for row in kalshi_bundle.order_events]
    assert "flatten_accepted" in statuses or "flatten_rejected" in statuses
    assert "created" in event_statuses
    assert "accepted" in event_statuses or "filled" in event_statuses
    assert "rejected" in event_statuses
