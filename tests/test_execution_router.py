import logging
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

from app.db import Database
from app.execution.equity_broker import EquityExecutionBroker
from app.execution.models import SignalLegIntent
from app.execution.orders import build_order_intent
from app.execution.router import ExecutionRouter
from app.instruments.models import InstrumentRef
from app.market_data.snapshots import MarketStateInput
from app.models import PortfolioSnapshot, Signal, SignalEdge, StrategyName
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


class FakeLiveEquityBroker(EquityExecutionBroker):
    def __init__(self) -> None:
        self.calls = []

    def is_live_capable(self) -> bool:
        return True

    def execute_order(self, intent, *, source_metadata, market_state):
        self.calls.append((intent, source_metadata, market_state))
        return __import__("app.execution.models", fromlist=["ExecutionOutcome"]).ExecutionOutcome(
            client_order_id=intent.order_id,
            instrument=intent.instrument,
            side=intent.side,
            action=intent.order_type,
            status="submitted",
            requested_quantity=int(intent.quantity),
            filled_quantity=0,
            price_cents=50123,
            time_in_force=intent.time_in_force,
            is_flatten=False,
            created_at=intent.created_at,
            metadata={**source_metadata, "broker_mode": "fake_live"},
        )


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
        quantity=1,
        edge=SignalEdge.from_values(edge_before_fees=0.06, edge_after_fees=0.05, fee_estimate=0.01),
        source="monotonicity_ladder",
        confidence=1.0,
        legs=[
            SignalLegIntent(InstrumentRef("LOW", contract_side="yes"), "buy", "limit", Decimal("0.40"), "FOK"),
            SignalLegIntent(InstrumentRef("HIGH", contract_side="yes"), "sell", "limit", Decimal("0.55"), "IOC"),
        ],
    )
    assert not router.execute_signal(signal)
    kalshi_bundle = reporting_repo.load_kalshi_reporting_bundle()
    statuses = [row["status"] for row in kalshi_bundle.orders]
    event_statuses = [row["status"] for row in kalshi_bundle.order_events]
    execution_statuses = [row["status"] for row in kalshi_bundle.execution_outcomes]
    assert "flatten_accepted" in statuses or "flatten_rejected" in statuses
    assert "created" in event_statuses
    assert "accepted" in event_statuses or "filled" in event_statuses
    assert "rejected" in event_statuses
    assert "rejected" in execution_statuses


def test_router_uses_live_equity_broker_for_live_equity_intents(tmp_path: Path) -> None:
    client = DummyClient()
    db = Database(tmp_path / "live_equity.db")
    repo = KalshiRepository(db)
    broker = FakeLiveEquityBroker()
    router = ExecutionRouter(
        client,
        repo,
        DummyPortfolio(client),
        RiskManager(0.02, 0.30, 0.02, 2, logger=logging.getLogger("test")),
        logging.getLogger("test"),
        True,
        equity_broker=broker,
    )
    created_at = datetime(2026, 3, 22, 12, 0, tzinfo=timezone.utc)
    intent = build_order_intent(
        instrument=InstrumentRef(symbol="SPY", venue="live_equity"),
        side="buy",
        quantity=10,
        order_type="market",
        strategy_name="momentum",
        signal_ref="momentum:SPY",
        seed="live-equity-test",
        created_at=created_at,
    )

    success, outcomes = router.execute_order_intents(
        [intent],
        source_metadata={"allocation_run_id": "run-1", "allocation_candidate_id": "cand-1"},
        market_state_inputs=[
            MarketStateInput(
                observed_at=created_at,
                instrument=InstrumentRef(symbol="SPY", venue="live_equity"),
                source="test",
                reference_kind="execution_reference",
                reference_price=Decimal("501.23"),
                provenance="test",
                order_id=intent.order_id,
            )
        ],
    )

    assert success is True
    assert len(broker.calls) == 1
    assert outcomes[0].status == "submitted"
    assert repo.load_execution_outcomes()[0].instrument.venue == "live_equity"
