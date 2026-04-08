from __future__ import annotations

import logging
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace

from app.brokers.ibkr_trend_adapter import IbkrTrendBrokerAdapter
from app.brokers.models import TrendOrderRequest
from app.db import Database
from app.execution.models import OrderIntent
from app.instruments.models import InstrumentRef
from app.persistence.kalshi_repo import KalshiRepository
from app.persistence.trend_broker_repo import TrendBrokerRepository
from app.persistence.trend_repo import TrendRepository


class FakeIbkrClient:
    def __init__(self) -> None:
        self.connected = False
        self.fail_connect = False
        self.fail_account = False
        self.reject_orders = False
        self._fills = []
        self._open_orders = []

    def connect(self, host: str, port: int, client_id: int, readonly: bool = False) -> None:
        if self.fail_connect:
            raise RuntimeError("auth_failed")
        self.connected = True

    def disconnect(self) -> None:
        self.connected = False

    def is_connected(self) -> bool:
        return self.connected

    def get_account_summary(self, account_id: str | None = None):
        if self.fail_account:
            raise RuntimeError("account_unavailable")
        return {
            "account_id": account_id or "DU123",
            "net_liquidation": "100000",
            "available_funds": "90000",
            "buying_power": "180000",
            "currency": "USD",
            "timestamp": datetime.now(timezone.utc),
        }

    def get_positions(self, account_id: str | None = None):
        return []

    def get_open_orders(self, account_id: str | None = None):
        return list(self._open_orders)

    def place_order(self, order: TrendOrderRequest, account_id: str | None = None):
        if self.reject_orders:
            return {
                "broker_order_id": "1001",
                "client_order_id": order.client_order_id,
                "status": "rejected",
                "received_at": datetime.now(timezone.utc),
                "reject_reason": "validation_error",
            }
        self._open_orders.append(
            {
                "broker_order_id": "1001",
                "client_order_id": order.client_order_id,
                "symbol": order.symbol,
                "side": order.side,
                "quantity": str(order.quantity),
                "order_type": order.order_type,
                "limit_price": None if order.limit_price is None else str(order.limit_price),
                "time_in_force": order.time_in_force,
                "status": "submitted",
                "created_at": datetime.now(timezone.utc),
            }
        )
        return {
            "broker_order_id": "1001",
            "client_order_id": order.client_order_id,
            "status": "submitted",
            "received_at": datetime.now(timezone.utc),
            "reject_reason": None,
        }

    def cancel_order(self, broker_order_id: str) -> None:
        self._open_orders = [row for row in self._open_orders if row["broker_order_id"] != broker_order_id]

    def get_fills(self, since=None, account_id: str | None = None):
        return list(self._fills)


def _config(**overrides):
    base = {
        "trend_live_broker_mode": "ibkr",
        "trend_live_broker_enabled": True,
        "ibkr_host": "127.0.0.1",
        "ibkr_port": 7497,
        "ibkr_client_id": 7,
        "ibkr_account_id": "DU123",
    }
    base.update(overrides)
    return SimpleNamespace(**base)


def test_ibkr_adapter_ready_when_all_checks_pass(tmp_path: Path) -> None:
    db = Database(tmp_path / "ibkr_ready.db")
    adapter = IbkrTrendBrokerAdapter(
        config=_config(),
        logger=logging.getLogger("ibkr"),
        trend_repo=TrendRepository(db),
        signal_repo=KalshiRepository(db),
        broker_repo=TrendBrokerRepository(db),
        client=FakeIbkrClient(),
    )

    readiness = adapter.readiness_check()

    assert readiness.is_ready is True
    assert readiness.reasons == []


def test_ibkr_adapter_not_ready_when_auth_fails(tmp_path: Path) -> None:
    db = Database(tmp_path / "ibkr_auth_fail.db")
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

    readiness = adapter.readiness_check()

    assert readiness.is_ready is False
    assert "ibkr_connect_failed" in readiness.reasons


def test_ibkr_adapter_rejected_order_is_persisted(tmp_path: Path) -> None:
    db = Database(tmp_path / "ibkr_reject.db")
    client = FakeIbkrClient()
    client.reject_orders = True
    adapter = IbkrTrendBrokerAdapter(
        config=_config(),
        logger=logging.getLogger("ibkr"),
        trend_repo=TrendRepository(db),
        signal_repo=KalshiRepository(db),
        broker_repo=TrendBrokerRepository(db),
        client=client,
    )
    intent = OrderIntent(
        order_id="trend-order-1",
        instrument=InstrumentRef(symbol="SPY", venue="live_equity"),
        side="buy",
        quantity=Decimal("10"),
        order_type="limit",
        strategy_name="momentum",
        signal_ref="momentum:SPY",
        limit_price=Decimal("500"),
        time_in_force="DAY",
        created_at=datetime(2026, 3, 22, 12, 0, tzinfo=timezone.utc),
    )

    outcome = adapter.execute_order(intent, source_metadata={}, market_state=None)

    assert outcome.status == "rejected"
    assert TrendBrokerRepository(db).count_rejects() == 1
