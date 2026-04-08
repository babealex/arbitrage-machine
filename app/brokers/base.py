from __future__ import annotations

from datetime import datetime
from typing import Protocol

from app.brokers.models import (
    AccountSnapshot,
    BrokerFill,
    BrokerOrder,
    BrokerOrderAck,
    BrokerReadinessResult,
    BrokerPosition,
    ReconciliationResult,
    TrendOrderRequest,
)


class TrendBrokerAdapter(Protocol):
    def readiness_check(self) -> BrokerReadinessResult: ...

    def get_account_snapshot(self) -> AccountSnapshot: ...

    def get_positions(self) -> list[BrokerPosition]: ...

    def get_open_orders(self) -> list[BrokerOrder]: ...

    def place_order(self, order: TrendOrderRequest) -> BrokerOrderAck: ...

    def cancel_order(self, broker_order_id: str) -> None: ...

    def get_fills(self, since: datetime | None = None) -> list[BrokerFill]: ...

    def reconcile(self) -> ReconciliationResult: ...
