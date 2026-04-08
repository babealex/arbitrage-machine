from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal


@dataclass(frozen=True, slots=True)
class BrokerReadinessResult:
    is_ready: bool
    reasons: list[str]
    broker_name: str
    checked_at: datetime
    last_successful_auth_at: datetime | None = None
    last_market_data_check_at: datetime | None = None
    last_reconciliation_check_at: datetime | None = None


@dataclass(frozen=True, slots=True)
class AccountSnapshot:
    account_id: str
    net_liquidation: Decimal
    available_funds: Decimal
    buying_power: Decimal
    currency: str
    timestamp: datetime


@dataclass(frozen=True, slots=True)
class BrokerPosition:
    symbol: str
    quantity: Decimal
    avg_cost: Decimal
    market_price: Decimal | None
    source_timestamp: datetime


@dataclass(frozen=True, slots=True)
class BrokerOrder:
    broker_order_id: str
    client_order_id: str
    symbol: str
    side: str
    quantity: Decimal
    order_type: str
    limit_price: Decimal | None
    time_in_force: str
    status: str
    created_at: datetime
    reject_reason: str | None = None


@dataclass(frozen=True, slots=True)
class BrokerFill:
    broker_fill_id: str
    broker_order_id: str
    client_order_id: str
    symbol: str
    quantity: Decimal
    price: Decimal
    side: str
    filled_at: datetime


@dataclass(frozen=True, slots=True)
class TrendOrderRequest:
    symbol: str
    side: str
    quantity: Decimal
    order_type: str
    limit_price: Decimal | None
    time_in_force: str
    client_order_id: str


@dataclass(frozen=True, slots=True)
class BrokerOrderAck:
    broker_order_id: str
    client_order_id: str
    status: str
    received_at: datetime
    reject_reason: str | None = None


@dataclass(frozen=True, slots=True)
class ReconciliationResult:
    is_consistent: bool
    position_diffs: list[str] = field(default_factory=list)
    open_order_diffs: list[str] = field(default_factory=list)
    cash_diff: Decimal | None = None
    reasons: list[str] = field(default_factory=list)
