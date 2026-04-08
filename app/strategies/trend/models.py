from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime
from decimal import Decimal

from app.core.time import utc_now


@dataclass(frozen=True, slots=True)
class DailyBar:
    symbol: str
    bar_date: date
    close: Decimal


@dataclass(frozen=True, slots=True)
class TrendTarget:
    symbol: str
    momentum_return: Decimal
    realized_volatility: Decimal
    target_weight: Decimal
    raw_weight: Decimal = Decimal("0")
    scaling_factor: Decimal = Decimal("1")
    cap_binding: str = "none"


@dataclass(frozen=True, slots=True)
class TrendPosition:
    symbol: str
    quantity: int
    average_price_cents: int
    market_price_cents: int
    market_value: Decimal
    updated_at: datetime = field(default_factory=utc_now)


@dataclass(frozen=True, slots=True)
class TrendEquitySnapshot:
    snapshot_date: date
    equity: Decimal
    cash: Decimal
    gross_exposure: Decimal
    net_exposure: Decimal
    drawdown: Decimal
    trade_count: int
    positions: dict[str, int]
    diagnostics: dict[str, object] = field(default_factory=dict)
    kill_switch_active: bool = False
    created_at: datetime = field(default_factory=utc_now)


@dataclass(frozen=True, slots=True)
class TrendReadiness:
    can_trade: bool
    halt_reasons: list[str]
    details: dict[str, object]
