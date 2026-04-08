from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime
from decimal import Decimal


@dataclass(frozen=True, slots=True)
class ConvexityOpportunity:
    symbol: str
    setup_type: str
    event_time: datetime | None
    spot_price: Decimal
    expected_move_pct: Decimal
    implied_move_pct: Decimal
    realized_vol_pct: Decimal
    implied_vol_pct: Decimal
    volume_spike_ratio: Decimal | None
    breakout_score: Decimal | None
    metadata: dict = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class OptionContractQuote:
    symbol: str
    expiry: date
    strike: Decimal
    option_type: str
    bid: Decimal
    ask: Decimal
    mid: Decimal
    iv: Decimal | None
    delta: Decimal | None
    gamma: Decimal | None
    vega: Decimal | None
    theta: Decimal | None
    open_interest: int | None
    volume: int | None


@dataclass(frozen=True, slots=True)
class ConvexTradeCandidate:
    symbol: str
    setup_type: str
    structure_type: str
    expiry: date
    legs: list[dict]
    debit: Decimal
    max_loss: Decimal
    max_profit_estimate: Decimal | None
    reward_to_risk: Decimal
    expected_value: Decimal
    expected_value_after_costs: Decimal
    convex_score: Decimal
    breakout_sensitivity: Decimal
    liquidity_score: Decimal
    confidence_score: Decimal
    metadata: dict = field(default_factory=dict)

    @property
    def candidate_id(self) -> str:
        return str(self.metadata.get("candidate_id", f"{self.symbol}:{self.structure_type}:{self.expiry.isoformat()}"))


@dataclass(frozen=True, slots=True)
class ConvexTradeOutcome:
    candidate_id: str
    symbol: str
    structure_type: str
    entry_cost: Decimal
    max_loss: Decimal
    realized_pnl: Decimal
    realized_multiple: Decimal
    held_to_exit: bool
    exit_reason: str
    metadata: dict = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class ConvexityAccountState:
    nav: Decimal
    available_cash: Decimal
    open_convexity_risk: Decimal = Decimal("0")
    open_trade_count: int = 0
    symbol_risk: dict[str, Decimal] = field(default_factory=dict)
    correlated_event_risk: dict[str, Decimal] = field(default_factory=dict)
