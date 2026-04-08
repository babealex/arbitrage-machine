from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal
from enum import Enum
from typing import TYPE_CHECKING

from app.core.money import to_decimal
from app.core.time import utc_now

if TYPE_CHECKING:
    from app.execution.models import SignalLegIntent


class Side(str, Enum):
    YES = "yes"
    NO = "no"


class StrategyName(str, Enum):
    MONOTONICITY = "monotonicity"
    PARTITION = "partition"
    CROSS_MARKET = "cross_market"
    MOMENTUM = "momentum"
    EVENT_DRIVEN = "event_driven"


@dataclass(slots=True)
class OrderBookLevel:
    price: float
    size: int


@dataclass(slots=True)
class OrderBookSnapshot:
    market_ticker: str
    yes_bids: list[OrderBookLevel] = field(default_factory=list)
    yes_asks: list[OrderBookLevel] = field(default_factory=list)
    no_bids: list[OrderBookLevel] = field(default_factory=list)
    no_asks: list[OrderBookLevel] = field(default_factory=list)
    ts: datetime = field(default_factory=utc_now)

    def best_yes_bid(self) -> float | None:
        return self.yes_bids[0].price if self.yes_bids else None

    def best_yes_ask(self) -> float | None:
        return self.yes_asks[0].price if self.yes_asks else None

    def best_no_bid(self) -> float | None:
        return self.no_bids[0].price if self.no_bids else None

    def best_no_ask(self) -> float | None:
        return self.no_asks[0].price if self.no_asks else None

    def best_yes_bid_size(self) -> int:
        return self.yes_bids[0].size if self.yes_bids else 0

    def best_yes_ask_size(self) -> int:
        return self.yes_asks[0].size if self.yes_asks else 0

    def best_no_bid_size(self) -> int:
        return self.no_bids[0].size if self.no_bids else 0

    def best_no_ask_size(self) -> int:
        return self.no_asks[0].size if self.no_asks else 0


@dataclass(slots=True)
class Market:
    ticker: str
    event_ticker: str
    title: str
    subtitle: str
    status: str
    yes_ask: float | None = None
    yes_bid: float | None = None
    no_ask: float | None = None
    no_bid: float | None = None
    strike: float | None = None
    expiry: datetime | None = None
    metadata: dict = field(default_factory=dict)


@dataclass(slots=True)
class ExternalProbability:
    source: str
    key: str
    probability: float
    as_of: datetime
    metadata: dict = field(default_factory=dict)


@dataclass(slots=True)
class ExternalDistribution:
    source: str
    key: str
    values: dict[float, float]
    as_of: datetime
    metadata: dict = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class SignalEdge:
    edge_before_fees: Decimal
    edge_after_fees: Decimal
    fee_estimate: Decimal = Decimal("0")
    execution_buffer: Decimal = Decimal("0")

    @property
    def expected_edge_bps(self) -> Decimal:
        return self.edge_after_fees * Decimal("10000")

    def to_dict(self) -> dict[str, str]:
        return {
            "edge_before_fees": str(self.edge_before_fees),
            "edge_after_fees": str(self.edge_after_fees),
            "fee_estimate": str(self.fee_estimate),
            "execution_buffer": str(self.execution_buffer),
            "expected_edge_bps": str(self.expected_edge_bps),
        }

    @classmethod
    def from_values(
        cls,
        *,
        edge_before_fees: Decimal | str | int | float,
        edge_after_fees: Decimal | str | int | float,
        fee_estimate: Decimal | str | int | float = 0,
        execution_buffer: Decimal | str | int | float = 0,
    ) -> "SignalEdge":
        return cls(
            edge_before_fees=to_decimal(edge_before_fees),
            edge_after_fees=to_decimal(edge_after_fees),
            fee_estimate=to_decimal(fee_estimate),
            execution_buffer=to_decimal(execution_buffer),
        )

    @classmethod
    def from_dict(cls, payload: dict[str, object]) -> "SignalEdge":
        return cls.from_values(
            edge_before_fees=payload.get("edge_before_fees", 0),
            edge_after_fees=payload.get("edge_after_fees", 0),
            fee_estimate=payload.get("fee_estimate", 0),
            execution_buffer=payload.get("execution_buffer", 0),
        )


@dataclass(slots=True)
class Signal:
    strategy: StrategyName
    ticker: str
    action: str
    quantity: int
    edge: SignalEdge
    source: str
    confidence: float
    priority: int = 0
    legs: list["SignalLegIntent"] = field(default_factory=list)
    ts: datetime = field(default_factory=utc_now)

    @property
    def probability_edge(self) -> float:
        return float(self.edge.edge_after_fees)

    @property
    def expected_edge_bps(self) -> float:
        return float(self.edge.expected_edge_bps)


@dataclass(slots=True)
class StrategyEvaluation:
    strategy: StrategyName
    ticker: str
    generated: bool
    reason: str
    edge_before_fees: float
    edge_after_fees: float
    fee_estimate: float
    quantity: int
    metadata: dict = field(default_factory=dict)
    ts: datetime = field(default_factory=utc_now)


@dataclass(slots=True)
class Fill:
    order_id: str
    ticker: str
    quantity: int
    price: int
    side: Side
    ts: datetime = field(default_factory=utc_now)


@dataclass(slots=True)
class Position:
    ticker: str
    yes_contracts: int = 0
    no_contracts: int = 0
    mark_price: float = 0.0

    @property
    def exposure(self) -> float:
        return abs(self.yes_contracts) + abs(self.no_contracts)


@dataclass(slots=True)
class PortfolioSnapshot:
    equity: float
    cash: float
    daily_pnl: float
    positions: dict[str, Position] = field(default_factory=dict)
    ts: datetime = field(default_factory=utc_now)
