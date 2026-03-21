from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


class Side(str, Enum):
    YES = "yes"
    NO = "no"


class StrategyName(str, Enum):
    MONOTONICITY = "monotonicity"
    PARTITION = "partition"
    CROSS_MARKET = "cross_market"
    MOMENTUM = "momentum"


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


@dataclass(slots=True)
class Signal:
    strategy: StrategyName
    ticker: str
    action: str
    probability_edge: float
    expected_edge_bps: float
    quantity: int
    legs: list[dict] = field(default_factory=list)
    metadata: dict = field(default_factory=dict)
    ts: datetime = field(default_factory=utc_now)


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
class OrderIntent:
    ticker: str
    side: Side
    action: str
    quantity: int
    price: int
    time_in_force: str
    client_order_id: str


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
