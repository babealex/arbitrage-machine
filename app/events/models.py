from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


@dataclass(slots=True)
class NewsItem:
    external_id: str
    timestamp_utc: datetime
    source: str
    headline: str
    body: str = ""
    url: str = ""
    metadata: dict = field(default_factory=dict)


@dataclass(slots=True)
class ClassifiedEvent:
    event_type: str
    region: str
    severity: float
    assets_affected: list[str]
    directional_bias: str
    tags: list[str] = field(default_factory=list)
    rationale: str = ""
    metadata: dict = field(default_factory=dict)


@dataclass(slots=True)
class TradeIdea:
    symbol: str
    side: str
    confidence: float
    event_type: str
    thesis: str
    confirmation_needed: int = 2
    scale_steps: int = 1
    scale_interval_seconds: int = 0
    metadata: dict = field(default_factory=dict)


@dataclass(slots=True)
class ConfirmationResult:
    symbol: str
    passed: bool
    agreeing_signals: int
    checked_signals: int
    details: list[dict] = field(default_factory=list)
    metadata: dict = field(default_factory=dict)


@dataclass(slots=True)
class BrokerFill:
    symbol: str
    side: str
    quantity: int
    price: float
    timestamp_utc: datetime = field(default_factory=utc_now)
    metadata: dict = field(default_factory=dict)

