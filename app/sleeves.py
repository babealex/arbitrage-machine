from __future__ import annotations

from dataclasses import dataclass

from app.maturity import ACTIVE_CORE, EXPERIMENTAL
from app.strategy_doctrine import SLEEVE_DOCTRINE


@dataclass(frozen=True, slots=True)
class SleeveDefinition:
    sleeve_name: str
    maturity: str
    enabled_by_default: bool
    paper_capable: bool
    live_capable: bool
    capital_weight_default: float
    description: str


SLEEVE_REGISTRY: dict[str, SleeveDefinition] = {
    "trend": SleeveDefinition(
        sleeve_name="trend",
        maturity=ACTIVE_CORE,
        enabled_by_default=True,
        paper_capable=True,
        live_capable=False,
        capital_weight_default=0.75,
        description=SLEEVE_DOCTRINE["trend"].mandate,
    ),
    "kalshi_event": SleeveDefinition(
        sleeve_name="kalshi_event",
        maturity=ACTIVE_CORE,
        enabled_by_default=True,
        paper_capable=True,
        live_capable=False,
        capital_weight_default=0.25,
        description=SLEEVE_DOCTRINE["kalshi_event"].mandate,
    ),
    "options": SleeveDefinition(
        sleeve_name="options",
        maturity=EXPERIMENTAL,
        enabled_by_default=False,
        paper_capable=True,
        live_capable=False,
        capital_weight_default=0.0,
        description=SLEEVE_DOCTRINE["options"].mandate,
    ),
    "convexity": SleeveDefinition(
        sleeve_name="convexity",
        maturity=EXPERIMENTAL,
        enabled_by_default=False,
        paper_capable=True,
        live_capable=False,
        capital_weight_default=0.0,
        description=SLEEVE_DOCTRINE["convexity"].mandate,
    ),
}


def enabled_sleeves(settings) -> dict[str, SleeveDefinition]:
    items: dict[str, SleeveDefinition] = {}
    if settings.enable_trend_sleeve:
        items["trend"] = SLEEVE_REGISTRY["trend"]
    if settings.enable_kalshi_sleeve:
        items["kalshi_event"] = SLEEVE_REGISTRY["kalshi_event"]
    if settings.enable_options_sleeve:
        items["options"] = SLEEVE_REGISTRY["options"]
    if settings.enable_convexity_sleeve:
        items["convexity"] = SLEEVE_REGISTRY["convexity"]
    return items


def sleeve_name_for_strategy(strategy_name: str) -> str:
    normalized = str(strategy_name or "").strip().lower()
    if normalized == "momentum":
        return "trend"
    if normalized in {"monotonicity", "partition", "cross_market", "event_driven"}:
        return "kalshi_event"
    return "unknown"
