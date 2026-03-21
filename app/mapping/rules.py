from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class MappingRule:
    name: str
    event_type: str
    region: str | None
    required_tags: tuple[str, ...]
    directional_bias: str | None
    long_symbols: tuple[str, ...]
    short_symbols: tuple[str, ...]
    confidence: float
    thesis: str


MAPPING_VERSION = "event-map-v1"

MAPPING_RULES: tuple[MappingRule, ...] = (
    MappingRule(
        name="middle_east_geopolitical",
        event_type="geopolitical",
        region="Middle East",
        required_tags=("energy_risk",),
        directional_bias=None,
        long_symbols=("XLE", "XOM", "CVX", "LMT"),
        short_symbols=("DAL", "UAL"),
        confidence=0.72,
        thesis="Middle East conflict tends to lift energy/defense and pressure airlines.",
    ),
    MappingRule(
        name="inflation_hot",
        event_type="macro",
        region=None,
        required_tags=("inflation_hot",),
        directional_bias="bearish",
        long_symbols=("UUP",),
        short_symbols=("SPY", "QQQ", "TLT"),
        confidence=0.74,
        thesis="Hot inflation is risk-off, dollar-positive, and bond-negative.",
    ),
    MappingRule(
        name="inflation_cool",
        event_type="macro",
        region=None,
        required_tags=("inflation_cool",),
        directional_bias="bullish",
        long_symbols=("SPY", "QQQ", "TLT"),
        short_symbols=("UUP",),
        confidence=0.70,
        thesis="Soft inflation is supportive for duration and growth beta.",
    ),
    MappingRule(
        name="hawkish_rates",
        event_type="macro",
        region="United States",
        required_tags=("hawkish_rates",),
        directional_bias="bearish",
        long_symbols=("UUP",),
        short_symbols=("SPY", "QQQ", "TLT"),
        confidence=0.68,
        thesis="Hawkish rate repricing is typically negative for equities and duration.",
    ),
    MappingRule(
        name="supply_shock_energy",
        event_type="supply_shock",
        region=None,
        required_tags=("supply_shock",),
        directional_bias=None,
        long_symbols=("XLE", "XOM", "CVX"),
        short_symbols=(),
        confidence=0.66,
        thesis="Energy supply disruptions support oil-linked equities.",
    ),
)
