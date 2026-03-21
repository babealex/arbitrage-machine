from __future__ import annotations

import re
from dataclasses import asdict, dataclass
from datetime import datetime

from app.models import ExternalDistribution, Market


MEETING_PATTERN = re.compile(
    r"\b(january|jan|february|feb|march|mar|april|apr|may|june|jun|july|jul|august|aug|september|sep|october|oct|november|nov|december|dec)\s+(20\d{2})\b",
    re.IGNORECASE,
)
RATE_PATTERN = re.compile(r"(\d+(?:\.\d+)?)")

MONTHS = {
    "jan": 1,
    "january": 1,
    "feb": 2,
    "february": 2,
    "mar": 3,
    "march": 3,
    "apr": 4,
    "april": 4,
    "may": 5,
    "jun": 6,
    "june": 6,
    "jul": 7,
    "july": 7,
    "aug": 8,
    "august": 8,
    "sep": 9,
    "september": 9,
    "oct": 10,
    "october": 10,
    "nov": 11,
    "november": 11,
    "dec": 12,
    "december": 12,
}


@dataclass(slots=True)
class ContractDefinition:
    family: str
    meeting_key: str | None
    settlement: str
    lower_bound: float | None
    upper_bound: float | None
    lower_inclusive: bool = True
    upper_inclusive: bool = False


def parse_contract_definition(market: Market) -> ContractDefinition | None:
    if market.metadata.get("contract_definition"):
        raw = market.metadata["contract_definition"]
        return ContractDefinition(
            family=raw["family"],
            meeting_key=raw.get("meeting_key"),
            settlement=raw.get("settlement", "upper_bound"),
            lower_bound=raw.get("lower_bound"),
            upper_bound=raw.get("upper_bound"),
            lower_inclusive=raw.get("lower_inclusive", True),
            upper_inclusive=raw.get("upper_inclusive", False),
        )

    text = f"{market.ticker} {market.title} {market.subtitle}".lower()
    if "fed" not in text and "target rate" not in text:
        return None
    meeting_match = MEETING_PATTERN.search(text)
    meeting_key = None
    if meeting_match:
        meeting_key = f"{int(meeting_match.group(2)):04d}-{MONTHS[meeting_match.group(1).lower()]:02d}"
    settlement = "upper_bound"
    if "lower bound" in text:
        settlement = "lower_bound"
    elif "midpoint" in text or "mid-point" in text or "mid point" in text:
        settlement = "midpoint"
    if "between" in text:
        rates = [float(match.group(1)) for match in RATE_PATTERN.finditer(text)]
        if len(rates) >= 2:
            return ContractDefinition("fed_target_rate", meeting_key, settlement, min(rates[:2]), max(rates[:2]), True, True)
    if "above" in text or "greater than" in text:
        rate = _first_rate(text)
        return ContractDefinition("fed_target_rate", meeting_key, settlement, rate, None, False, False)
    if "below" in text or "less than" in text:
        rate = _first_rate(text)
        return ContractDefinition("fed_target_rate", meeting_key, settlement, None, rate, True, False)
    if "at" in text or "equals" in text:
        rate = _first_rate(text)
        return ContractDefinition("fed_target_rate", meeting_key, settlement, rate, rate, True, True)
    return None


def map_market_probability(market: Market, distributions: dict[str, ExternalDistribution]) -> tuple[float | None, dict]:
    definition = parse_contract_definition(market)
    if definition is None:
        return None, {"reason": "missing_contract_definition"}
    distribution = _find_distribution(definition, distributions)
    if distribution is None:
        return None, {"reason": "missing_distribution", "meeting_key": definition.meeting_key}
    probability = 0.0
    for raw_rate, raw_prob in distribution.values.items():
        rate = _convert_rate(raw_rate, distribution.metadata.get("settlement", "upper_bound"), definition.settlement)
        if _within_bin(rate, definition):
            probability += raw_prob
    return probability, {
        "reason": "mapped",
        "meeting_key": definition.meeting_key,
        "distribution_key": distribution.key,
        "contract_definition": asdict(definition),
    }


def _find_distribution(definition: ContractDefinition, distributions: dict[str, ExternalDistribution]) -> ExternalDistribution | None:
    if definition.meeting_key and definition.meeting_key in distributions:
        return distributions[definition.meeting_key]
    if len(distributions) == 1:
        return next(iter(distributions.values()))
    return None


def _convert_rate(rate: float, source_settlement: str, target_settlement: str) -> float:
    if source_settlement == target_settlement:
        return rate
    if source_settlement == "upper_bound" and target_settlement == "midpoint":
        return rate - 0.125
    if source_settlement == "midpoint" and target_settlement == "upper_bound":
        return rate + 0.125
    if source_settlement == "lower_bound" and target_settlement == "midpoint":
        return rate + 0.125
    if source_settlement == "midpoint" and target_settlement == "lower_bound":
        return rate - 0.125
    return rate


def _within_bin(rate: float, definition: ContractDefinition) -> bool:
    lower_ok = True
    upper_ok = True
    if definition.lower_bound is not None:
        lower_ok = rate >= definition.lower_bound if definition.lower_inclusive else rate > definition.lower_bound
    if definition.upper_bound is not None:
        upper_ok = rate <= definition.upper_bound if definition.upper_inclusive else rate < definition.upper_bound
    return lower_ok and upper_ok


def _first_rate(text: str) -> float | None:
    match = RATE_PATTERN.search(text)
    return float(match.group(1)) if match else None
