from __future__ import annotations

from dataclasses import dataclass

from app.config import Settings
from app.models import Market


@dataclass(slots=True)
class MarketFilterResult:
    included: list[Market]
    excluded: list[tuple[Market, str, str, str]]
    diagnostics: list[dict]


def _tokenize(value: str) -> set[str]:
    token = []
    tokens: set[str] = set()
    for ch in value.lower():
        if ch.isalnum():
            token.append(ch)
        else:
            if token:
                tokens.add("".join(token))
                token = []
    if token:
        tokens.add("".join(token))
    return tokens


def _find_match(market: Market, terms: list[str], rule_type: str) -> tuple[str, str] | None:
    fields = {
        "ticker": market.ticker,
        "event_ticker": market.event_ticker,
        "title": market.title,
        "subtitle": market.subtitle,
    }
    token_fields = {name: _tokenize(value) for name, value in fields.items()}
    for term in terms:
        normalized = term.lower().replace("-", "")
        for field_name, tokens in token_fields.items():
            if normalized in tokens:
                return field_name, term
        if rule_type == "excluded":
            for field_name, value in fields.items():
                compact = value.lower().replace("-", "")
                if normalized and normalized in compact and (field_name == "ticker" or field_name == "event_ticker"):
                    return field_name, term
    return None


def filter_structural_markets(markets: list[Market], settings: Settings) -> MarketFilterResult:
    included: list[Market] = []
    excluded: list[tuple[Market, str, str, str]] = []
    diagnostics: list[dict] = []
    for market in markets:
        excluded_match = _find_match(market, settings.excluded_market_terms, "excluded")
        if excluded_match is not None:
            field_name, term = excluded_match
            excluded.append((market, "excluded_keyword", field_name, term))
            diagnostics.append(
                {
                    "ticker": market.ticker,
                    "title": market.title,
                    "subtitle": market.subtitle,
                    "included": False,
                    "reason": "excluded_keyword",
                    "matched_keyword": term,
                    "matched_rule_type": "excluded",
                    "matched_field": field_name,
                }
            )
            continue
        allowed_match = _find_match(market, settings.allowed_market_terms, "allowed")
        if settings.app_mode == "safe" and allowed_match is None:
            excluded.append((market, "not_structural_universe", "", ""))
            diagnostics.append(
                {
                    "ticker": market.ticker,
                    "title": market.title,
                    "subtitle": market.subtitle,
                    "included": False,
                    "reason": "not_structural_universe",
                    "matched_keyword": "",
                    "matched_rule_type": "allowed",
                    "matched_field": "",
                }
            )
            continue
        included.append(market)
        diagnostics.append(
            {
                "ticker": market.ticker,
                "title": market.title,
                "subtitle": market.subtitle,
                "included": True,
                "reason": "included",
                "matched_keyword": allowed_match[1] if allowed_match else "",
                "matched_rule_type": "allowed",
                "matched_field": allowed_match[0] if allowed_match else "",
            }
        )
    return MarketFilterResult(included=included, excluded=excluded, diagnostics=diagnostics)
