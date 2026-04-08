from __future__ import annotations

from decimal import Decimal

from app.options.pricing import years_to_expiry
from app.strategies.convexity.models import ConvexityOpportunity, OptionContractQuote


def implied_move_from_atm_straddle(*, quotes: list[OptionContractQuote], spot_price: Decimal) -> Decimal | None:
    if spot_price <= 0:
        return None
    call, put = _find_atm_pair(quotes, spot_price)
    if call is None or put is None:
        return None
    return (call.mid + put.mid) / spot_price


def implied_move_from_atm_iv(*, atm_iv: Decimal | None, days_to_expiry: int) -> Decimal | None:
    if atm_iv is None or days_to_expiry <= 0:
        return None
    return atm_iv * years_to_expiry(days_to_expiry).sqrt()


def realized_event_move_estimate(
    *,
    realized_vol_lookahead_estimate: Decimal,
    event_history_median_move: Decimal | None,
    breakout_adjusted_move: Decimal | None,
) -> Decimal:
    candidates = [realized_vol_lookahead_estimate]
    if event_history_median_move is not None:
        candidates.append(event_history_median_move)
    if breakout_adjusted_move is not None:
        candidates.append(breakout_adjusted_move)
    return max(candidates)


def move_edge(*, expected_move_pct: Decimal, implied_move_pct: Decimal) -> Decimal:
    return expected_move_pct - implied_move_pct


def opportunity_move_edge(opportunity: ConvexityOpportunity) -> Decimal:
    return move_edge(expected_move_pct=opportunity.expected_move_pct, implied_move_pct=opportunity.implied_move_pct)


def _find_atm_pair(quotes: list[OptionContractQuote], spot_price: Decimal) -> tuple[OptionContractQuote | None, OptionContractQuote | None]:
    if not quotes:
        return None, None
    nearest_expiry = min({quote.expiry for quote in quotes})
    expiry_quotes = [quote for quote in quotes if quote.expiry == nearest_expiry]
    strikes = sorted({quote.strike for quote in expiry_quotes})
    if not strikes:
        return None, None
    atm_strike = min(strikes, key=lambda strike: abs(strike - spot_price))
    call = next((quote for quote in expiry_quotes if quote.strike == atm_strike and quote.option_type == "call"), None)
    put = next((quote for quote in expiry_quotes if quote.strike == atm_strike and quote.option_type == "put"), None)
    return call, put
