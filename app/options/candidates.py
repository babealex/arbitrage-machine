from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from decimal import Decimal

from app.options.models import OptionChainSnapshot, OptionQuote, OptionStateVector
from app.options.regimes import classify_regime
from app.options.structures import OptionStructure, call_calendar, call_vertical_spread, long_straddle, put_vertical_spread


@dataclass(frozen=True, slots=True)
class CandidateFilterConfig:
    min_days_to_expiry: int = 5
    max_near_days_to_expiry: int = 21
    max_far_days_to_expiry: int = 60
    max_relative_spread: Decimal = Decimal("0.20")
    min_open_interest: int = 100


def generate_candidates(
    *,
    snapshot: OptionChainSnapshot,
    state_vector: OptionStateVector,
    config: CandidateFilterConfig | None = None,
) -> list[OptionStructure]:
    settings = config or CandidateFilterConfig()
    regime = classify_regime(state_vector)
    if regime.name == "unstable_surface":
        return []
    expiries = sorted({quote.contract.expiration_date for quote in snapshot.quotes})
    near_expiry = _find_near_expiry(snapshot=snapshot, expiries=expiries, settings=settings)
    far_expiry = _find_far_expiry(snapshot=snapshot, expiries=expiries, near_expiry=near_expiry, settings=settings)
    atm_strike = _nearest_strike(snapshot.quotes, snapshot.forward_price or snapshot.spot_price)
    strikes = sorted({quote.contract.strike for quote in snapshot.quotes if quote.contract.expiration_date == near_expiry})
    candidates: list[OptionStructure] = []

    if regime.allow_long_convexity and near_expiry is not None and atm_strike is not None and _has_liquid_pair(snapshot, near_expiry, atm_strike, settings):
        candidates.append(long_straddle(
            underlying=snapshot.underlying,
            expiration_date=near_expiry,
            strike=atm_strike,
            quantity=1,
        ))

    higher_strike = _next_higher_strike(strikes, atm_strike)
    if near_expiry is not None and atm_strike is not None and higher_strike is not None:
        if _has_liquid_call(snapshot, near_expiry, atm_strike, settings) and _has_liquid_call(snapshot, near_expiry, higher_strike, settings):
            candidates.append(call_vertical_spread(
                underlying=snapshot.underlying,
                expiration_date=near_expiry,
                long_strike=atm_strike,
                short_strike=higher_strike,
                quantity=1,
            ))

    lower_strike = _next_lower_strike(strikes, atm_strike)
    if regime.allow_downside_skew_trades and near_expiry is not None and atm_strike is not None and lower_strike is not None:
        if _has_liquid_put(snapshot, near_expiry, atm_strike, settings) and _has_liquid_put(snapshot, near_expiry, lower_strike, settings):
            candidates.append(put_vertical_spread(
                underlying=snapshot.underlying,
                expiration_date=near_expiry,
                long_strike=atm_strike,
                short_strike=lower_strike,
                quantity=1,
            ))

    if near_expiry is not None and far_expiry is not None and atm_strike is not None and regime.prefer_calendars:
        if _has_liquid_call(snapshot, near_expiry, atm_strike, settings) and _has_liquid_call(snapshot, far_expiry, atm_strike, settings):
            candidates.append(call_calendar(
                underlying=snapshot.underlying,
                near_expiration_date=near_expiry,
                far_expiration_date=far_expiry,
                strike=atm_strike,
                quantity=1,
            ))

    return candidates


def _find_near_expiry(*, snapshot: OptionChainSnapshot, expiries: list[date], settings: CandidateFilterConfig) -> date | None:
    for expiry in expiries:
        days = (expiry - snapshot.observed_at.date()).days
        if settings.min_days_to_expiry <= days <= settings.max_near_days_to_expiry:
            return expiry
    return None


def _find_far_expiry(
    *,
    snapshot: OptionChainSnapshot,
    expiries: list[date],
    near_expiry: date | None,
    settings: CandidateFilterConfig,
) -> date | None:
    if near_expiry is None:
        return None
    for expiry in expiries:
        days = (expiry - snapshot.observed_at.date()).days
        if expiry > near_expiry and days <= settings.max_far_days_to_expiry:
            return expiry
    return None


def _nearest_strike(quotes: list[OptionQuote], target: Decimal) -> Decimal | None:
    if not quotes:
        return None
    strikes = sorted({quote.contract.strike for quote in quotes})
    return min(strikes, key=lambda strike: abs(strike - target))


def _next_higher_strike(strikes: list[Decimal], base: Decimal | None) -> Decimal | None:
    if base is None:
        return None
    for strike in strikes:
        if strike > base:
            return strike
    return None


def _next_lower_strike(strikes: list[Decimal], base: Decimal | None) -> Decimal | None:
    if base is None:
        return None
    for strike in reversed(strikes):
        if strike < base:
            return strike
    return None


def _has_liquid_pair(snapshot: OptionChainSnapshot, expiry: date, strike: Decimal, settings: CandidateFilterConfig) -> bool:
    return _has_liquid_contract(snapshot, expiry, strike, "call", settings) and _has_liquid_contract(snapshot, expiry, strike, "put", settings)


def _has_liquid_call(snapshot: OptionChainSnapshot, expiry: date, strike: Decimal, settings: CandidateFilterConfig) -> bool:
    return _has_liquid_contract(snapshot, expiry, strike, "call", settings)


def _has_liquid_put(snapshot: OptionChainSnapshot, expiry: date, strike: Decimal, settings: CandidateFilterConfig) -> bool:
    return _has_liquid_contract(snapshot, expiry, strike, "put", settings)


def _has_liquid_contract(
    snapshot: OptionChainSnapshot,
    expiry: date,
    strike: Decimal,
    option_type: str,
    settings: CandidateFilterConfig,
) -> bool:
    for quote in snapshot.quotes:
        if quote.contract.expiration_date != expiry or quote.contract.strike != strike or quote.contract.option_type != option_type:
            continue
        return _is_liquid(quote, settings)
    return False


def _is_liquid(quote: OptionQuote, settings: CandidateFilterConfig) -> bool:
    if quote.open_interest is not None and quote.open_interest < settings.min_open_interest:
        return False
    if quote.bid is None or quote.ask is None or quote.ask <= quote.bid:
        return False
    mid = (quote.bid + quote.ask) / Decimal("2")
    if mid <= Decimal("0"):
        return False
    relative_spread = (quote.ask - quote.bid) / mid
    return relative_spread <= settings.max_relative_spread
