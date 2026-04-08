from __future__ import annotations

from collections import defaultdict
from datetime import date
from decimal import Decimal

from app.core.money import to_decimal
from app.options.models import ChainSurfaceFeatures, ExpirySurfaceFeatures, OptionChainSnapshot, OptionQuote


def build_surface_features(snapshot: OptionChainSnapshot) -> tuple[list[ExpirySurfaceFeatures], ChainSurfaceFeatures]:
    by_expiry: dict[date, list[OptionQuote]] = defaultdict(list)
    for quote in snapshot.quotes:
        by_expiry[quote.contract.expiration_date].append(quote)
    expiry_features: list[ExpirySurfaceFeatures] = []
    for expiration_date in sorted(by_expiry):
        expiry_quotes = by_expiry[expiration_date]
        expiry_features.append(_build_expiry_features(snapshot, expiration_date, expiry_quotes))
    near = expiry_features[0] if expiry_features else None
    nxt = expiry_features[1] if len(expiry_features) > 1 else None
    return expiry_features, ChainSurfaceFeatures(
        underlying=snapshot.underlying,
        observed_at=snapshot.observed_at,
        source=snapshot.source,
        spot_price=snapshot.spot_price,
        expiries_count=len(expiry_features),
        atm_iv_near=None if near is None else near.atm_iv,
        atm_iv_next=None if nxt is None else nxt.atm_iv,
        atm_total_variance_near=None if near is None else near.atm_total_variance,
        atm_total_variance_next=None if nxt is None else nxt.atm_total_variance,
        atm_iv_term_slope=_term_slope(near, nxt),
        event_premium_proxy=_event_premium_proxy(near, nxt),
    )


def _build_expiry_features(snapshot: OptionChainSnapshot, expiration_date: date, quotes: list[OptionQuote]) -> ExpirySurfaceFeatures:
    days_to_expiry = max(1, (expiration_date - snapshot.observed_at.date()).days)
    atm_call = _nearest_quote(quotes, snapshot.spot_price, "call")
    atm_put = _nearest_quote(quotes, snapshot.spot_price, "put")
    atm_iv = _average_defined([_iv(atm_call), _iv(atm_put)])
    atm_total_variance = None if atm_iv is None else (atm_iv * atm_iv) * (Decimal(days_to_expiry) / Decimal("365"))
    put_skew = _relative_iv(_nearest_quote(quotes, snapshot.spot_price * Decimal("0.95"), "put"), atm_iv)
    call_skew = _relative_iv(_nearest_quote(quotes, snapshot.spot_price * Decimal("1.05"), "call"), atm_iv)
    return ExpirySurfaceFeatures(
        underlying=snapshot.underlying,
        observed_at=snapshot.observed_at,
        source=snapshot.source,
        expiration_date=expiration_date,
        days_to_expiry=days_to_expiry,
        atm_iv=atm_iv,
        atm_total_variance=atm_total_variance,
        put_skew_95=put_skew,
        call_skew_105=call_skew,
        quote_count=len(quotes),
    )


def _nearest_quote(quotes: list[OptionQuote], target_strike: Decimal, option_type: str) -> OptionQuote | None:
    candidates = [quote for quote in quotes if quote.contract.option_type == option_type and quote.implied_volatility is not None]
    if not candidates:
        return None
    return min(candidates, key=lambda quote: abs(quote.contract.strike - target_strike))


def _iv(quote: OptionQuote | None) -> Decimal | None:
    if quote is None:
        return None
    return quote.implied_volatility


def _average_defined(values: list[Decimal | None]) -> Decimal | None:
    defined = [value for value in values if value is not None]
    if not defined:
        return None
    return sum(defined, Decimal("0")) / Decimal(len(defined))


def _relative_iv(quote: OptionQuote | None, atm_iv: Decimal | None) -> Decimal | None:
    quote_iv = _iv(quote)
    if quote_iv is None or atm_iv is None:
        return None
    return quote_iv - atm_iv


def _term_slope(near: ExpirySurfaceFeatures | None, nxt: ExpirySurfaceFeatures | None) -> Decimal | None:
    if near is None or nxt is None or near.atm_iv is None or nxt.atm_iv is None:
        return None
    days_gap = max(1, nxt.days_to_expiry - near.days_to_expiry)
    return (nxt.atm_iv - near.atm_iv) / Decimal(days_gap)


def _event_premium_proxy(near: ExpirySurfaceFeatures | None, nxt: ExpirySurfaceFeatures | None) -> Decimal | None:
    if near is None or nxt is None:
        return None
    if near.atm_total_variance is None or nxt.atm_total_variance is None:
        return None
    return near.atm_total_variance - nxt.atm_total_variance
