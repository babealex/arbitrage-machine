from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime
from decimal import Decimal

from app.core.money import to_decimal


@dataclass(frozen=True, slots=True)
class OptionContract:
    underlying: str
    expiration_date: date
    strike: Decimal
    option_type: str
    venue: str = "listed_option"
    multiplier: int = 100

    def contract_key(self) -> str:
        return f"{self.underlying}:{self.expiration_date.isoformat()}:{format(self.strike, 'f')}:{self.option_type}"


@dataclass(frozen=True, slots=True)
class OptionQuote:
    contract: OptionContract
    observed_at: datetime
    source: str
    bid: Decimal | None = None
    ask: Decimal | None = None
    last: Decimal | None = None
    implied_volatility: Decimal | None = None
    delta: Decimal | None = None
    gamma: Decimal | None = None
    vega: Decimal | None = None
    theta: Decimal | None = None
    bid_size: int | None = None
    ask_size: int | None = None
    open_interest: int | None = None
    volume: int | None = None


@dataclass(frozen=True, slots=True)
class OptionChainSnapshot:
    underlying: str
    observed_at: datetime
    source: str
    spot_price: Decimal
    forward_price: Decimal | None = None
    risk_free_rate: Decimal = Decimal("0")
    dividend_yield: Decimal = Decimal("0")
    realized_vol_20d: Decimal | None = None
    quotes: list[OptionQuote] = field(default_factory=list)


@dataclass(frozen=True, slots=True)
class ExpirySurfaceFeatures:
    underlying: str
    observed_at: datetime
    source: str
    expiration_date: date
    days_to_expiry: int
    atm_iv: Decimal | None
    atm_total_variance: Decimal | None
    put_skew_95: Decimal | None
    call_skew_105: Decimal | None
    quote_count: int


@dataclass(frozen=True, slots=True)
class ChainSurfaceFeatures:
    underlying: str
    observed_at: datetime
    source: str
    spot_price: Decimal
    expiries_count: int
    atm_iv_near: Decimal | None
    atm_iv_next: Decimal | None
    atm_total_variance_near: Decimal | None
    atm_total_variance_next: Decimal | None
    atm_iv_term_slope: Decimal | None
    event_premium_proxy: Decimal | None


@dataclass(frozen=True, slots=True)
class OptionStateVector:
    underlying: str
    observed_at: datetime
    source: str
    spot_price: Decimal
    forward_price: Decimal
    realized_vol_20d: Decimal | None
    atm_iv_near: Decimal | None
    atm_iv_next: Decimal | None
    atm_iv_term_slope: Decimal | None
    event_premium_proxy: Decimal | None
    vrp_proxy_near: Decimal | None
    total_variance_term_monotonic: bool


def option_quote(
    *,
    underlying: str,
    expiration_date: date,
    strike: Decimal | str | int | float,
    option_type: str,
    observed_at: datetime,
    source: str,
    bid: Decimal | str | int | float | None = None,
    ask: Decimal | str | int | float | None = None,
    last: Decimal | str | int | float | None = None,
    implied_volatility: Decimal | str | int | float | None = None,
    delta: Decimal | str | int | float | None = None,
    gamma: Decimal | str | int | float | None = None,
    vega: Decimal | str | int | float | None = None,
    theta: Decimal | str | int | float | None = None,
    bid_size: int | None = None,
    ask_size: int | None = None,
    open_interest: int | None = None,
    volume: int | None = None,
) -> OptionQuote:
    return OptionQuote(
        contract=OptionContract(
            underlying=underlying,
            expiration_date=expiration_date,
            strike=to_decimal(strike),
            option_type=option_type,
        ),
        observed_at=observed_at,
        source=source,
        bid=None if bid is None else to_decimal(bid),
        ask=None if ask is None else to_decimal(ask),
        last=None if last is None else to_decimal(last),
        implied_volatility=None if implied_volatility is None else to_decimal(implied_volatility),
        delta=None if delta is None else to_decimal(delta),
        gamma=None if gamma is None else to_decimal(gamma),
        vega=None if vega is None else to_decimal(vega),
        theta=None if theta is None else to_decimal(theta),
        bid_size=bid_size,
        ask_size=ask_size,
        open_interest=open_interest,
        volume=volume,
    )
