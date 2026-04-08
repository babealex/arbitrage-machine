from __future__ import annotations

import math
from decimal import Decimal

from app.core.money import to_decimal


def black_scholes_price(
    *,
    spot: Decimal,
    strike: Decimal,
    time_to_expiry_years: Decimal,
    risk_free_rate: Decimal,
    dividend_yield: Decimal,
    implied_volatility: Decimal,
    option_type: str,
) -> Decimal:
    tau = float(max(time_to_expiry_years, Decimal("0")))
    s = float(spot)
    k = float(strike)
    r = float(risk_free_rate)
    q = float(dividend_yield)
    sigma = float(max(implied_volatility, Decimal("0.0001")))
    if tau <= 0.0:
        return intrinsic_value(spot=spot, strike=strike, option_type=option_type)
    sqrt_tau = math.sqrt(tau)
    d1 = (math.log(s / k) + (r - q + 0.5 * sigma * sigma) * tau) / (sigma * sqrt_tau)
    d2 = d1 - sigma * sqrt_tau
    discount_dividend = math.exp(-q * tau)
    discount_rate = math.exp(-r * tau)
    if option_type.lower() == "call":
        value = s * discount_dividend * _norm_cdf(d1) - k * discount_rate * _norm_cdf(d2)
    else:
        value = k * discount_rate * _norm_cdf(-d2) - s * discount_dividend * _norm_cdf(-d1)
    return to_decimal(value)


def intrinsic_value(*, spot: Decimal, strike: Decimal, option_type: str) -> Decimal:
    if option_type.lower() == "call":
        return max(Decimal("0"), spot - strike)
    return max(Decimal("0"), strike - spot)


def years_to_expiry(days_to_expiry: int) -> Decimal:
    return to_decimal(days_to_expiry) / Decimal("365")


def _norm_cdf(value: float) -> float:
    return 0.5 * (1.0 + math.erf(value / math.sqrt(2.0)))
