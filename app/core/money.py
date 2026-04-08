from __future__ import annotations

from decimal import Decimal, ROUND_CEILING, ROUND_HALF_EVEN


CENT = Decimal("0.01")
FOUR_DP = Decimal("0.0001")


def to_decimal(value: Decimal | str | int | float) -> Decimal:
    if isinstance(value, Decimal):
        return value
    return Decimal(str(value))


def quantize_cents(value: Decimal | str | int | float) -> Decimal:
    return to_decimal(value).quantize(CENT, rounding=ROUND_HALF_EVEN)


def ceil_four_dp(value: Decimal | str | int | float) -> Decimal:
    return to_decimal(value).quantize(FOUR_DP, rounding=ROUND_CEILING)


def bps_to_fraction(value_bps: Decimal | str | int | float) -> Decimal:
    return to_decimal(value_bps) / Decimal("10000")


def probability_from_cents(price_cents: int | float) -> Decimal:
    return to_decimal(price_cents) / Decimal("100")
