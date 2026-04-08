from __future__ import annotations

from decimal import Decimal

from app.core.money import quantize_cents, to_decimal


def apply_bps_slippage(
    *,
    reference_price: Decimal | str | int | float,
    side: str,
    slippage_bps: Decimal | str | int | float,
) -> Decimal:
    price = to_decimal(reference_price)
    bps = to_decimal(slippage_bps) / Decimal("10000")
    if side == "buy":
        return quantize_cents(price * (Decimal("1") + bps))
    return quantize_cents(price * (Decimal("1") - bps))


def realized_slippage_bps(
    *,
    expected_price_cents: int | None,
    realized_price_cents: int | None,
    side: str,
) -> Decimal:
    if expected_price_cents in (None, 0) or realized_price_cents is None:
        return Decimal("0")
    expected = Decimal(expected_price_cents)
    realized = Decimal(realized_price_cents)
    delta = realized - expected if side == "buy" else expected - realized
    return (delta / expected) * Decimal("10000")
