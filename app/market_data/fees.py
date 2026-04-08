from __future__ import annotations

from dataclasses import dataclass
from decimal import ROUND_CEILING, ROUND_FLOOR, Decimal


ZERO = Decimal("0")
ONE = Decimal("1")
CENT = Decimal("0.01")
CENTICENT = Decimal("0.0001")
FEE_RATE = Decimal("0.07")


def as_decimal(value: Decimal | str | int | float) -> Decimal:
    if isinstance(value, Decimal):
        return value
    return Decimal(str(value))


def ceil_centicent(value: Decimal | str | int | float) -> Decimal:
    return as_decimal(value).quantize(CENTICENT, rounding=ROUND_CEILING)


def floor_cent(value: Decimal | str | int | float) -> Decimal:
    return as_decimal(value).quantize(CENT, rounding=ROUND_FLOOR)


def price_probability(price_dollars: Decimal | str | int | float) -> Decimal:
    probability = as_decimal(price_dollars)
    if probability < ZERO:
        return ZERO
    if probability > ONE:
        return ONE
    return probability


def trade_fee_dollars(price_dollars: Decimal | str | int | float, quantity: Decimal | str | int | float = ONE) -> Decimal:
    price = price_probability(price_dollars)
    contracts = as_decimal(quantity)
    if contracts <= ZERO:
        return ZERO
    raw_fee = FEE_RATE * price * (ONE - price) * contracts
    return ceil_centicent(raw_fee)


def balance_aligned_fee_dollars(
    price_dollars: Decimal | str | int | float,
    quantity: Decimal | str | int | float = ONE,
    *,
    side: str = "buy",
) -> Decimal:
    price = price_probability(price_dollars)
    contracts = as_decimal(quantity)
    if contracts <= ZERO:
        return ZERO
    trade_fee = trade_fee_dollars(price, contracts)
    revenue = price * contracts
    signed_revenue = -revenue if side == "buy" else revenue
    balance_change = signed_revenue - trade_fee
    aligned_change = floor_cent(balance_change)
    rounding_fee = balance_change - aligned_change
    return trade_fee + rounding_fee


def estimate_fee_cents(price_cents: int, contracts: int) -> int:
    fee_dollars = balance_aligned_fee_dollars(Decimal(price_cents) / Decimal("100"), contracts, side="buy")
    return int((fee_dollars * Decimal("100")).to_integral_value(rounding=ROUND_CEILING))


def fee_per_contract(price_cents: int, contracts: int = 1) -> float:
    if contracts <= 0:
        return 0.0
    fee_dollars = balance_aligned_fee_dollars(Decimal(price_cents) / Decimal("100"), contracts, side="buy")
    return float(fee_dollars / Decimal(contracts))


@dataclass(slots=True)
class EdgeThreshold:
    edge_before_fees: Decimal
    fee_estimate: Decimal
    execution_buffer: Decimal
    edge_after_fees: Decimal

    def passes_bps(self, min_edge_bps: float) -> bool:
        threshold = Decimal(str(min_edge_bps)) / Decimal("10000")
        return self.edge_after_fees >= threshold

    def passes_probability(self, min_edge: float) -> bool:
        return self.edge_after_fees >= Decimal(str(min_edge))


def edge_threshold(
    *,
    edge_before_fees: Decimal | str | int | float,
    fee_estimate: Decimal | str | int | float,
    execution_buffer: Decimal | str | int | float,
) -> EdgeThreshold:
    raw_edge = as_decimal(edge_before_fees)
    fee = as_decimal(fee_estimate)
    buffer_value = as_decimal(execution_buffer)
    return EdgeThreshold(
        edge_before_fees=raw_edge,
        fee_estimate=fee,
        execution_buffer=buffer_value,
        edge_after_fees=raw_edge - fee - buffer_value,
    )
