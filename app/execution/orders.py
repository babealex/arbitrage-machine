from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from hashlib import sha256

from app.core.money import quantize_cents, to_decimal
from app.core.time import utc_now
from app.execution.models import OrderIntent, SignalLegIntent

TIF_MAP = {
    "IOC": "immediate_or_cancel",
    "FOK": "fill_or_kill",
    "GTC": "good_till_cancelled",
    "immediate_or_cancel": "immediate_or_cancel",
    "fill_or_kill": "fill_or_kill",
    "good_till_cancelled": "good_till_cancelled",
}


def deterministic_order_id(seed: str) -> str:
    return sha256(seed.encode()).hexdigest()[:24]


def build_order_intent(
    *,
    instrument,
    side: str,
    quantity: Decimal | str | int | float,
    order_type: str,
    strategy_name: str,
    signal_ref: str,
    seed: str,
    limit_price: Decimal | str | int | float | None = None,
    time_in_force: str = "day",
    created_at: datetime | None = None,
) -> OrderIntent:
    return OrderIntent(
        order_id=deterministic_order_id(seed),
        instrument=instrument,
        side=side,
        quantity=to_decimal(quantity),
        limit_price=None if limit_price is None else quantize_cents(limit_price),
        order_type=order_type,
        time_in_force=TIF_MAP.get(time_in_force, time_in_force),
        strategy_name=strategy_name,
        signal_ref=signal_ref,
        created_at=created_at or utc_now(),
    )


def build_order_intent_from_leg(leg: SignalLegIntent, quantity: int, strategy_name: str, signal_ref: str, seed: str, created_at: datetime | None = None) -> OrderIntent:
    return build_order_intent(
        instrument=leg.instrument,
        side=leg.side,
        quantity=quantity,
        order_type=leg.order_type,
        limit_price=leg.limit_price,
        time_in_force=leg.time_in_force,
        strategy_name=strategy_name,
        signal_ref=signal_ref,
        seed=seed,
        created_at=created_at,
    )


def to_api_payload(intent: OrderIntent) -> dict:
    price_cents = intent.limit_price_cents
    return {
        "ticker": intent.instrument.symbol,
        "client_order_id": intent.order_id,
        "side": intent.instrument.contract_side,
        "action": intent.side,
        "count": int(intent.quantity),
        "yes_price": price_cents if intent.instrument.contract_side == "yes" else None,
        "no_price": price_cents if intent.instrument.contract_side == "no" else None,
        "type": intent.order_type,
        "time_in_force": intent.time_in_force,
    }


def reverse_leg(leg: SignalLegIntent) -> SignalLegIntent:
    return SignalLegIntent(
        instrument=leg.instrument,
        side="sell" if leg.side == "buy" else "buy",
        order_type="limit",
        limit_price=Decimal("0.01") if leg.side == "buy" else Decimal("0.99"),
        time_in_force="IOC",
    )
