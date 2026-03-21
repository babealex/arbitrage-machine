from __future__ import annotations

from hashlib import sha256

from app.models import OrderIntent, Side

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


def build_order_intent(ticker: str, side: str, action: str, quantity: int, price: int, tif: str, seed: str) -> OrderIntent:
    return OrderIntent(
        ticker=ticker,
        side=Side(side),
        action=action,
        quantity=quantity,
        price=price,
        time_in_force=TIF_MAP.get(tif, tif),
        client_order_id=deterministic_order_id(seed),
    )


def to_api_payload(intent: OrderIntent) -> dict:
    return {
        "ticker": intent.ticker,
        "client_order_id": intent.client_order_id,
        "side": intent.side.value,
        "action": intent.action,
        "count": intent.quantity,
        "yes_price": intent.price if intent.side.value == "yes" else None,
        "no_price": intent.price if intent.side.value == "no" else None,
        "type": "limit",
        "time_in_force": intent.time_in_force,
    }


def reverse_leg(leg: dict) -> dict:
    return {
        "ticker": leg["ticker"],
        "side": leg["side"],
        "action": "sell" if leg["action"] == "buy" else "buy",
        "price": 1 if leg["action"] == "buy" else 99,
        "tif": "IOC",
    }
