from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal
from typing import Any

from app.core.money import probability_from_cents, quantize_cents, to_decimal
from app.core.time import utc_now
from app.instruments.models import InstrumentRef


@dataclass(frozen=True, slots=True)
class SignalLegIntent:
    instrument: InstrumentRef
    side: str
    order_type: str
    limit_price: Decimal | None
    time_in_force: str

    @property
    def limit_probability(self):
        if self.limit_price is None:
            return None
        return probability_from_cents(int((self.limit_price * Decimal("100")).to_integral_value()))

    @property
    def price_cents(self) -> int | None:
        if self.limit_price is None:
            return None
        return int((quantize_cents(self.limit_price) * Decimal("100")).to_integral_value())

    def to_dict(self) -> dict[str, Any]:
        payload = {
            "instrument": self.instrument.to_dict(),
            "side": self.side,
            "order_type": self.order_type,
            "tif": self.time_in_force,
        }
        if self.limit_price is not None:
            payload["limit_price"] = format(self.limit_price, "f")
            payload["price"] = self.price_cents
        return payload

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> "SignalLegIntent":
        instrument_payload = payload.get("instrument") or {"symbol": payload.get("ticker"), "venue": payload.get("venue", "kalshi")}
        return cls(
            instrument=InstrumentRef.from_dict(instrument_payload),
            side=str(payload["side"]),
            order_type=str(payload.get("order_type", payload.get("action", "limit"))),
            limit_price=None if payload.get("limit_price", payload.get("price_cents", payload.get("price"))) is None else to_decimal(payload.get("limit_price", to_decimal(payload.get("price_cents", payload.get("price"))) / Decimal("100"))),
            time_in_force=str(payload.get("time_in_force", payload.get("tif"))),
        )


def serialize_signal_leg(leg: SignalLegIntent | dict[str, Any]) -> dict[str, Any]:
    if isinstance(leg, SignalLegIntent):
        return leg.to_dict()
    return SignalLegIntent.from_dict(leg).to_dict()


@dataclass(frozen=True, slots=True)
class OrderIntent:
    order_id: str
    instrument: InstrumentRef
    side: str
    quantity: Decimal
    order_type: str
    strategy_name: str
    signal_ref: str
    limit_price: Decimal | None = None
    time_in_force: str = "day"
    created_at: datetime = field(default_factory=utc_now)

    @property
    def quantity_decimal(self) -> Decimal:
        return to_decimal(self.quantity)

    @property
    def limit_price_cents(self) -> int | None:
        if self.limit_price is None:
            return None
        return int((quantize_cents(self.limit_price) * Decimal("100")).to_integral_value())


@dataclass(frozen=True, slots=True)
class ExecutionOutcome:
    client_order_id: str
    instrument: InstrumentRef
    side: str
    action: str
    status: str
    requested_quantity: int
    filled_quantity: int
    price_cents: int | None
    time_in_force: str
    is_flatten: bool
    created_at: datetime = field(default_factory=utc_now)
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_row(self) -> dict[str, Any]:
        return {
            "client_order_id": self.client_order_id,
            "ticker": self.instrument.symbol,
            "venue": self.instrument.venue,
            "contract_side": self.instrument.contract_side,
            "side": self.side,
            "action": self.action,
            "status": self.status,
            "requested_quantity": self.requested_quantity,
            "requested_quantity_decimal": str(self.requested_quantity),
            "filled_quantity": self.filled_quantity,
            "filled_quantity_decimal": str(self.filled_quantity),
            "price_cents": self.price_cents,
            "tif": self.time_in_force,
            "is_flatten": 1 if self.is_flatten else 0,
            "metadata": self.metadata,
            "created_at": self.created_at.isoformat(),
        }

    @classmethod
    def from_row(cls, row) -> "ExecutionOutcome":
        metadata = row["metadata"] if isinstance(row["metadata"], dict) else row["metadata"]
        return cls(
            client_order_id=str(row["client_order_id"]),
            instrument=InstrumentRef(symbol=str(row["ticker"]), venue=str(row["venue"]), contract_side=str(row["contract_side"]) if row.get("contract_side") is not None else None),
            side=str(row["side"]),
            action=str(row["action"]),
            status=str(row["status"]),
            requested_quantity=int(row["requested_quantity"]),
            filled_quantity=int(row["filled_quantity"]),
            price_cents=None if row["price_cents"] is None else int(row["price_cents"]),
            time_in_force=str(row["tif"]),
            is_flatten=bool(row["is_flatten"]),
            created_at=datetime.fromisoformat(row["created_at"]),
            metadata=metadata if isinstance(metadata, dict) else {},
        )

    @classmethod
    def from_exchange_fill(cls, raw_fill: dict[str, Any], *, venue: str = "kalshi") -> "ExecutionOutcome":
        client_order_id = str(raw_fill.get("order_id", raw_fill.get("orderId", "")))
        price_cents = raw_fill.get("price")
        contract_side = str(raw_fill.get("side", "yes")).lower()
        filled_quantity = int(raw_fill.get("count", raw_fill.get("quantity", 0)))
        return cls(
            client_order_id=client_order_id,
            instrument=InstrumentRef(symbol=str(raw_fill.get("ticker", "")), venue=venue, contract_side=contract_side if venue == "kalshi" else None),
            side="buy",
            action="limit",
            status="reconciled_fill",
            requested_quantity=filled_quantity,
            filled_quantity=filled_quantity,
            price_cents=None if price_cents is None else int(price_cents),
            time_in_force=str(raw_fill.get("time_in_force", raw_fill.get("timeInForce", "unknown"))),
            is_flatten=False,
            metadata={"raw_fill": raw_fill},
        )

    @classmethod
    def from_paper_equity_fill(
        cls,
        *,
        client_order_id: str,
        symbol: str,
        side: str,
        requested_quantity: int,
        filled_quantity: int,
        price: Decimal | str | int | float | None,
        status: str,
        metadata: dict[str, Any],
        created_at: datetime | None = None,
    ) -> "ExecutionOutcome":
        price_cents = None
        if price is not None:
            quantized = quantize_cents(price)
            price_cents = int((quantized * Decimal("100")).to_integral_value())
        return cls(
            client_order_id=client_order_id,
            instrument=InstrumentRef(symbol=symbol, venue="paper_equity"),
            side=side,
            action="rebalance",
            status=status,
            requested_quantity=requested_quantity,
            filled_quantity=filled_quantity,
            price_cents=price_cents,
            time_in_force="day",
            is_flatten=False,
            created_at=created_at or utc_now(),
            metadata=metadata,
        )
