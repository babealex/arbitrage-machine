from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal

from app.core.money import quantize_cents, to_decimal
from app.instruments.models import InstrumentRef
from app.models import Market


@dataclass(frozen=True, slots=True)
class MarketStateInput:
    observed_at: datetime
    instrument: InstrumentRef
    source: str
    reference_kind: str
    reference_price: Decimal
    provenance: str
    best_bid: Decimal | None = None
    best_ask: Decimal | None = None
    bid_size: int | None = None
    ask_size: int | None = None
    order_id: str | None = None

    @property
    def reference_price_cents(self) -> int:
        return int((quantize_cents(self.reference_price) * Decimal("100")).to_integral_value())

    @property
    def best_bid_cents(self) -> int | None:
        if self.best_bid is None:
            return None
        return int((quantize_cents(self.best_bid) * Decimal("100")).to_integral_value())

    @property
    def best_ask_cents(self) -> int | None:
        if self.best_ask is None:
            return None
        return int((quantize_cents(self.best_ask) * Decimal("100")).to_integral_value())

    def to_row(self) -> dict[str, object]:
        return {
            "venue": self.instrument.venue,
            "symbol": self.instrument.symbol,
            "contract_side": self.instrument.contract_side,
            "observed_at": self.observed_at.isoformat(),
            "reference_price_cents": self.reference_price_cents,
            "order_side": None,
            "best_bid_cents": self.best_bid_cents,
            "best_ask_cents": self.best_ask_cents,
            "bid_size": self.bid_size,
            "ask_size": self.ask_size,
            "source": self.source,
            "provenance": self.provenance,
            "kind": self.reference_kind,
            "order_id": self.order_id,
        }

    @classmethod
    def from_row(cls, row) -> "MarketStateInput":
        return cls(
            observed_at=datetime.fromisoformat(str(row["observed_at"])),
            instrument=InstrumentRef(
                symbol=str(row["symbol"]),
                venue=str(row["venue"]),
                contract_side=None if row["contract_side"] is None else str(row["contract_side"]),
            ),
            source=str(row["source"]),
            reference_kind=str(row["kind"]),
            reference_price=to_decimal(row["reference_price_cents"]) / Decimal("100"),
            provenance=str(row["provenance"]),
            best_bid=None if row["best_bid_cents"] is None else to_decimal(row["best_bid_cents"]) / Decimal("100"),
            best_ask=None if row["best_ask_cents"] is None else to_decimal(row["best_ask_cents"]) / Decimal("100"),
            bid_size=None if row["bid_size"] is None else int(row["bid_size"]),
            ask_size=None if row["ask_size"] is None else int(row["ask_size"]),
            order_id=None if row["order_id"] is None else str(row["order_id"]),
        )


def market_state_inputs_from_kalshi_market(market: Market, observed_at: datetime) -> list[MarketStateInput]:
    diagnostics = dict(market.metadata.get("quote_diagnostics", {}))
    inputs: list[MarketStateInput] = []
    for contract_side, bid_key, ask_key, bid_size_key, ask_size_key in (
        ("yes", "best_yes_bid", "best_yes_ask", "best_yes_bid_size", "best_yes_ask_size"),
        ("no", "best_no_bid", "best_no_ask", "best_no_bid_size", "best_no_ask_size"),
    ):
        bid = diagnostics.get(bid_key)
        ask = diagnostics.get(ask_key)
        if bid is None and ask is None:
            continue
        reference_price = bid if bid is not None else ask
        inputs.append(
            MarketStateInput(
                observed_at=observed_at,
                instrument=InstrumentRef(symbol=market.ticker, venue="kalshi", contract_side=contract_side),
                source="kalshi_quote_hydration",
                reference_kind="quote",
                reference_price=to_decimal(reference_price) / Decimal("100"),
                provenance="market_data_service",
                best_bid=None if bid is None else to_decimal(bid) / Decimal("100"),
                best_ask=None if ask is None else to_decimal(ask) / Decimal("100"),
                bid_size=None if diagnostics.get(bid_size_key) is None else int(diagnostics[bid_size_key]),
                ask_size=None if diagnostics.get(ask_size_key) is None else int(diagnostics[ask_size_key]),
            )
        )
    return inputs
