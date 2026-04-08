from __future__ import annotations

from app.db import Database
from app.market_data.snapshots import MarketStateInput


class MarketStateRepository:
    def __init__(self, db: Database) -> None:
        self.db = db

    def persist_market_state_input(self, item: MarketStateInput) -> int:
        row = item.to_row()
        return self.db.execute_insert(
            """
            INSERT INTO market_state_inputs(
                venue, symbol, contract_side, observed_at, reference_price_cents, order_side,
                best_bid_cents, best_ask_cents, bid_size, ask_size, source, provenance, kind, order_id
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                row["venue"],
                row["symbol"],
                row["contract_side"],
                row["observed_at"],
                row["reference_price_cents"],
                row["order_side"],
                row["best_bid_cents"],
                row["best_ask_cents"],
                row["bid_size"],
                row["ask_size"],
                row["source"],
                row["provenance"],
                row["kind"],
                row["order_id"],
            ),
        )

    def persist_many(self, items: list[MarketStateInput]) -> None:
        for item in items:
            self.persist_market_state_input(item)

    def load_market_state_inputs(self) -> list[MarketStateInput]:
        return [
            MarketStateInput.from_row(row)
            for row in self.db.fetchall("SELECT * FROM market_state_inputs ORDER BY observed_at ASC, id ASC")
        ]
