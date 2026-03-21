from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any

from app.db import Database, _json_load
from app.models import Fill, PortfolioSnapshot, Signal, StrategyEvaluation
from app.persistence.contracts import (
    CrossMarketSnapshotRecord,
    KalshiOrderRecord,
    MonotonicityObservationRecord,
)


class KalshiRepository:
    def __init__(self, db: Database) -> None:
        self.db = db

    def persist_signal(self, signal: Signal) -> None:
        self.db.execute(
            """
            INSERT INTO signals(strategy, ticker, action, probability_edge, expected_edge_bps, quantity, legs, metadata, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                signal.strategy.value,
                signal.ticker,
                signal.action,
                signal.probability_edge,
                signal.expected_edge_bps,
                signal.quantity,
                json.dumps(signal.legs),
                json.dumps(signal.metadata),
                signal.ts.isoformat(),
            ),
        )

    def persist_evaluation(self, evaluation: StrategyEvaluation) -> None:
        self.db.execute(
            """
            INSERT INTO strategy_evaluations(strategy, ticker, generated, reason, edge_before_fees, edge_after_fees, fee_estimate, quantity, metadata, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                evaluation.strategy.value,
                evaluation.ticker,
                1 if evaluation.generated else 0,
                evaluation.reason,
                evaluation.edge_before_fees,
                evaluation.edge_after_fees,
                evaluation.fee_estimate,
                evaluation.quantity,
                json.dumps(evaluation.metadata),
                evaluation.ts.isoformat(),
            ),
        )

    def persist_order(self, record: KalshiOrderRecord) -> None:
        metadata = record.metadata
        created_at = record.created_at or metadata.get("created_at") or datetime.now(timezone.utc).isoformat()
        self.db.execute(
            """
            INSERT INTO order_events(client_order_id, ticker, side, action, quantity, price, tif, status, metadata, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                record.intent.client_order_id,
                record.intent.ticker,
                record.intent.side.value,
                record.intent.action,
                record.intent.quantity,
                record.intent.price,
                record.intent.time_in_force,
                record.status,
                json.dumps(metadata),
                created_at,
            ),
        )
        self.db.execute(
            """
            INSERT INTO orders(client_order_id, ticker, side, action, quantity, price, tif, status, metadata, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(client_order_id) DO UPDATE SET
                ticker = excluded.ticker,
                side = excluded.side,
                action = excluded.action,
                quantity = excluded.quantity,
                price = excluded.price,
                tif = excluded.tif,
                status = excluded.status,
                metadata = excluded.metadata,
                updated_at = excluded.updated_at
            """,
            (
                record.intent.client_order_id,
                record.intent.ticker,
                record.intent.side.value,
                record.intent.action,
                record.intent.quantity,
                record.intent.price,
                record.intent.time_in_force,
                record.status,
                json.dumps(metadata),
                created_at,
                created_at,
            ),
        )

    def persist_fill(self, fill: Fill) -> None:
        self.db.execute(
            "INSERT INTO fills(order_id, ticker, quantity, price, side, created_at) VALUES (?, ?, ?, ?, ?, ?)",
            (fill.order_id, fill.ticker, fill.quantity, fill.price, fill.side.value, fill.ts.isoformat()),
        )

    def persist_snapshot(self, snapshot: PortfolioSnapshot) -> None:
        payload = {
            ticker: {
                "yes_contracts": pos.yes_contracts,
                "no_contracts": pos.no_contracts,
                "mark_price": pos.mark_price,
            }
            for ticker, pos in snapshot.positions.items()
        }
        self.db.execute(
            "INSERT INTO pnl_snapshots(equity, cash, daily_pnl, payload, created_at) VALUES (?, ?, ?, ?, ?)",
            (snapshot.equity, snapshot.cash, snapshot.daily_pnl, json.dumps(payload), snapshot.ts.isoformat()),
        )

    def replace_positions(self, snapshot: PortfolioSnapshot) -> None:
        self.db.execute("DELETE FROM positions")
        for ticker, pos in snapshot.positions.items():
            self.db.execute(
                "INSERT INTO positions(ticker, yes_contracts, no_contracts, mark_price, updated_at) VALUES (?, ?, ?, ?, ?)",
                (ticker, pos.yes_contracts, pos.no_contracts, pos.mark_price, snapshot.ts.isoformat()),
            )

    def persist_monotonicity_observation(self, observation: MonotonicityObservationRecord) -> None:
        self.db.execute(
            """
            INSERT OR REPLACE INTO monotonicity_signal_observations(
                signal_key, ticker, lower_ticker, higher_ticker, product_family, accepted_at,
                edge_before_fees, edge_after_fees, lower_yes_ask_price, lower_yes_ask_size,
                higher_yes_bid_price, higher_yes_bid_size, next_refresh_status,
                next_refresh_observed_at, time_to_disappear_seconds, latest_status,
                latest_observed_at, metadata
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                observation.signal_key,
                observation.ticker,
                observation.lower_ticker,
                observation.higher_ticker,
                observation.product_family,
                observation.accepted_at,
                observation.edge_before_fees,
                observation.edge_after_fees,
                observation.lower_yes_ask_price,
                observation.lower_yes_ask_size,
                observation.higher_yes_bid_price,
                observation.higher_yes_bid_size,
                observation.next_refresh_status,
                observation.next_refresh_observed_at,
                observation.time_to_disappear_seconds,
                observation.latest_status,
                observation.latest_observed_at,
                json.dumps(observation.metadata_json),
            ),
        )

    def persist_cross_market_snapshot(self, snapshot: CrossMarketSnapshotRecord) -> None:
        self.db.execute(
            """
            INSERT INTO cross_market_snapshots(
                timestamp, event_ticker, contract_pair, kalshi_probability, kalshi_bid, kalshi_ask,
                kalshi_yes_bid_size, kalshi_yes_ask_size, spy_price, qqq_price, tlt_price, btc_price,
                derived_risk_score, notes
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                snapshot.timestamp,
                snapshot.event_ticker,
                snapshot.contract_pair,
                snapshot.kalshi_probability,
                snapshot.kalshi_bid,
                snapshot.kalshi_ask,
                snapshot.kalshi_yes_bid_size,
                snapshot.kalshi_yes_ask_size,
                snapshot.spy_price,
                snapshot.qqq_price,
                snapshot.tlt_price,
                snapshot.btc_price,
                snapshot.derived_risk_score,
                snapshot.notes,
            ),
        )

    def open_order_ids(self) -> set[str]:
        return {
            row["client_order_id"]
            for row in self.db.fetchall(
                "SELECT client_order_id FROM orders WHERE status IN ('created', 'accepted')"
            )
        }

    def max_signal_id(self) -> int:
        row = self.db.fetchone("SELECT MAX(id) AS max_id FROM signals")
        return int(row["max_id"] or 0) if row else 0

    def max_evaluation_id(self) -> int:
        row = self.db.fetchone("SELECT MAX(id) AS max_id FROM strategy_evaluations")
        return int(row["max_id"] or 0) if row else 0
