from __future__ import annotations

import json
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any

from app.core.money import to_decimal
from app.execution.models import ExecutionOutcome, OrderIntent, SignalLegIntent, serialize_signal_leg
from app.instruments.models import InstrumentRef
from app.db import Database, _json_load
from app.models import Fill, PortfolioSnapshot, Signal, SignalEdge, StrategyEvaluation, StrategyName
from app.persistence.contracts import (
    CrossMarketSnapshotRecord,
    KalshiOrderRecord,
    MonotonicityObservationRecord,
)


class KalshiRepository:
    def __init__(self, db: Database) -> None:
        self.db = db

    def persist_signal(self, signal: Signal) -> None:
        probability_edge = self._decimal_text(signal.edge.edge_after_fees)
        expected_edge_bps = self._decimal_text(signal.edge.expected_edge_bps)
        self.db.execute(
            """
            INSERT INTO signals(
                strategy, ticker, action, probability_edge, expected_edge_bps,
                probability_edge_decimal, expected_edge_bps_decimal, edge_details,
                source, confidence, priority, quantity, legs, created_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                signal.strategy.value,
                signal.ticker,
                signal.action,
                float(signal.edge.edge_after_fees),
                float(signal.edge.expected_edge_bps),
                probability_edge,
                expected_edge_bps,
                json.dumps(signal.edge.to_dict()),
                signal.source,
                signal.confidence,
                signal.priority,
                signal.quantity,
                json.dumps([serialize_signal_leg(leg) for leg in signal.legs]),
                signal.ts.isoformat(),
            ),
        )

    def persist_evaluation(self, evaluation: StrategyEvaluation) -> None:
        edge_before_fees = self._decimal_text(evaluation.edge_before_fees)
        edge_after_fees = self._decimal_text(evaluation.edge_after_fees)
        fee_estimate = self._decimal_text(evaluation.fee_estimate)
        self.db.execute(
            """
            INSERT INTO strategy_evaluations(
                strategy, ticker, generated, reason, edge_before_fees, edge_after_fees, fee_estimate,
                edge_before_fees_decimal, edge_after_fees_decimal, fee_estimate_decimal,
                quantity, metadata, created_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                evaluation.strategy.value,
                evaluation.ticker,
                1 if evaluation.generated else 0,
                evaluation.reason,
                evaluation.edge_before_fees,
                evaluation.edge_after_fees,
                evaluation.fee_estimate,
                edge_before_fees,
                edge_after_fees,
                fee_estimate,
                evaluation.quantity,
                json.dumps(evaluation.metadata),
                evaluation.ts.isoformat(),
            ),
        )

    def persist_order(self, record: KalshiOrderRecord) -> None:
        metadata = record.metadata
        created_at = record.created_at or metadata.get("created_at") or datetime.now(timezone.utc).isoformat()
        price_cents = record.intent.limit_price_cents
        quantity_int = int(record.intent.quantity)
        self.db.execute(
            """
            INSERT INTO order_events(
                client_order_id, venue, ticker, contract_side, side, action, quantity, quantity_decimal,
                price, limit_price_decimal, tif, order_type, strategy_name, signal_ref,
                status, metadata, created_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                record.intent.order_id,
                record.intent.instrument.venue,
                record.intent.instrument.symbol,
                record.intent.instrument.contract_side,
                record.intent.side,
                record.intent.order_type,
                quantity_int,
                self._decimal_text(record.intent.quantity),
                0 if price_cents is None else price_cents,
                None if record.intent.limit_price is None else self._decimal_text(record.intent.limit_price),
                record.intent.time_in_force,
                record.intent.order_type,
                record.intent.strategy_name,
                record.intent.signal_ref,
                record.status,
                json.dumps(metadata),
                created_at,
            ),
        )
        self.db.execute(
            """
            INSERT INTO orders(
                client_order_id, venue, ticker, contract_side, side, action, quantity, quantity_decimal,
                price, limit_price_decimal, tif, order_type, strategy_name, signal_ref,
                status, metadata, created_at, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(client_order_id) DO UPDATE SET
                venue = excluded.venue,
                ticker = excluded.ticker,
                contract_side = excluded.contract_side,
                side = excluded.side,
                action = excluded.action,
                quantity = excluded.quantity,
                quantity_decimal = excluded.quantity_decimal,
                price = excluded.price,
                limit_price_decimal = excluded.limit_price_decimal,
                tif = excluded.tif,
                order_type = excluded.order_type,
                strategy_name = excluded.strategy_name,
                signal_ref = excluded.signal_ref,
                status = excluded.status,
                metadata = excluded.metadata,
                updated_at = excluded.updated_at
            """,
            (
                record.intent.order_id,
                record.intent.instrument.venue,
                record.intent.instrument.symbol,
                record.intent.instrument.contract_side,
                record.intent.side,
                record.intent.order_type,
                quantity_int,
                self._decimal_text(record.intent.quantity),
                0 if price_cents is None else price_cents,
                None if record.intent.limit_price is None else self._decimal_text(record.intent.limit_price),
                record.intent.time_in_force,
                record.intent.order_type,
                record.intent.strategy_name,
                record.intent.signal_ref,
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

    def persist_execution_outcome(self, outcome: ExecutionOutcome) -> None:
        row = outcome.to_row()
        self.db.execute(
            """
            INSERT INTO execution_outcomes(
                client_order_id, ticker, venue, contract_side, side, action, status, requested_quantity,
                requested_quantity_decimal, filled_quantity, filled_quantity_decimal, price_cents, tif, is_flatten, metadata, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                row["client_order_id"],
                row["ticker"],
                row["venue"],
                row["contract_side"],
                row["side"],
                row["action"],
                row["status"],
                row["requested_quantity"],
                row["requested_quantity_decimal"],
                row["filled_quantity"],
                row["filled_quantity_decimal"],
                row["price_cents"],
                row["tif"],
                row["is_flatten"],
                json.dumps(row["metadata"]),
                row["created_at"],
            ),
        )

    def load_execution_outcomes(self) -> list[ExecutionOutcome]:
        rows = self.db.fetch_table_rows("execution_outcomes")
        outcomes: list[ExecutionOutcome] = []
        for row in rows:
            payload = dict(row)
            payload["metadata"] = _json_load(row["metadata"])
            outcomes.append(ExecutionOutcome.from_row(payload))
        return outcomes

    def load_order_intents(self) -> list[OrderIntent]:
        intents: list[OrderIntent] = []
        for row in self.db.fetch_table_rows("orders"):
            intents.append(
                OrderIntent(
                    order_id=str(row["client_order_id"]),
                    instrument=InstrumentRef(
                        symbol=str(row["ticker"]),
                        venue=str(row["venue"] or "kalshi"),
                        contract_side=str(row["contract_side"]) if row["contract_side"] is not None else None,
                    ),
                    side=str(row["side"]),
                    quantity=to_decimal(row["quantity_decimal"] or row["quantity"]),
                    order_type=str(row["order_type"] or row["action"]),
                    strategy_name=str(row["strategy_name"] or "unknown"),
                    signal_ref=str(row["signal_ref"] or ""),
                    limit_price=None if row["limit_price_decimal"] is None else to_decimal(row["limit_price_decimal"]),
                    time_in_force=str(row["tif"]),
                    created_at=datetime.fromisoformat(row["created_at"]),
                )
            )
        return intents

    def load_signals(self) -> list[Signal]:
        rows = self.db.fetch_table_rows("signals")
        signals: list[Signal] = []
        for row in rows:
            edge_payload = _json_load(row["edge_details"])
            if not edge_payload:
                edge_payload = {
                    "edge_before_fees": row["probability_edge"],
                    "edge_after_fees": row["probability_edge_decimal"] or row["probability_edge"],
                    "fee_estimate": "0",
                    "execution_buffer": "0",
                }
            legs_payload = json.loads(row["legs"])
            signals.append(
                Signal(
                    strategy=StrategyName(str(row["strategy"])),
                    ticker=str(row["ticker"]),
                    action=str(row["action"]),
                    quantity=int(row["quantity"]),
                    edge=SignalEdge.from_dict(edge_payload),
                    source=str(row["source"] or "unknown"),
                    confidence=float(row["confidence"] if row["confidence"] is not None else 0.0),
                    priority=int(row["priority"] or 0),
                    legs=[SignalLegIntent.from_dict(leg) for leg in legs_payload],
                    ts=datetime.fromisoformat(row["created_at"]),
                )
            )
        return signals

    def load_evaluations(self) -> list[StrategyEvaluation]:
        rows = self.db.fetch_table_rows("strategy_evaluations")
        evaluations: list[StrategyEvaluation] = []
        for row in rows:
            evaluations.append(
                StrategyEvaluation(
                    strategy=StrategyName(str(row["strategy"])),
                    ticker=str(row["ticker"]),
                    generated=bool(row["generated"]),
                    reason=str(row["reason"]),
                    edge_before_fees=float(row["edge_before_fees_decimal"] or row["edge_before_fees"]),
                    edge_after_fees=float(row["edge_after_fees_decimal"] or row["edge_after_fees"]),
                    fee_estimate=float(row["fee_estimate_decimal"] or row["fee_estimate"]),
                    quantity=int(row["quantity"]),
                    metadata=_json_load(row["metadata"]),
                    ts=datetime.fromisoformat(row["created_at"]),
                )
            )
        return evaluations

    def persist_snapshot(self, snapshot: PortfolioSnapshot) -> None:
        payload = {
            ticker: {
                "yes_contracts": pos.yes_contracts,
                "no_contracts": pos.no_contracts,
                "mark_price": self._decimal_text(pos.mark_price),
            }
            for ticker, pos in snapshot.positions.items()
        }
        self.db.execute(
            """
            INSERT INTO pnl_snapshots(equity, cash, daily_pnl, equity_decimal, cash_decimal, daily_pnl_decimal, payload, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                snapshot.equity,
                snapshot.cash,
                snapshot.daily_pnl,
                self._decimal_text(snapshot.equity),
                self._decimal_text(snapshot.cash),
                self._decimal_text(snapshot.daily_pnl),
                json.dumps(payload),
                snapshot.ts.isoformat(),
            ),
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

    @staticmethod
    def _decimal_text(value: Decimal | str | int | float) -> str:
        return format(to_decimal(value), "f")
