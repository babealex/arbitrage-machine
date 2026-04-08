from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from decimal import Decimal

from app.core.money import to_decimal
from app.db import Database, _json_load
from app.execution.models import OrderIntent
from app.strategies.convexity.models import ConvexTradeCandidate, ConvexTradeOutcome, ConvexityOpportunity


class ConvexityRepository:
    def __init__(self, db: Database) -> None:
        self.db = db

    def persist_opportunities(self, run_id: str, opportunities: list[ConvexityOpportunity], *, timestamp: datetime | None = None) -> list[int]:
        observed_at = (timestamp or datetime.now(timezone.utc)).isoformat()
        ids: list[int] = []
        for item in opportunities:
            ids.append(
                self.db.execute_insert(
                    """
                    INSERT INTO convexity_opportunity_snapshots(
                        run_id, timestamp, symbol, setup_type, event_time, spot_price_decimal,
                        expected_move_pct_decimal, implied_move_pct_decimal, move_edge_pct_decimal,
                        realized_vol_pct_decimal, implied_vol_pct_decimal, volume_spike_ratio_decimal,
                        breakout_score_decimal, metadata_json
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        run_id,
                        observed_at,
                        item.symbol,
                        item.setup_type,
                        None if item.event_time is None else item.event_time.isoformat(),
                        self._decimal_text(item.spot_price),
                        self._decimal_text(item.expected_move_pct),
                        self._decimal_text(item.implied_move_pct),
                        self._decimal_text(item.expected_move_pct - item.implied_move_pct),
                        self._decimal_text(item.realized_vol_pct),
                        self._decimal_text(item.implied_vol_pct),
                        self._nullable_decimal(item.volume_spike_ratio),
                        self._nullable_decimal(item.breakout_score),
                        json.dumps(item.metadata),
                    ),
                )
            )
        return ids

    def persist_candidates(
        self,
        run_id: str,
        candidates: list[ConvexTradeCandidate],
        statuses: dict[str, str],
        rejection_reasons: dict[str, str],
        *,
        opportunity_id_map: dict[str, int] | None = None,
        timestamp: datetime | None = None,
    ) -> list[int]:
        observed_at = (timestamp or datetime.now(timezone.utc)).isoformat()
        ids: list[int] = []
        for candidate in candidates:
            ids.append(
                self.db.execute_insert(
                    """
                    INSERT INTO convexity_trade_candidates(
                        run_id, opportunity_id, candidate_id, timestamp, symbol, setup_type, structure_type,
                        expiry, legs_json, debit_decimal, max_loss_decimal, max_profit_estimate_decimal,
                        reward_to_risk_decimal, expected_value_decimal, expected_value_after_costs_decimal,
                        convex_score_decimal, breakout_sensitivity_decimal, liquidity_score_decimal,
                        confidence_score_decimal, status, rejection_reason, metadata_json
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        run_id,
                        None if opportunity_id_map is None else opportunity_id_map.get(candidate.symbol),
                        candidate.candidate_id,
                        observed_at,
                        candidate.symbol,
                        candidate.setup_type,
                        candidate.structure_type,
                        candidate.expiry.isoformat(),
                        json.dumps(candidate.legs, default=str),
                        self._decimal_text(candidate.debit),
                        self._decimal_text(candidate.max_loss),
                        self._nullable_decimal(candidate.max_profit_estimate),
                        self._decimal_text(candidate.reward_to_risk),
                        self._decimal_text(candidate.expected_value),
                        self._decimal_text(candidate.expected_value_after_costs),
                        self._decimal_text(candidate.convex_score),
                        self._decimal_text(candidate.breakout_sensitivity),
                        self._decimal_text(candidate.liquidity_score),
                        self._decimal_text(candidate.confidence_score),
                        statuses.get(candidate.candidate_id, "rejected"),
                        rejection_reasons.get(candidate.candidate_id),
                        json.dumps(candidate.metadata, default=str),
                    ),
                )
            )
        return ids

    def persist_order_intents(
        self,
        run_id: str,
        order_intents: list[OrderIntent],
        candidate_ids: list[str],
        *,
        candidate_lookup: dict[str, ConvexTradeCandidate] | None = None,
        timestamp: datetime | None = None,
        status: str = "created",
    ) -> list[int]:
        observed_at = (timestamp or datetime.now(timezone.utc)).isoformat()
        ids: list[int] = []
        for index, intent in enumerate(order_intents):
            candidate_id = candidate_ids[index] if index < len(candidate_ids) else intent.signal_ref
            candidate = None if candidate_lookup is None else candidate_lookup.get(candidate_id)
            ids.append(
                self.db.execute_insert(
                    """
                    INSERT INTO convexity_order_intents(
                        run_id, candidate_id, timestamp, symbol, structure_type, order_intent_json,
                        risk_amount_decimal, limit_debit_decimal, status, metadata_json
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        run_id,
                        candidate_id,
                        observed_at,
                        intent.instrument.symbol,
                        "unknown" if candidate is None else candidate.structure_type,
                        json.dumps(
                            {
                                "order_id": intent.order_id,
                                "symbol": intent.instrument.symbol,
                                "venue": intent.instrument.venue,
                                "side": intent.side,
                                "quantity": format(intent.quantity, "f"),
                                "limit_price": None if intent.limit_price is None else format(intent.limit_price, "f"),
                                "order_type": intent.order_type,
                                "time_in_force": intent.time_in_force,
                                "signal_ref": intent.signal_ref,
                            }
                        ),
                        self._decimal_text(Decimal("0") if candidate is None else candidate.max_loss),
                        self._decimal_text(Decimal("0") if candidate is None else candidate.debit),
                        status,
                        json.dumps({"paper_only": True}),
                    ),
                )
            )
        return ids

    def persist_trade_outcomes(self, outcomes: list[ConvexTradeOutcome]) -> None:
        for item in outcomes:
            self.db.execute(
                """
                INSERT INTO convexity_trade_outcomes(
                    candidate_id, order_intent_id, timestamp_opened, timestamp_closed, symbol, structure_type,
                    entry_cost_decimal, max_loss_decimal, realized_pnl_decimal, realized_multiple_decimal,
                    exit_reason, held_to_exit, metadata_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    item.candidate_id,
                    item.metadata.get("order_intent_id"),
                    str(item.metadata.get("timestamp_opened", datetime.now(timezone.utc).isoformat())),
                    str(item.metadata.get("timestamp_closed", datetime.now(timezone.utc).isoformat())),
                    item.symbol,
                    item.structure_type,
                    self._decimal_text(item.entry_cost),
                    self._decimal_text(item.max_loss),
                    self._decimal_text(item.realized_pnl),
                    self._decimal_text(item.realized_multiple),
                    item.exit_reason,
                    1 if item.held_to_exit else 0,
                    json.dumps(item.metadata, default=str),
                ),
            )

    def persist_run_summary(self, summary: dict) -> None:
        self.db.execute(
            """
            INSERT OR REPLACE INTO convexity_run_summaries(
                run_id, timestamp, scanned_opportunities, candidate_trades, accepted_trades,
                rejected_for_liquidity, rejected_for_negative_ev, rejected_for_risk_budget, rejected_other,
                open_convexity_risk_decimal, avg_reward_to_risk_decimal, avg_expected_value_after_costs_decimal,
                metadata_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                summary["run_id"],
                summary["timestamp"],
                summary["scanned_opportunities"],
                summary["candidate_trades"],
                summary["accepted_trades"],
                summary["rejected_for_liquidity"],
                summary["rejected_for_negative_ev"],
                summary["rejected_for_risk_budget"],
                summary["rejected_other"],
                self._decimal_text(summary["open_convexity_risk"]),
                self._decimal_text(summary["avg_reward_to_risk"]),
                self._decimal_text(summary["avg_expected_value_after_costs"]),
                json.dumps(summary.get("metadata_json", {}), default=str),
            ),
        )

    def load_recent_candidate_history(self, lookback_days: int = 90) -> list[dict]:
        cutoff = (datetime.now(timezone.utc) - timedelta(days=lookback_days)).isoformat()
        return [dict(row) for row in self.db.fetchall("SELECT * FROM convexity_trade_candidates WHERE timestamp >= ? ORDER BY timestamp ASC, id ASC", (cutoff,))]

    def load_recent_outcomes(self, lookback_days: int = 180) -> list[dict]:
        cutoff = (datetime.now(timezone.utc) - timedelta(days=lookback_days)).isoformat()
        return [dict(row) for row in self.db.fetchall("SELECT * FROM convexity_trade_outcomes WHERE timestamp_opened >= ? ORDER BY timestamp_opened ASC, id ASC", (cutoff,))]

    def load_convexity_summary_window(self, lookback_days: int = 30) -> dict:
        cutoff = (datetime.now(timezone.utc) - timedelta(days=lookback_days)).isoformat()
        summaries = [dict(row) for row in self.db.fetchall("SELECT * FROM convexity_run_summaries WHERE timestamp >= ? ORDER BY timestamp ASC", (cutoff,))]
        return {"runs": summaries}

    def load_latest_run_summary(self) -> dict | None:
        row = self.db.fetchone("SELECT * FROM convexity_run_summaries ORDER BY timestamp DESC LIMIT 1")
        return None if row is None else dict(row)

    @staticmethod
    def _decimal_text(value: Decimal | str | int | float) -> str:
        return format(to_decimal(value), "f")

    @classmethod
    def _nullable_decimal(cls, value: Decimal | None) -> str | None:
        return None if value is None else cls._decimal_text(value)
