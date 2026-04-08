from __future__ import annotations

import json

from app.db import Database
from app.persistence.contracts import (
    BeliefSourceRecord,
    BeliefSnapshotRecord,
    CrossMarketDisagreementRecord,
    DisagreementSnapshotRecord,
    EventDefinitionRecord,
    OutcomeRealizationRecord,
    OutcomeTrackingRecord,
    OutcomeTrackingUpdate,
)


class BeliefRepository:
    def __init__(self, db: Database) -> None:
        self.db = db

    def persist_belief_source(self, source: BeliefSourceRecord) -> None:
        self.db.execute(
            """
            INSERT OR REPLACE INTO belief_sources(
                source_key, source_type, venue, instrument_class, description, active, metadata_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                source.source_key,
                source.source_type,
                source.venue,
                source.instrument_class,
                source.description,
                1 if source.active else 0,
                json.dumps(source.metadata_json),
            ),
        )

    def persist_event_definition(self, event: EventDefinitionRecord) -> None:
        self.db.execute(
            """
            INSERT OR REPLACE INTO event_definitions(
                event_key, family, normalized_underlying_key, title, event_date, metadata_json
            ) VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                event.event_key,
                event.family,
                event.normalized_underlying_key,
                event.title,
                event.event_date,
                json.dumps(event.metadata_json),
            ),
        )

    def due_outcomes(self, now_iso: str):
        return self.db.fetchall(
            """
            SELECT *
            FROM outcome_tracking
            WHERE (due_at_30s IS NOT NULL AND due_at_30s <= ? AND value_after_30s IS NULL)
               OR (due_at_60s IS NOT NULL AND due_at_60s <= ? AND value_after_60s IS NULL)
               OR (due_at_300s IS NOT NULL AND due_at_300s <= ? AND value_after_300s IS NULL)
               OR (due_at_900s IS NOT NULL AND due_at_900s <= ? AND value_after_900s IS NULL)
            ORDER BY timestamp_detected ASC
            """,
            (now_iso, now_iso, now_iso, now_iso),
        )

    def update_outcome_tracking(self, row_id: int, outcome: OutcomeTrackingUpdate) -> None:
        self.db.execute(
            """
            UPDATE outcome_tracking
            SET metadata_json = ?,
                due_at_30s = ?,
                due_at_60s = ?,
                due_at_300s = ?,
                due_at_900s = ?,
                value_after_30s = ?,
                value_after_60s = ?,
                value_after_300s = ?,
                value_after_900s = ?,
                max_favorable_move = ?,
                max_adverse_move = ?,
                resolved_direction = ?
            WHERE id = ?
            """,
            (
                json.dumps(outcome.metadata_json),
                outcome.due_at_30s,
                outcome.due_at_60s,
                outcome.due_at_300s,
                outcome.due_at_900s,
                outcome.value_after_30s,
                outcome.value_after_60s,
                outcome.value_after_300s,
                outcome.value_after_900s,
                outcome.max_favorable_move,
                outcome.max_adverse_move,
                outcome.resolved_direction,
                row_id,
            ),
        )

    def persist_belief_snapshot(self, snapshot: BeliefSnapshotRecord) -> None:
        self.db.execute(
            """
            INSERT INTO belief_snapshots(
                timestamp_utc, source_key, event_key, normalized_underlying_key, family, event_ticker,
                market_ticker, source_type, object_type, value, confidence, comparison_metric,
                payload_summary_json, belief_summary_json, quality_flags_json, cost_estimate_bps,
                bid, ask, size_bid, size_ask, metadata_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                snapshot.timestamp_utc,
                snapshot.source_key,
                snapshot.event_key,
                snapshot.normalized_underlying_key,
                snapshot.family,
                snapshot.event_ticker,
                snapshot.market_ticker,
                snapshot.source_type,
                snapshot.object_type,
                snapshot.value,
                snapshot.confidence,
                snapshot.comparison_metric,
                json.dumps(snapshot.payload_summary_json),
                json.dumps(snapshot.belief_summary_json),
                json.dumps(snapshot.quality_flags_json),
                snapshot.cost_estimate_bps,
                snapshot.bid,
                snapshot.ask,
                snapshot.size_bid,
                snapshot.size_ask,
                json.dumps(snapshot.metadata_json),
            ),
        )

    def persist_disagreement_snapshot(self, snapshot: DisagreementSnapshotRecord) -> None:
        self.db.execute(
            """
            INSERT INTO disagreement_snapshots(
                timestamp_utc, normalized_underlying_key, left_source_key, right_source_key,
                gross_disagreement, cost_estimate_bps, net_disagreement, tradeable,
                event_group, kalshi_reference, external_reference,
                disagreement_score, zscore, metadata_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                snapshot.timestamp_utc,
                snapshot.normalized_underlying_key,
                snapshot.left_source_key,
                snapshot.right_source_key,
                snapshot.gross_disagreement,
                snapshot.cost_estimate_bps,
                snapshot.net_disagreement,
                1 if snapshot.tradeable else 0,
                snapshot.event_group,
                snapshot.kalshi_reference,
                snapshot.external_reference,
                snapshot.disagreement_score,
                snapshot.zscore,
                json.dumps(snapshot.metadata_json),
            ),
        )

    def persist_cross_market_disagreement(self, row: CrossMarketDisagreementRecord) -> None:
        self.db.execute(
            """
            INSERT INTO cross_market_disagreements(
                timestamp_utc, normalized_underlying_key, event_key, comparison_metric,
                left_source_key, right_source_key, left_value, right_value, gross_disagreement,
                fees_bps, slippage_bps, execution_uncertainty_bps, net_disagreement,
                tradeable, metadata_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                row.timestamp_utc,
                row.normalized_underlying_key,
                row.event_key,
                row.comparison_metric,
                row.left_source_key,
                row.right_source_key,
                row.left_value,
                row.right_value,
                row.gross_disagreement,
                row.fees_bps,
                row.slippage_bps,
                row.execution_uncertainty_bps,
                row.net_disagreement,
                1 if row.tradeable else 0,
                json.dumps(row.metadata_json),
            ),
        )

    def persist_outcome_tracking(self, outcome: OutcomeTrackingRecord) -> int:
        return self.db.execute_insert(
            """
            INSERT INTO outcome_tracking(
                timestamp_detected, event_ticker, family, initial_value, disagreement_score,
                metadata_json, due_at_30s, due_at_60s, due_at_300s, due_at_900s,
                value_after_30s, value_after_60s, value_after_300s, value_after_900s,
                max_favorable_move, max_adverse_move, resolved_direction
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                outcome.timestamp_detected,
                outcome.event_ticker,
                outcome.family,
                outcome.initial_value,
                outcome.disagreement_score,
                json.dumps(outcome.metadata_json),
                outcome.due_at_30s,
                outcome.due_at_60s,
                outcome.due_at_300s,
                outcome.due_at_900s,
                outcome.value_after_30s,
                outcome.value_after_60s,
                outcome.value_after_300s,
                outcome.value_after_900s,
                outcome.max_favorable_move,
                outcome.max_adverse_move,
                outcome.resolved_direction,
            ),
        )

    def persist_outcome_realization(self, outcome: OutcomeRealizationRecord) -> None:
        self.db.execute(
            """
            INSERT INTO outcome_realizations(
                timestamp_utc, event_key, normalized_underlying_key, outcome_type, realized_value, metadata_json
            ) VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                outcome.timestamp_utc,
                outcome.event_key,
                outcome.normalized_underlying_key,
                outcome.outcome_type,
                outcome.realized_value,
                json.dumps(outcome.metadata_json),
            ),
        )
