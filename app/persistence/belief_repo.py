from __future__ import annotations

import json

from app.db import Database
from app.persistence.contracts import (
    BeliefSnapshotRecord,
    DisagreementSnapshotRecord,
    OutcomeTrackingRecord,
    OutcomeTrackingUpdate,
)


class BeliefRepository:
    def __init__(self, db: Database) -> None:
        self.db = db

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
                timestamp_utc, family, event_ticker, market_ticker, source_type, object_type,
                value, bid, ask, size_bid, size_ask, metadata_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                snapshot.timestamp_utc,
                snapshot.family,
                snapshot.event_ticker,
                snapshot.market_ticker,
                snapshot.source_type,
                snapshot.object_type,
                snapshot.value,
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
                timestamp_utc, event_group, kalshi_reference, external_reference,
                disagreement_score, zscore, metadata_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                snapshot.timestamp_utc,
                snapshot.event_group,
                snapshot.kalshi_reference,
                snapshot.external_reference,
                snapshot.disagreement_score,
                snapshot.zscore,
                json.dumps(snapshot.metadata_json),
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
