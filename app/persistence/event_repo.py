from __future__ import annotations

import json

from app.db import Database, _json_load
from app.persistence.contracts import (
    ClassifiedEventRecord,
    EventEntryQueueRecord,
    EventPositionRecord,
    EventTradeOutcomeRecord,
    NewsEventRecord,
    TradeCandidateRecord,
)


class EventRepository:
    def __init__(self, db: Database) -> None:
        self.db = db

    def news_event_exists(self, external_id: str) -> bool:
        row = self.db.fetchone(
            "SELECT 1 FROM news_events WHERE external_id = ? LIMIT 1",
            (external_id,),
        )
        return row is not None

    def persist_news_event(self, item: NewsEventRecord) -> int:
        return self.db.execute_insert(
            """
            INSERT INTO news_events(external_id, timestamp_utc, source, headline, body, url, metadata_json)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                item.external_id,
                item.timestamp_utc,
                item.source,
                item.headline,
                item.body,
                item.url,
                json.dumps(item.metadata_json),
            ),
        )

    def persist_classified_event(self, item: ClassifiedEventRecord) -> int:
        return self.db.execute_insert(
            """
            INSERT INTO classified_events(
                news_event_id, event_type, region, severity, directional_bias,
                assets_affected_json, metadata_json, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                item.news_event_id,
                item.event_type,
                item.region,
                item.severity,
                item.directional_bias,
                json.dumps(item.assets_affected),
                json.dumps(item.metadata_json),
                item.created_at,
            ),
        )

    def persist_trade_candidate(self, item: TradeCandidateRecord) -> int:
        return self.db.execute_insert(
            """
            INSERT INTO trade_candidates(
                classified_event_id, symbol, side, event_type, status,
                confidence, confirmation_score, metadata_json, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                item.classified_event_id,
                item.symbol,
                item.side,
                item.event_type,
                item.status,
                item.confidence,
                item.confirmation_score,
                json.dumps(item.metadata_json),
                item.created_at,
            ),
        )

    def persist_entry_queue_item(self, item: EventEntryQueueRecord) -> int:
        return self.db.execute_insert(
            """
            INSERT INTO event_entry_queue(
                trade_candidate_id, due_at, symbol, side, quantity, tranche_index,
                total_tranches, status, metadata_json, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                item.trade_candidate_id,
                item.due_at,
                item.symbol,
                item.side,
                item.quantity,
                item.tranche_index,
                item.total_tranches,
                item.status,
                json.dumps(item.metadata_json),
                item.created_at,
            ),
        )

    def due_event_entries(self, now_iso: str):
        return self.db.fetchall(
            """
            SELECT * FROM event_entry_queue
            WHERE status = 'pending' AND due_at <= ?
            ORDER BY due_at ASC, id ASC
            """,
            (now_iso,),
        )

    def update_event_entry_status(self, row_id: int, status: str, metadata: dict | None = None) -> None:
        row = self.db.fetchone(
            "SELECT metadata_json FROM event_entry_queue WHERE id = ?",
            (row_id,),
        )
        current_metadata = _json_load(row["metadata_json"]) if row else {}
        if metadata:
            current_metadata.update(metadata)
        self.db.execute(
            "UPDATE event_entry_queue SET status = ?, metadata_json = ? WHERE id = ?",
            (status, json.dumps(current_metadata), row_id),
        )

    def persist_event_position(self, item: EventPositionRecord) -> int:
        return self.db.execute_insert(
            """
            INSERT INTO event_positions(
                trade_candidate_id, news_event_id, symbol, side, quantity,
                entry_price, stop_price, opened_at, planned_exit_at, status, metadata_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                item.trade_candidate_id,
                item.news_event_id,
                item.symbol,
                item.side,
                item.quantity,
                item.entry_price,
                item.stop_price,
                item.opened_at,
                item.planned_exit_at,
                item.status,
                json.dumps(item.metadata_json),
            ),
        )

    def open_event_positions(self):
        return self.db.fetchall(
            "SELECT * FROM event_positions WHERE status = 'open' ORDER BY opened_at ASC, id ASC"
        )

    def close_event_position(self, row_id: int, status: str, metadata: dict | None = None) -> None:
        row = self.db.fetchone(
            "SELECT metadata_json FROM event_positions WHERE id = ?",
            (row_id,),
        )
        current_metadata = _json_load(row["metadata_json"]) if row else {}
        if metadata:
            current_metadata.update(metadata)
        self.db.execute(
            "UPDATE event_positions SET status = ?, metadata_json = ? WHERE id = ?",
            (status, json.dumps(current_metadata), row_id),
        )

    def update_event_position_quantity(self, row_id: int, quantity: int, metadata: dict | None = None) -> None:
        row = self.db.fetchone(
            "SELECT metadata_json FROM event_positions WHERE id = ?",
            (row_id,),
        )
        current_metadata = _json_load(row["metadata_json"]) if row else {}
        if metadata:
            current_metadata.update(metadata)
        self.db.execute(
            "UPDATE event_positions SET quantity = ?, metadata_json = ? WHERE id = ?",
            (quantity, json.dumps(current_metadata), row_id),
        )

    def persist_event_trade_outcome(self, item: EventTradeOutcomeRecord) -> int:
        return self.db.execute_insert(
            """
            INSERT INTO event_trade_outcomes(
                position_id, symbol, entry_price, exit_price, return_pct,
                holding_seconds, exit_reason, metadata_json, closed_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                item.position_id,
                item.symbol,
                item.entry_price,
                item.exit_price,
                item.return_pct,
                item.holding_seconds,
                item.exit_reason,
                json.dumps(item.metadata_json),
                item.closed_at,
            ),
        )

    def load_recent_trade_candidates(self, *, lookback_hours: int = 72) -> list[dict]:
        rows = self.db.fetchall(
            """
            SELECT tc.*, ce.directional_bias, ce.severity, ce.metadata_json AS classified_metadata_json
            FROM trade_candidates tc
            LEFT JOIN classified_events ce ON ce.id = tc.classified_event_id
            WHERE tc.created_at >= datetime('now', ?)
            ORDER BY tc.created_at DESC, tc.id DESC
            """,
            (f"-{int(lookback_hours)} hours",),
        )
        return [dict(row) for row in rows]
