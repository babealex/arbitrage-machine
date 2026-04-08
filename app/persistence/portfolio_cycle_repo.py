from __future__ import annotations

import json
from datetime import datetime

from app.core.money import to_decimal
from app.db import Database, _json_load
from app.persistence.contracts import PortfolioCycleSnapshotRecord


class PortfolioCycleRepository:
    def __init__(self, db: Database) -> None:
        self.db = db

    def persist_snapshot(self, record: PortfolioCycleSnapshotRecord) -> None:
        self.db.execute(
            """
            INSERT OR REPLACE INTO portfolio_cycle_snapshots(
                run_id, observed_at, candidate_count, approved_count, rejected_count,
                allocated_capital_decimal, expected_portfolio_ev_decimal,
                strategy_counts_json, rejection_counts_json, readiness_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                record.run_id,
                record.observed_at,
                record.candidate_count,
                record.approved_count,
                record.rejected_count,
                record.allocated_capital_decimal,
                record.expected_portfolio_ev_decimal,
                json.dumps(record.strategy_counts_json),
                json.dumps(record.rejection_counts_json),
                json.dumps(record.readiness_json),
            ),
        )

    def load_latest_snapshot(self) -> PortfolioCycleSnapshotRecord | None:
        row = self.db.fetchone(
            """
            SELECT *
            FROM portfolio_cycle_snapshots
            ORDER BY observed_at DESC, id DESC
            LIMIT 1
            """
        )
        if row is None:
            return None
        return self._from_row(row)

    def load_snapshot(self, run_id: str) -> PortfolioCycleSnapshotRecord | None:
        row = self.db.fetchone("SELECT * FROM portfolio_cycle_snapshots WHERE run_id = ?", (run_id,))
        if row is None:
            return None
        return self._from_row(row)

    @staticmethod
    def _from_row(row) -> PortfolioCycleSnapshotRecord:
        return PortfolioCycleSnapshotRecord(
            run_id=str(row["run_id"]),
            observed_at=str(row["observed_at"]),
            candidate_count=int(row["candidate_count"]),
            approved_count=int(row["approved_count"]),
            rejected_count=int(row["rejected_count"]),
            allocated_capital_decimal=format(to_decimal(row["allocated_capital_decimal"]), "f"),
            expected_portfolio_ev_decimal=format(to_decimal(row["expected_portfolio_ev_decimal"]), "f"),
            strategy_counts_json=_json_load(row["strategy_counts_json"]),
            rejection_counts_json=_json_load(row["rejection_counts_json"]),
            readiness_json=_json_load(row["readiness_json"]),
        )
