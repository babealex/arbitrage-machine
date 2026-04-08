from __future__ import annotations

import json
from datetime import datetime

from app.db import Database, _json_load
from app.risk.models import RiskTransitionEvent


class RiskRepository:
    def __init__(self, db: Database) -> None:
        self.db = db

    def persist_transition(self, event: RiskTransitionEvent) -> None:
        self.db.execute(
            "INSERT INTO risk_events(state, reason, details, occurred_at) VALUES (?, ?, ?, ?)",
            (event.state, event.reason, json.dumps(event.details), event.occurred_at.isoformat()),
        )

    def load_transitions(self) -> list[RiskTransitionEvent]:
        rows = self.db.fetch_table_rows("risk_events")
        return [
            RiskTransitionEvent(
                state=str(row["state"]),
                reason=str(row["reason"]),
                details=_json_load(row["details"]),
                occurred_at=datetime.fromisoformat(str(row["occurred_at"])),
            )
            for row in rows
        ]
