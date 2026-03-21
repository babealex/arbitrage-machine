from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Any

from app.persistence.schema.bootstrap import schema_sql


class Database:
    def __init__(self, path: Path) -> None:
        self.path = path
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.connection = sqlite3.connect(path)
        self.connection.row_factory = sqlite3.Row
        self.connection.executescript(schema_sql())
        self._ensure_schema_upgrades()
        self.connection.commit()

    def _ensure_schema_upgrades(self) -> None:
        self._ensure_columns(
            "outcome_tracking",
            {
                "due_at_30s": "TEXT",
                "due_at_60s": "TEXT",
                "due_at_300s": "TEXT",
                "due_at_900s": "TEXT",
            },
        )
        self._ensure_columns(
            "orders",
            {
                "updated_at": "TEXT",
            },
        )

    def _ensure_columns(self, table: str, columns: dict[str, str]) -> None:
        existing = {
            row["name"]
            for row in self.connection.execute(f"PRAGMA table_info({table})").fetchall()
        }
        for name, definition in columns.items():
            if name in existing:
                continue
            self.connection.execute(f"ALTER TABLE {table} ADD COLUMN {name} {definition}")

    def execute(self, sql: str, params: tuple[Any, ...] = ()) -> None:
        self.connection.execute(sql, params)
        self.connection.commit()

    def execute_insert(self, sql: str, params: tuple[Any, ...] = ()) -> int:
        cursor = self.connection.execute(sql, params)
        self.connection.commit()
        return int(cursor.lastrowid)

    def fetchone(self, sql: str, params: tuple[Any, ...] = ()) -> sqlite3.Row | None:
        return self.connection.execute(sql, params).fetchone()

    def fetchall(self, sql: str, params: tuple[Any, ...] = ()) -> list[sqlite3.Row]:
        return self.connection.execute(sql, params).fetchall()

    def fetch_table_rows(self, table: str) -> list[sqlite3.Row]:
        cursor = self.connection.execute(f"SELECT * FROM {table}")
        return cursor.fetchall()


def _json_load(raw: str | None) -> dict[str, Any]:
    if not raw:
        return {}
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        return {}
