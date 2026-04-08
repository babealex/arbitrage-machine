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
            "signals",
            {
                "probability_edge_decimal": "TEXT",
                "expected_edge_bps_decimal": "TEXT",
                "edge_details": "TEXT",
                "source": "TEXT",
                "confidence": "REAL",
                "priority": "INTEGER",
            },
        )
        self._ensure_columns(
            "strategy_evaluations",
            {
                "edge_before_fees_decimal": "TEXT",
                "edge_after_fees_decimal": "TEXT",
                "fee_estimate_decimal": "TEXT",
            },
        )
        self._ensure_columns(
            "execution_outcomes",
            {
                "contract_side": "TEXT",
                "requested_quantity_decimal": "TEXT",
                "filled_quantity_decimal": "TEXT",
            },
        )
        self._ensure_columns(
            "pnl_snapshots",
            {
                "equity_decimal": "TEXT",
                "cash_decimal": "TEXT",
                "daily_pnl_decimal": "TEXT",
            },
        )
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
                "venue": "TEXT",
                "contract_side": "TEXT",
                "quantity_decimal": "TEXT",
                "limit_price_decimal": "TEXT",
                "order_type": "TEXT",
                "strategy_name": "TEXT",
                "signal_ref": "TEXT",
                "updated_at": "TEXT",
            },
        )
        self._ensure_columns(
            "order_events",
            {
                "venue": "TEXT",
                "contract_side": "TEXT",
                "quantity_decimal": "TEXT",
                "limit_price_decimal": "TEXT",
                "order_type": "TEXT",
                "strategy_name": "TEXT",
                "signal_ref": "TEXT",
            },
        )
        self._ensure_columns(
            "option_structure_selections",
            {
                "regime_name": "TEXT",
                "candidate_count": "INTEGER",
                "due_at_1d": "TEXT",
                "due_at_3d": "TEXT",
                "due_at_expiry": "TEXT",
                "realized_return_decimal": "TEXT",
                "realized_vol_decimal": "TEXT",
                "realized_iv_change_decimal": "TEXT",
                "realized_pnl_decimal": "TEXT",
                "max_drawdown_realized_decimal": "TEXT",
                "prediction_error_decimal": "TEXT",
                "move_error_decimal": "TEXT",
                "vol_error_decimal": "TEXT",
                "iv_error_decimal": "TEXT",
            },
        )
        self._ensure_columns(
            "allocation_runs",
            {
                "allocated_capital_decimal": "TEXT",
                "expected_portfolio_ev_decimal": "TEXT",
                "realized_pnl_decimal": "TEXT",
            },
        )
        self._ensure_columns(
            "allocation_decisions",
            {
                "execution_price_decimal": "TEXT",
                "slippage_estimate_decimal": "TEXT",
                "fill_status": "TEXT",
            },
        )
        self._ensure_columns(
            "sleeve_trade_ledger",
            {
                "realized_edge_after_costs_decimal": "TEXT",
            },
        )
        self._ensure_columns(
            "belief_snapshots",
            {
                "source_key": "TEXT",
                "event_key": "TEXT",
                "normalized_underlying_key": "TEXT",
                "confidence": "REAL",
                "comparison_metric": "TEXT",
                "payload_summary_json": "TEXT",
                "belief_summary_json": "TEXT",
                "quality_flags_json": "TEXT",
                "cost_estimate_bps": "REAL",
            },
        )
        self._ensure_columns(
            "disagreement_snapshots",
            {
                "normalized_underlying_key": "TEXT",
                "left_source_key": "TEXT",
                "right_source_key": "TEXT",
                "gross_disagreement": "REAL",
                "cost_estimate_bps": "REAL",
                "net_disagreement": "REAL",
                "tradeable": "INTEGER NOT NULL DEFAULT 0",
            },
        )
        self._ensure_columns(
            "sleeve_runtime_status",
            {
                "run_mode": "TEXT NOT NULL DEFAULT 'data_unavailable'",
                "opportunities_seen": "INTEGER NOT NULL DEFAULT 0",
                "trades_attempted": "INTEGER NOT NULL DEFAULT 0",
                "trades_filled": "INTEGER NOT NULL DEFAULT 0",
                "no_trade_reasons": "TEXT NOT NULL DEFAULT '{}'",
            },
        )
        self._ensure_columns(
            "sleeve_metrics_snapshots",
            {
                "run_mode": "TEXT NOT NULL DEFAULT 'data_unavailable'",
                "opportunities_seen": "INTEGER NOT NULL DEFAULT 0",
                "trades_attempted": "INTEGER NOT NULL DEFAULT 0",
                "trades_filled": "INTEGER NOT NULL DEFAULT 0",
                "no_trade_reasons": "TEXT NOT NULL DEFAULT '{}'",
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
