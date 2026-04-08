from __future__ import annotations

import json
from datetime import datetime, timezone
from decimal import Decimal

from app.core.money import to_decimal
from app.db import Database


class SleeveRepository:
    def __init__(self, db: Database) -> None:
        self.db = db

    def upsert_runtime_status(self, *, sleeve_name: str, maturity: str, run_mode: str, status: str, enabled: bool, paper_capable: bool, live_capable: bool, capital_weight: Decimal, capital_assigned: Decimal, opportunities_seen: int, trades_attempted: int, trades_filled: int, open_positions: int, realized_pnl: Decimal | None, unrealized_pnl: Decimal | None, fees: Decimal | None, avg_slippage_bps: Decimal | None, fill_rate: Decimal | None, trade_count: int, error_count: int, avg_expected_edge: Decimal | None, avg_realized_edge: Decimal | None, expected_realized_gap: Decimal | None, no_trade_reasons: dict[str, int], details: dict) -> None:
        timestamp = datetime.now(timezone.utc).isoformat()
        self.db.execute(
            """
            INSERT INTO sleeve_runtime_status(
                sleeve_name, maturity, run_mode, status, enabled, paper_capable, live_capable,
                capital_weight_decimal, capital_assigned_decimal, opportunities_seen, trades_attempted,
                trades_filled, open_positions, realized_pnl_decimal,
                unrealized_pnl_decimal, fees_decimal, avg_slippage_bps_decimal, fill_rate_decimal,
                trade_count, error_count, avg_expected_edge_decimal, avg_realized_edge_decimal,
                expected_realized_gap_decimal, no_trade_reasons, details, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(sleeve_name) DO UPDATE SET
                maturity=excluded.maturity,
                run_mode=excluded.run_mode,
                status=excluded.status,
                enabled=excluded.enabled,
                paper_capable=excluded.paper_capable,
                live_capable=excluded.live_capable,
                capital_weight_decimal=excluded.capital_weight_decimal,
                capital_assigned_decimal=excluded.capital_assigned_decimal,
                opportunities_seen=excluded.opportunities_seen,
                trades_attempted=excluded.trades_attempted,
                trades_filled=excluded.trades_filled,
                open_positions=excluded.open_positions,
                realized_pnl_decimal=excluded.realized_pnl_decimal,
                unrealized_pnl_decimal=excluded.unrealized_pnl_decimal,
                fees_decimal=excluded.fees_decimal,
                avg_slippage_bps_decimal=excluded.avg_slippage_bps_decimal,
                fill_rate_decimal=excluded.fill_rate_decimal,
                trade_count=excluded.trade_count,
                error_count=excluded.error_count,
                avg_expected_edge_decimal=excluded.avg_expected_edge_decimal,
                avg_realized_edge_decimal=excluded.avg_realized_edge_decimal,
                expected_realized_gap_decimal=excluded.expected_realized_gap_decimal,
                no_trade_reasons=excluded.no_trade_reasons,
                details=excluded.details,
                updated_at=excluded.updated_at
            """,
            (
                sleeve_name,
                maturity,
                run_mode,
                status,
                1 if enabled else 0,
                1 if paper_capable else 0,
                1 if live_capable else 0,
                self._fmt(capital_weight),
                self._fmt(capital_assigned),
                opportunities_seen,
                trades_attempted,
                trades_filled,
                open_positions,
                self._fmt_opt(realized_pnl),
                self._fmt_opt(unrealized_pnl),
                self._fmt_opt(fees),
                self._fmt_opt(avg_slippage_bps),
                self._fmt_opt(fill_rate),
                trade_count,
                error_count,
                self._fmt_opt(avg_expected_edge),
                self._fmt_opt(avg_realized_edge),
                self._fmt_opt(expected_realized_gap),
                json.dumps(no_trade_reasons),
                json.dumps(details),
                timestamp,
            ),
        )

    def append_metrics_snapshot(self, *, sleeve_name: str, run_mode: str, status: str, capital_assigned: Decimal, opportunities_seen: int, trades_attempted: int, trades_filled: int, open_positions: int, realized_pnl: Decimal | None, unrealized_pnl: Decimal | None, fees: Decimal | None, avg_slippage_bps: Decimal | None, fill_rate: Decimal | None, trade_count: int, error_count: int, avg_expected_edge: Decimal | None, avg_realized_edge: Decimal | None, expected_realized_gap: Decimal | None, no_trade_reasons: dict[str, int], details: dict) -> None:
        self.db.execute(
            """
            INSERT INTO sleeve_metrics_snapshots(
                sleeve_name, run_mode, status, capital_assigned_decimal, opportunities_seen,
                trades_attempted, trades_filled, open_positions, realized_pnl_decimal,
                unrealized_pnl_decimal, fees_decimal, avg_slippage_bps_decimal, fill_rate_decimal,
                trade_count, error_count, avg_expected_edge_decimal, avg_realized_edge_decimal,
                expected_realized_gap_decimal, no_trade_reasons, details, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                sleeve_name,
                run_mode,
                status,
                self._fmt(capital_assigned),
                opportunities_seen,
                trades_attempted,
                trades_filled,
                open_positions,
                self._fmt_opt(realized_pnl),
                self._fmt_opt(unrealized_pnl),
                self._fmt_opt(fees),
                self._fmt_opt(avg_slippage_bps),
                self._fmt_opt(fill_rate),
                trade_count,
                error_count,
                self._fmt_opt(avg_expected_edge),
                self._fmt_opt(avg_realized_edge),
                self._fmt_opt(expected_realized_gap),
                json.dumps(no_trade_reasons),
                json.dumps(details),
                datetime.now(timezone.utc).isoformat(),
            ),
        )

    def persist_trade_ledger_row(self, row: dict[str, object]) -> None:
        self.db.execute(
            """
            INSERT OR REPLACE INTO sleeve_trade_ledger(
                trade_id, sleeve_name, timestamp, instrument_or_market, signal_type, strategy_name, venue,
                expected_edge_decimal, expected_edge_after_costs_decimal, bid_decimal, ask_decimal,
                decision_price_reference_decimal, order_type, intended_size_decimal, filled_size_decimal,
                fill_price_decimal, fill_status, fees_decimal, slippage_estimate_bps_decimal, realized_edge_after_costs_decimal,
                realized_pnl_decimal, notes_or_error_code, order_id
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                row["trade_id"],
                row["sleeve_name"],
                row["timestamp"],
                row["instrument_or_market"],
                row.get("signal_type"),
                row.get("strategy_name"),
                row.get("venue"),
                row.get("expected_edge_decimal"),
                row.get("expected_edge_after_costs_decimal"),
                row.get("bid_decimal"),
                row.get("ask_decimal"),
                row.get("decision_price_reference_decimal"),
                row.get("order_type"),
                row.get("intended_size_decimal"),
                row.get("filled_size_decimal"),
                row.get("fill_price_decimal"),
                row.get("fill_status"),
                row.get("fees_decimal"),
                row.get("slippage_estimate_bps_decimal"),
                row.get("realized_edge_after_costs_decimal"),
                row.get("realized_pnl_decimal"),
                row.get("notes_or_error_code"),
                row.get("order_id"),
            ),
        )

    def record_error(self, *, sleeve_name: str, error_code: str, message: str, details: dict) -> None:
        self.db.execute(
            """
            INSERT INTO sleeve_error_events(sleeve_name, error_code, message, details, created_at)
            VALUES (?, ?, ?, ?, ?)
            """,
            (
                sleeve_name,
                error_code,
                message,
                json.dumps(details),
                datetime.now(timezone.utc).isoformat(),
            ),
        )

    def load_runtime_statuses(self) -> list[dict]:
        return [dict(row) for row in self.db.fetch_table_rows("sleeve_runtime_status")]

    def load_recent_metrics(self, sleeve_name: str, limit: int = 200) -> list[dict]:
        rows = self.db.fetchall(
            """
            SELECT * FROM sleeve_metrics_snapshots
            WHERE sleeve_name = ?
            ORDER BY id DESC
            LIMIT ?
            """,
            (sleeve_name, limit),
        )
        return [dict(row) for row in reversed(rows)]

    def load_trade_ledger(self, *, sleeve_name: str | None = None, limit: int = 500) -> list[dict]:
        if sleeve_name:
            rows = self.db.fetchall(
                """
                SELECT * FROM sleeve_trade_ledger
                WHERE sleeve_name = ?
                ORDER BY timestamp DESC
                LIMIT ?
                """,
                (sleeve_name, limit),
            )
        else:
            rows = self.db.fetchall(
                """
                SELECT * FROM sleeve_trade_ledger
                ORDER BY timestamp DESC
                LIMIT ?
                """,
                (limit,),
            )
        return [dict(row) for row in rows]

    def load_recent_errors(self, *, sleeve_name: str | None = None, limit: int = 100) -> list[dict]:
        if sleeve_name:
            rows = self.db.fetchall(
                """
                SELECT * FROM sleeve_error_events
                WHERE sleeve_name = ?
                ORDER BY id DESC
                LIMIT ?
                """,
                (sleeve_name, limit),
            )
        else:
            rows = self.db.fetchall(
                """
                SELECT * FROM sleeve_error_events
                ORDER BY id DESC
                LIMIT ?
                """,
                (limit,),
            )
        return [dict(row) for row in rows]

    @staticmethod
    def _fmt(value: Decimal | str | int | float) -> str:
        return format(to_decimal(value), "f")

    @staticmethod
    def _fmt_opt(value: Decimal | str | int | float | None) -> str | None:
        if value is None:
            return None
        return format(to_decimal(value), "f")
