from __future__ import annotations

import json
from datetime import datetime

from app.brokers.models import BrokerFill, BrokerOrder, BrokerOrderAck, BrokerReadinessResult, ReconciliationResult
from app.core.money import to_decimal
from app.db import Database, _json_load


class TrendBrokerRepository:
    def __init__(self, db: Database) -> None:
        self.db = db

    def persist_readiness_check(self, result: BrokerReadinessResult) -> None:
        self.db.execute(
            """
            INSERT INTO trend_broker_readiness_checks(
                broker_name, checked_at, is_ready, reasons_json,
                last_successful_auth_at, last_market_data_check_at, last_reconciliation_check_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                result.broker_name,
                result.checked_at.isoformat(),
                1 if result.is_ready else 0,
                json.dumps(result.reasons),
                None if result.last_successful_auth_at is None else result.last_successful_auth_at.isoformat(),
                None if result.last_market_data_check_at is None else result.last_market_data_check_at.isoformat(),
                None if result.last_reconciliation_check_at is None else result.last_reconciliation_check_at.isoformat(),
            ),
        )

    def persist_session_event(self, *, broker_name: str, event_type: str, status: str, reason: str | None, occurred_at: datetime) -> None:
        self.db.execute(
            """
            INSERT INTO trend_broker_session_events(broker_name, event_type, status, reason, occurred_at)
            VALUES (?, ?, ?, ?, ?)
            """,
            (broker_name, event_type, status, reason, occurred_at.isoformat()),
        )

    def persist_order_event(self, *, broker_name: str, order: BrokerOrder | None = None, ack: BrokerOrderAck | None = None) -> None:
        if order is None and ack is None:
            return
        broker_order_id = order.broker_order_id if order is not None else ack.broker_order_id
        client_order_id = order.client_order_id if order is not None else ack.client_order_id
        symbol = "" if order is None else order.symbol
        side = "" if order is None else order.side
        quantity = "0" if order is None else format(order.quantity, "f")
        order_type = "" if order is None else order.order_type
        limit_price = None if order is None or order.limit_price is None else format(order.limit_price, "f")
        tif = "" if order is None else order.time_in_force
        status = order.status if order is not None else ack.status
        reject_reason = order.reject_reason if order is not None else ack.reject_reason
        event_at = order.created_at if order is not None else ack.received_at
        self.db.execute(
            """
            INSERT INTO trend_broker_order_events(
                broker_name, broker_order_id, client_order_id, symbol, side, quantity_decimal,
                order_type, limit_price_decimal, time_in_force, status, reject_reason, event_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                broker_name,
                broker_order_id,
                client_order_id,
                symbol,
                side,
                quantity,
                order_type,
                limit_price,
                tif,
                status,
                reject_reason,
                event_at.isoformat(),
            ),
        )

    def persist_fill(self, *, broker_name: str, fill: BrokerFill) -> None:
        self.db.execute(
            """
            INSERT INTO trend_broker_fills(
                broker_name, broker_fill_id, broker_order_id, client_order_id, symbol, side,
                quantity_decimal, price_decimal, filled_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                broker_name,
                fill.broker_fill_id,
                fill.broker_order_id,
                fill.client_order_id,
                fill.symbol,
                fill.side,
                format(fill.quantity, "f"),
                format(fill.price, "f"),
                fill.filled_at.isoformat(),
            ),
        )

    def persist_reconciliation_result(self, *, broker_name: str, checked_at: datetime, result: ReconciliationResult) -> None:
        self.db.execute(
            """
            INSERT INTO trend_broker_reconciliation_runs(
                broker_name, checked_at, is_consistent, position_diff_count, open_order_diff_count,
                cash_diff_decimal, reasons_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                broker_name,
                checked_at.isoformat(),
                1 if result.is_consistent else 0,
                len(result.position_diffs),
                len(result.open_order_diffs),
                None if result.cash_diff is None else format(result.cash_diff, "f"),
                json.dumps(result.reasons),
            ),
        )

    def load_latest_readiness_check(self) -> BrokerReadinessResult | None:
        row = self.db.fetchone(
            """
            SELECT *
            FROM trend_broker_readiness_checks
            ORDER BY checked_at DESC, id DESC
            LIMIT 1
            """
        )
        if row is None:
            return None
        return BrokerReadinessResult(
            is_ready=bool(row["is_ready"]),
            reasons=list(_json_load(row["reasons_json"])),
            broker_name=str(row["broker_name"]),
            checked_at=datetime.fromisoformat(str(row["checked_at"])),
            last_successful_auth_at=None if row["last_successful_auth_at"] is None else datetime.fromisoformat(str(row["last_successful_auth_at"])),
            last_market_data_check_at=None if row["last_market_data_check_at"] is None else datetime.fromisoformat(str(row["last_market_data_check_at"])),
            last_reconciliation_check_at=None if row["last_reconciliation_check_at"] is None else datetime.fromisoformat(str(row["last_reconciliation_check_at"])),
        )

    def count_rejects(self) -> int:
        row = self.db.fetchone("SELECT COUNT(*) AS count FROM trend_broker_order_events WHERE status = 'rejected'")
        return int(row["count"] or 0) if row else 0

    def count_reconnect_events(self) -> int:
        row = self.db.fetchone("SELECT COUNT(*) AS count FROM trend_broker_session_events WHERE event_type = 'reconnect'")
        return int(row["count"] or 0) if row else 0

    def count_reconciliation_mismatches(self) -> int:
        row = self.db.fetchone("SELECT COUNT(*) AS count FROM trend_broker_reconciliation_runs WHERE is_consistent = 0")
        return int(row["count"] or 0) if row else 0

    def latest_reject_timestamp(self) -> str | None:
        row = self.db.fetchone("SELECT MAX(event_at) AS latest FROM trend_broker_order_events WHERE status = 'rejected'")
        return None if row is None or row["latest"] is None else str(row["latest"])

    def load_latest_reconciliation(self) -> ReconciliationResult | None:
        row = self.db.fetchone(
            """
            SELECT *
            FROM trend_broker_reconciliation_runs
            ORDER BY checked_at DESC, id DESC
            LIMIT 1
            """
        )
        if row is None:
            return None
        return ReconciliationResult(
            is_consistent=bool(row["is_consistent"]),
            position_diffs=[f"count:{row['position_diff_count']}"] if int(row["position_diff_count"]) else [],
            open_order_diffs=[f"count:{row['open_order_diff_count']}"] if int(row["open_order_diff_count"]) else [],
            cash_diff=None if row["cash_diff_decimal"] is None else to_decimal(row["cash_diff_decimal"]),
            reasons=list(_json_load(row["reasons_json"])),
        )
