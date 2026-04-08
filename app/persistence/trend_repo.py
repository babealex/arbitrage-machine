from __future__ import annotations

import json
from datetime import date, datetime
from decimal import Decimal

from app.core.money import to_decimal
from app.db import Database
from app.strategies.trend.models import DailyBar, TrendEquitySnapshot, TrendPosition


class TrendRepository:
    def __init__(self, db: Database) -> None:
        self.db = db

    def persist_bars(self, bars: list[DailyBar]) -> None:
        for bar in bars:
            self.db.execute(
                """
                INSERT OR REPLACE INTO daily_bars(symbol, bar_date, close, close_decimal)
                VALUES (?, ?, ?, ?)
                """,
                (bar.symbol, bar.bar_date.isoformat(), float(bar.close), self._decimal_text(bar.close)),
            )

    def load_recent_bars(self, symbol: str, limit: int) -> list[DailyBar]:
        rows = self.db.fetchall(
            """
            SELECT symbol, bar_date, close_decimal
            FROM daily_bars
            WHERE symbol = ?
            ORDER BY bar_date ASC
            """,
            (symbol,),
        )
        trimmed = rows[-limit:] if limit > 0 else rows
        return [
            DailyBar(
                symbol=str(row["symbol"]),
                bar_date=date.fromisoformat(row["bar_date"]),
                close=to_decimal(row["close_decimal"]),
            )
            for row in trimmed
        ]

    def replace_positions(self, positions: list[TrendPosition]) -> None:
        self.db.execute("DELETE FROM trend_positions")
        for position in positions:
            self.db.execute(
                """
                INSERT INTO trend_positions(symbol, quantity, average_price_cents, market_price_cents, market_value_decimal, updated_at)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    position.symbol,
                    position.quantity,
                    position.average_price_cents,
                    position.market_price_cents,
                    self._decimal_text(position.market_value),
                    position.updated_at.isoformat(),
                ),
            )

    def load_positions(self) -> list[TrendPosition]:
        return [
            TrendPosition(
                symbol=str(row["symbol"]),
                quantity=int(row["quantity"]),
                average_price_cents=int(row["average_price_cents"]),
                market_price_cents=int(row["market_price_cents"]),
                market_value=to_decimal(row["market_value_decimal"]),
                updated_at=datetime.fromisoformat(row["updated_at"]),
            )
            for row in self.db.fetch_table_rows("trend_positions")
        ]

    def persist_equity_snapshot(self, snapshot: TrendEquitySnapshot) -> None:
        payload = {
            "positions": snapshot.positions,
            "diagnostics": snapshot.diagnostics,
            "kill_switch_active": snapshot.kill_switch_active,
        }
        self.db.execute(
            """
            INSERT INTO trend_equity_snapshots(
                snapshot_date, equity_decimal, cash_decimal, gross_exposure_decimal,
                net_exposure_decimal, drawdown_decimal, trade_count, payload, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                snapshot.snapshot_date.isoformat(),
                self._decimal_text(snapshot.equity),
                self._decimal_text(snapshot.cash),
                self._decimal_text(snapshot.gross_exposure),
                self._decimal_text(snapshot.net_exposure),
                self._decimal_text(snapshot.drawdown),
                snapshot.trade_count,
                json.dumps(payload),
                snapshot.created_at.isoformat(),
            ),
        )

    def load_equity_snapshots(self) -> list[TrendEquitySnapshot]:
        snapshots: list[TrendEquitySnapshot] = []
        for row in self.db.fetch_table_rows("trend_equity_snapshots"):
            payload = json.loads(row["payload"])
            snapshots.append(
                TrendEquitySnapshot(
                    snapshot_date=date.fromisoformat(row["snapshot_date"]),
                    equity=to_decimal(row["equity_decimal"]),
                    cash=to_decimal(row["cash_decimal"]),
                    gross_exposure=to_decimal(row["gross_exposure_decimal"]),
                    net_exposure=to_decimal(row["net_exposure_decimal"]),
                    drawdown=to_decimal(row["drawdown_decimal"]),
                    trade_count=int(row["trade_count"]),
                    positions={str(symbol): int(quantity) for symbol, quantity in payload.get("positions", {}).items()},
                    diagnostics=dict(payload.get("diagnostics", {})),
                    kill_switch_active=bool(payload.get("kill_switch_active", False)),
                    created_at=datetime.fromisoformat(row["created_at"]),
                )
            )
        return snapshots

    @staticmethod
    def _decimal_text(value: Decimal | str | int | float) -> str:
        return format(to_decimal(value), "f")
