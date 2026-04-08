from __future__ import annotations

import math
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Callable

from app.events.models import BrokerFill
from app.market_sessions import is_session_boundary, is_submission_allowed
from app.market_data.macro_fetcher import fetch_symbol_price, fetch_symbol_prices


PriceFetcher = Callable[[str], float | None]


@dataclass(slots=True)
class PriceSnapshot:
    symbol: str
    price: float | None
    previous_price: float | None
    ts: datetime

    @property
    def pct_change(self) -> float | None:
        if self.price is None or self.previous_price in (None, 0):
            return None
        return (self.price / self.previous_price) - 1.0


@dataclass(slots=True)
class PaperBookLevel:
    price: float
    size: int


@dataclass(slots=True)
class PaperOrderBook:
    symbol: str
    mid_price: float
    bid_price: float
    ask_price: float
    bid_levels: list[PaperBookLevel]
    ask_levels: list[PaperBookLevel]
    ts: datetime
    source: str = "synthetic_top_of_book"

    @property
    def top_level_size(self) -> int:
        best_sizes = []
        if self.bid_levels:
            best_sizes.append(self.bid_levels[0].size)
        if self.ask_levels:
            best_sizes.append(self.ask_levels[0].size)
        return min(best_sizes) if best_sizes else 0


@dataclass(slots=True)
class PaperExecutionResult:
    fill: BrokerFill | None
    requested_quantity: int
    filled_quantity: int
    remaining_quantity: int
    status: str
    submitted_at: datetime
    order_style: str
    book: PaperOrderBook | None
    audit_events: list[dict] = field(default_factory=list)


class MarketProxyBook:
    def __init__(self) -> None:
        self._last_prices: dict[str, tuple[float, datetime]] = {}

    def snapshot(self, symbols: list[str]) -> dict[str, PriceSnapshot]:
        current = fetch_symbol_prices(symbols)
        now = datetime.now(timezone.utc)
        snapshots: dict[str, PriceSnapshot] = {}
        for symbol, price in current.items():
            previous = self._last_prices.get(symbol)
            snapshots[symbol] = PriceSnapshot(
                symbol=symbol,
                price=price,
                previous_price=previous[0] if previous else None,
                ts=now,
            )
            if price is not None:
                self._last_prices[symbol] = (price, now)
        return snapshots

    def quote(self, symbol: str) -> PriceSnapshot:
        now = datetime.now(timezone.utc)
        price = fetch_symbol_price(symbol)
        previous = self._last_prices.get(symbol)
        if price is not None:
            self._last_prices[symbol] = (price, now)
        return PriceSnapshot(symbol=symbol, price=price, previous_price=previous[0] if previous else None, ts=now)


class PaperBroker:
    """Conservative paper broker.

    The broker never fills at mid. Marketable buy orders consume ask liquidity,
    marketable sell orders consume bid liquidity, and fills are capped by
    displayed synthetic depth. Passive orders only fill if a later recheck shows
    the limit has been crossed; otherwise they remain unfilled.
    """

    def __init__(
        self,
        starting_capital: float,
        execution_enabled: bool,
        *,
        submission_latency_seconds: float = 2.0,
        half_spread_bps: float = 8.0,
        top_level_size: int = 25,
        depth_levels: int = 3,
        passive_recheck_seconds: float = 1.0,
        session_boundary_buffer_seconds: int = 300,
        session_boundary_liquidity_factor: float = 0.25,
        session_boundary_spread_multiplier: float = 2.0,
        price_fetcher: PriceFetcher | None = None,
    ) -> None:
        self.cash = starting_capital
        self.execution_enabled = execution_enabled
        self.submission_latency_seconds = max(0.0, submission_latency_seconds)
        self.half_spread_bps = max(0.0, half_spread_bps)
        self.top_level_size = max(1, top_level_size)
        self.depth_levels = max(1, depth_levels)
        self.passive_recheck_seconds = max(0.1, passive_recheck_seconds)
        self.session_boundary_buffer_seconds = max(0, session_boundary_buffer_seconds)
        self.session_boundary_liquidity_factor = max(0.0, min(1.0, session_boundary_liquidity_factor))
        self.session_boundary_spread_multiplier = max(1.0, session_boundary_spread_multiplier)
        self._price_fetcher = price_fetcher or fetch_symbol_price

    def place_order(
        self,
        symbol: str,
        side: str,
        quantity: int,
        *,
        signal_ts: datetime | None = None,
        order_style: str = "marketable",
        limit_price: float | None = None,
    ) -> PaperExecutionResult:
        submitted_at = self._submitted_at(signal_ts)
        audit_events: list[dict] = [
            {
                "timestamp_utc": submitted_at.isoformat(),
                "state": "submitted",
                "decision": "submit_order",
                "symbol": symbol,
                "side": side,
                "requested_quantity": quantity,
                "order_style": order_style,
                "limit_price": limit_price,
                "submission_latency_seconds": self.submission_latency_seconds,
            }
        ]
        if not self.execution_enabled or quantity <= 0:
            reason = "execution_disabled" if not self.execution_enabled else "non_positive_quantity"
            audit_events.append(
                {
                    "timestamp_utc": submitted_at.isoformat(),
                    "state": "rejected",
                    "decision": "reject_order",
                    "reason": reason,
                }
            )
            return PaperExecutionResult(
                fill=None,
                requested_quantity=quantity,
                filled_quantity=0,
                remaining_quantity=max(0, quantity),
                status="rejected",
                submitted_at=submitted_at,
                order_style=order_style,
                book=None,
                audit_events=audit_events,
            )
        if not is_submission_allowed(symbol, submitted_at):
            audit_events.append(
                {
                    "timestamp_utc": submitted_at.isoformat(),
                    "state": "rejected",
                    "decision": "reject_order",
                    "reason": "market_closed",
                }
            )
            return PaperExecutionResult(
                fill=None,
                requested_quantity=quantity,
                filled_quantity=0,
                remaining_quantity=quantity,
                status="rejected",
                submitted_at=submitted_at,
                order_style=order_style,
                book=None,
                audit_events=audit_events,
            )
        book = self._build_book(symbol, submitted_at)
        if book is None:
            audit_events.append(
                {
                    "timestamp_utc": submitted_at.isoformat(),
                    "state": "rejected",
                    "decision": "reject_order",
                    "reason": "price_unavailable",
                }
            )
            return PaperExecutionResult(
                fill=None,
                requested_quantity=quantity,
                filled_quantity=0,
                remaining_quantity=quantity,
                status="rejected",
                submitted_at=submitted_at,
                order_style=order_style,
                book=None,
                audit_events=audit_events,
            )
        if order_style == "passive":
            return self._simulate_passive(symbol, side, quantity, submitted_at, limit_price, book, audit_events)
        return self._simulate_marketable(symbol, side, quantity, submitted_at, book, audit_events)

    def submitted_at_for(self, signal_ts: datetime | None = None) -> datetime:
        return self._submitted_at(signal_ts)

    def snapshot_order_book(self, symbol: str, signal_ts: datetime | None = None) -> PaperOrderBook | None:
        return self._build_book(symbol, self._submitted_at(signal_ts))

    def get_price(self, symbol: str, *, liquidation_side: str | None = None) -> float | None:
        book = self._build_book(symbol, datetime.now(timezone.utc))
        if book is None:
            return None
        if liquidation_side == "buy":
            return book.ask_price
        if liquidation_side == "sell":
            return book.bid_price
        return book.mid_price

    def planned_exit_at(self, max_hold_seconds: int) -> datetime:
        return datetime.now(timezone.utc) + timedelta(seconds=max_hold_seconds)

    def _simulate_marketable(
        self,
        symbol: str,
        side: str,
        quantity: int,
        submitted_at: datetime,
        book: PaperOrderBook,
        audit_events: list[dict],
    ) -> PaperExecutionResult:
        levels = book.ask_levels if side == "buy" else book.bid_levels
        fill_levels: list[tuple[float, int]] = []
        remaining = quantity
        for index, level in enumerate(levels, start=1):
            if remaining <= 0:
                break
            executed_here = min(remaining, level.size)
            if executed_here <= 0:
                continue
            remaining -= executed_here
            fill_levels.append((level.price, executed_here))
            audit_events.append(
                {
                    "timestamp_utc": submitted_at.isoformat(),
                    "state": "fill_level",
                    "decision": "consume_displayed_liquidity",
                    "level_index": index,
                    "price": level.price,
                    "displayed_size": level.size,
                    "executed_quantity": executed_here,
                    "remaining_quantity": remaining,
                }
            )
        filled_quantity = quantity - remaining
        if filled_quantity <= 0:
            audit_events.append(
                {
                    "timestamp_utc": submitted_at.isoformat(),
                    "state": "unfilled",
                    "decision": "leave_unfilled",
                    "reason": "no_displayed_liquidity",
                }
            )
            return PaperExecutionResult(
                fill=None,
                requested_quantity=quantity,
                filled_quantity=0,
                remaining_quantity=quantity,
                status="unfilled",
                submitted_at=submitted_at,
                order_style="marketable",
                book=book,
                audit_events=audit_events,
            )
        weighted_notional = math.fsum(price * size for price, size in fill_levels)
        avg_price = round(weighted_notional / filled_quantity, 6)
        gross_cash = avg_price * filled_quantity
        if side == "buy":
            self.cash -= gross_cash
        else:
            self.cash += gross_cash
        status = "filled" if remaining == 0 else "partial_fill"
        audit_events.append(
            {
                "timestamp_utc": submitted_at.isoformat(),
                "state": status,
                "decision": "finalize_marketable_fill",
                "average_fill_price": avg_price,
                "filled_quantity": filled_quantity,
                "remaining_quantity": remaining,
                "best_bid": book.bid_price,
                "best_ask": book.ask_price,
                "mid_price": book.mid_price,
            }
        )
        fill = BrokerFill(
            symbol=symbol,
            side=side,
            quantity=filled_quantity,
            price=avg_price,
            timestamp_utc=submitted_at,
            metadata={
                "requested_quantity": quantity,
                "remaining_quantity": remaining,
                "order_style": "marketable",
                "best_bid": book.bid_price,
                "best_ask": book.ask_price,
                "mid_price": book.mid_price,
                "fill_levels": [
                    {"price": price, "quantity": size}
                    for price, size in fill_levels
                ],
                "book_source": book.source,
            },
        )
        return PaperExecutionResult(
            fill=fill,
            requested_quantity=quantity,
            filled_quantity=filled_quantity,
            remaining_quantity=remaining,
            status=status,
            submitted_at=submitted_at,
            order_style="marketable",
            book=book,
            audit_events=audit_events,
        )

    def _simulate_passive(
        self,
        symbol: str,
        side: str,
        quantity: int,
        submitted_at: datetime,
        limit_price: float | None,
        book: PaperOrderBook,
        audit_events: list[dict],
    ) -> PaperExecutionResult:
        if limit_price is None:
            audit_events.append(
                {
                    "timestamp_utc": submitted_at.isoformat(),
                    "state": "unfilled",
                    "decision": "leave_unfilled",
                    "reason": "passive_order_requires_limit",
                }
            )
            return PaperExecutionResult(None, quantity, 0, quantity, "unfilled", submitted_at, "passive", book, audit_events)
        crosses_on_submit = (side == "buy" and limit_price >= book.ask_price) or (side == "sell" and limit_price <= book.bid_price)
        if crosses_on_submit:
            audit_events.append(
                {
                    "timestamp_utc": submitted_at.isoformat(),
                    "state": "passive_crossed_on_submit",
                    "decision": "treat_as_marketable",
                    "reason": "limit_crossed_displayed_book",
                }
            )
            return self._simulate_marketable(symbol, side, quantity, submitted_at, book, audit_events)
        recheck_ts = submitted_at + timedelta(seconds=self.passive_recheck_seconds)
        recheck_book = self._build_book(symbol, recheck_ts)
        if recheck_book is None:
            audit_events.append(
                {
                    "timestamp_utc": recheck_ts.isoformat(),
                    "state": "unfilled",
                    "decision": "leave_unfilled",
                    "reason": "recheck_price_unavailable",
                }
            )
            return PaperExecutionResult(None, quantity, 0, quantity, "unfilled", submitted_at, "passive", book, audit_events)
        crosses_later = (side == "buy" and recheck_book.ask_price <= limit_price) or (side == "sell" and recheck_book.bid_price >= limit_price)
        if not crosses_later:
            audit_events.append(
                {
                    "timestamp_utc": recheck_ts.isoformat(),
                    "state": "unfilled",
                    "decision": "leave_unfilled",
                    "reason": "passive_not_crossed",
                    "recheck_best_bid": recheck_book.bid_price,
                    "recheck_best_ask": recheck_book.ask_price,
                }
            )
            return PaperExecutionResult(None, quantity, 0, quantity, "unfilled", submitted_at, "passive", book, audit_events)
        audit_events.append(
            {
                "timestamp_utc": recheck_ts.isoformat(),
                "state": "passive_crossed_on_recheck",
                "decision": "fill_after_recheck_cross",
                "reason": "later_book_crossed_limit",
            }
        )
        return self._simulate_marketable(symbol, side, quantity, recheck_ts, recheck_book, audit_events)

    def _submitted_at(self, signal_ts: datetime | None) -> datetime:
        base = signal_ts.astimezone(timezone.utc) if signal_ts is not None else datetime.now(timezone.utc)
        return base + timedelta(seconds=self.submission_latency_seconds)

    def _build_book(self, symbol: str, ts: datetime) -> PaperOrderBook | None:
        mid = self._price_fetcher(symbol)
        if mid is None or mid <= 0:
            return None
        boundary_flag = is_session_boundary(symbol, ts, self.session_boundary_buffer_seconds)
        half_spread_fraction = self.half_spread_bps / 10_000.0
        if boundary_flag:
            half_spread_fraction *= self.session_boundary_spread_multiplier
        best_bid = round(mid * max(0.0, 1.0 - half_spread_fraction), 6)
        best_ask = round(mid * (1.0 + half_spread_fraction), 6)
        bid_levels: list[PaperBookLevel] = []
        ask_levels: list[PaperBookLevel] = []
        for level_index in range(self.depth_levels):
            distance = half_spread_fraction * (level_index + 1)
            level_size = max(1, int(round(self.top_level_size / (level_index + 1))))
            if boundary_flag:
                level_size = max(1, int(round(level_size * self.session_boundary_liquidity_factor)))
            bid_levels.append(
                PaperBookLevel(
                    price=round(mid * max(0.0, 1.0 - distance), 6),
                    size=level_size,
                )
            )
            ask_levels.append(
                PaperBookLevel(
                    price=round(mid * (1.0 + distance), 6),
                    size=level_size,
                )
            )
        return PaperOrderBook(
            symbol=symbol,
            mid_price=round(mid, 6),
            bid_price=best_bid,
            ask_price=best_ask,
            bid_levels=bid_levels,
            ask_levels=ask_levels,
            ts=ts,
            source="synthetic_top_of_book_boundary_adjusted" if boundary_flag else "synthetic_top_of_book",
        )
