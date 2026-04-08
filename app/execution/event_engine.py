from __future__ import annotations

import json
import math
from datetime import datetime, timedelta, timezone

from app.core.money import to_decimal
from app.events.models import TradeIdea
from app.execution.broker import PaperBroker
from app.execution.models import SignalLegIntent
from app.execution.orders import build_order_intent
from app.execution.router import ExecutionRouter
from app.instruments.models import InstrumentRef
from app.market_data.snapshots import MarketStateInput
from app.models import Signal, SignalEdge, StrategyName
from app.persistence.contracts import EventEntryQueueRecord, EventPositionRecord, EventTradeOutcomeRecord
from app.persistence.event_repo import EventRepository
from app.persistence.kalshi_repo import KalshiRepository


class EventExecutionEngine:
    def __init__(
        self,
        repo: EventRepository,
        signal_repo: KalshiRepository,
        broker: PaperBroker,
        execution_router: ExecutionRouter,
    ) -> None:
        self.repo = repo
        self.signal_repo = signal_repo
        self.broker = broker
        self.execution_router = execution_router

    def queue_trade(self, trade_candidate_id: int, news_event_id: int, idea: TradeIdea, quantity: int) -> None:
        if quantity <= 0:
            return
        per_step = max(1, math.ceil(quantity / max(1, idea.scale_steps)))
        now = datetime.now(timezone.utc)
        for tranche_index in range(max(1, idea.scale_steps)):
            due_at = now + timedelta(seconds=tranche_index * max(0, idea.scale_interval_seconds))
            tranche_quantity = per_step if tranche_index < idea.scale_steps - 1 else max(1, quantity - per_step * tranche_index)
            metadata = dict(idea.metadata)
            metadata["news_event_id"] = news_event_id
            metadata["confidence"] = idea.confidence
            self.repo.persist_entry_queue_item(
                EventEntryQueueRecord(
                    trade_candidate_id=trade_candidate_id,
                    due_at=due_at.isoformat(),
                    symbol=idea.symbol,
                    side=idea.side,
                    quantity=tranche_quantity,
                    tranche_index=tranche_index + 1,
                    total_tranches=max(1, idea.scale_steps),
                    status="pending",
                    metadata_json=metadata,
                    created_at=now.isoformat(),
                )
            )

    def process_due_entries(self, stop_loss_pct: float, max_hold_seconds: int, logger, now: datetime | None = None) -> int:
        processed = 0
        effective_now = now or datetime.now(timezone.utc)
        now_iso = effective_now.isoformat()
        for row in self.repo.due_event_entries(now_iso):
            parsed_metadata = _parse_metadata(row["metadata_json"])
            due_at = datetime.fromisoformat(str(row["due_at"]))
            signal = self._build_signal(
                symbol=str(row["symbol"]),
                side=str(row["side"]),
                quantity=int(row["quantity"]),
                order_type=_normalize_order_type(str(parsed_metadata.get("paper_order_style", "marketable"))),
                limit_price=parsed_metadata.get("paper_limit_price"),
                source="event_trade_mapper",
                confidence=float(parsed_metadata.get("confidence", 0.0)),
                timestamp=due_at,
                action_prefix="event_entry",
            )
            self.signal_repo.persist_signal(signal)
            signal_ref = f"{signal.strategy.value}:{signal.ticker}:{signal.ts.isoformat()}"
            intent = build_order_intent(
                instrument=InstrumentRef(symbol=str(row["symbol"]), venue="paper_equity"),
                side=str(row["side"]),
                quantity=int(row["quantity"]),
                order_type=_normalize_order_type(str(parsed_metadata.get("paper_order_style", "marketable"))),
                strategy_name=signal.strategy.value,
                signal_ref=signal_ref,
                seed=f"event-entry:{row['id']}:{row['symbol']}:{row['side']}:{row['tranche_index']}",
                limit_price=parsed_metadata.get("paper_limit_price"),
                time_in_force="DAY",
                created_at=signal.ts,
            )
            book = self.broker.snapshot_order_book(str(row["symbol"]), due_at)
            success, order_outcomes = self.execution_router.execute_order_intents(
                [intent],
                source_metadata={
                    "signal": signal.strategy.value,
                    "signal_ref": signal_ref,
                    "trade_candidate_id": int(row["trade_candidate_id"]),
                    "news_event_id": int(parsed_metadata.get("news_event_id", 0)),
                },
                market_prices={} if book is None else {str(row["symbol"]): to_decimal(book.ask_price if row["side"] == "buy" else book.bid_price)},
                market_state_inputs=[] if book is None else [self._market_state_from_book(book, str(row["side"]), intent.order_id)],
            )
            outcome = order_outcomes[0]
            if not success or outcome.filled_quantity <= 0 or outcome.price_cents is None:
                self.repo.update_event_entry_status(
                    int(row["id"]),
                    "failed" if outcome.status == "rejected" else "unfilled",
                    {
                        "failure_reason": outcome.status,
                        "broker_status": outcome.status,
                        "submitted_at": outcome.created_at.isoformat(),
                        "order_id": intent.order_id,
                    },
                )
                logger.info(
                    "event_order_failed",
                    extra={"event": {"symbol": row["symbol"], "queue_id": row["id"], "reason": outcome.status, "status": outcome.status}},
                )
                continue
            entry_status = "filled" if outcome.filled_quantity >= int(row["quantity"]) else "partially_filled"
            fill_price = outcome.price_cents / 100.0
            remaining_quantity = max(0, int(row["quantity"]) - outcome.filled_quantity)
            self.repo.update_event_entry_status(
                int(row["id"]),
                entry_status,
                {
                    "fill_price": fill_price,
                    "filled_at": outcome.created_at.isoformat(),
                    "filled_quantity": outcome.filled_quantity,
                    "remaining_quantity": remaining_quantity,
                    "broker_status": outcome.status,
                    "order_id": intent.order_id,
                },
            )
            self.repo.persist_event_position(
                EventPositionRecord(
                    trade_candidate_id=int(row["trade_candidate_id"]),
                    news_event_id=int(parsed_metadata.get("news_event_id", 0)),
                    symbol=str(row["symbol"]),
                    side=str(row["side"]),
                    quantity=outcome.filled_quantity,
                    entry_price=fill_price,
                    stop_price=_stop_price(fill_price, str(row["side"]), stop_loss_pct),
                    opened_at=outcome.created_at.isoformat(),
                    planned_exit_at=(outcome.created_at + timedelta(seconds=max_hold_seconds)).isoformat(),
                    status="open",
                    metadata_json={
                        "queue_id": int(row["id"]),
                        "entry_order_id": intent.order_id,
                        "tranche_index": int(row["tranche_index"]),
                        "total_tranches": int(row["total_tranches"]),
                        "trade_metadata": parsed_metadata,
                        "requested_quantity": int(row["quantity"]),
                        "filled_quantity": outcome.filled_quantity,
                        "remaining_quantity": remaining_quantity,
                    },
                )
            )
            logger.info(
                "event_order_filled",
                extra={
                    "event": {
                        "symbol": row["symbol"],
                        "side": row["side"],
                        "requested_quantity": row["quantity"],
                        "filled_quantity": outcome.filled_quantity,
                        "remaining_quantity": remaining_quantity,
                        "price": fill_price,
                        "queue_id": row["id"],
                        "status": outcome.status,
                    }
                },
            )
            processed += 1
        return processed

    def close_due_positions(self, logger, now: datetime | None = None) -> int:
        closed = 0
        effective_now = now or datetime.now(timezone.utc)
        for row in self.repo.open_event_positions():
            liquidation_side = "sell" if row["side"] == "buy" else "buy"
            book = self.broker.snapshot_order_book(str(row["symbol"]), effective_now)
            current_price = None
            if book is not None:
                current_price = book.ask_price if liquidation_side == "buy" else book.bid_price
            planned_exit_at = datetime.fromisoformat(str(row["planned_exit_at"]))
            should_exit_for_time = planned_exit_at <= effective_now
            if current_price is None:
                continue
            stop_hit = (row["side"] == "buy" and current_price <= float(row["stop_price"])) or (
                row["side"] == "sell" and current_price >= float(row["stop_price"])
            )
            if not stop_hit and not should_exit_for_time:
                continue
            signal = self._build_signal(
                symbol=str(row["symbol"]),
                side=liquidation_side,
                quantity=int(row["quantity"]),
                order_type="market",
                limit_price=None,
                source="event_position_exit",
                confidence=1.0,
                timestamp=effective_now,
                action_prefix="event_exit",
            )
            self.signal_repo.persist_signal(signal)
            signal_ref = f"{signal.strategy.value}:{signal.ticker}:{signal.ts.isoformat()}"
            intent = build_order_intent(
                instrument=InstrumentRef(symbol=str(row["symbol"]), venue="paper_equity"),
                side=liquidation_side,
                quantity=int(row["quantity"]),
                order_type="market",
                strategy_name=signal.strategy.value,
                signal_ref=signal_ref,
                seed=f"event-exit:{row['id']}:{row['symbol']}:{liquidation_side}",
                limit_price=None,
                time_in_force="DAY",
                created_at=signal.ts,
            )
            success, order_outcomes = self.execution_router.execute_order_intents(
                [intent],
                source_metadata={"signal": signal.strategy.value, "signal_ref": signal_ref, "position_id": int(row["id"])},
                market_prices={str(row["symbol"]): to_decimal(current_price)},
                market_state_inputs=[self._market_state_from_book(book, liquidation_side, intent.order_id)],
            )
            outcome = order_outcomes[0]
            if not success or outcome.filled_quantity <= 0 or outcome.price_cents is None:
                logger.info(
                    "event_position_exit_failed",
                    extra={"event": {"position_id": row["id"], "symbol": row["symbol"], "reason": outcome.status, "status": outcome.status}},
                )
                continue
            exit_reason = "stop_loss" if stop_hit else "time_exit"
            exit_price = outcome.price_cents / 100.0
            entry_price = float(row["entry_price"])
            ret = ((exit_price / entry_price) - 1.0) if row["side"] == "buy" else ((entry_price / exit_price) - 1.0)
            opened_at = datetime.fromisoformat(str(row["opened_at"]))
            holding_seconds = (outcome.created_at - opened_at).total_seconds()
            remaining_position = max(0, int(row["quantity"]) - outcome.filled_quantity)
            if remaining_position > 0:
                self.repo.update_event_position_quantity(
                    int(row["id"]),
                    remaining_position,
                    {
                        "last_partial_exit_price": exit_price,
                        "last_partial_exit_reason": exit_reason,
                        "last_partial_exit_quantity": outcome.filled_quantity,
                        "remaining_quantity": remaining_position,
                        "last_partial_exit_at": outcome.created_at.isoformat(),
                        "exit_order_id": intent.order_id,
                    },
                )
                logger.info(
                    "event_position_partially_closed",
                    extra={"event": {"position_id": row["id"], "symbol": row["symbol"], "filled_quantity": outcome.filled_quantity, "remaining_quantity": remaining_position, "exit_reason": exit_reason}},
                )
                continue
            self.repo.close_event_position(int(row["id"]), "closed", {"exit_price": exit_price, "exit_reason": exit_reason, "exit_order_id": intent.order_id})
            self.repo.persist_event_trade_outcome(
                EventTradeOutcomeRecord(
                    position_id=int(row["id"]),
                    symbol=str(row["symbol"]),
                    entry_price=entry_price,
                    exit_price=exit_price,
                    return_pct=ret,
                    holding_seconds=holding_seconds,
                    exit_reason=exit_reason,
                    metadata_json={"side": row["side"]},
                    closed_at=outcome.created_at.isoformat(),
                )
            )
            logger.info("event_position_closed", extra={"event": {"position_id": row["id"], "symbol": row["symbol"], "exit_reason": exit_reason, "return_pct": ret}})
            closed += 1
        return closed

    def _build_signal(
        self,
        *,
        symbol: str,
        side: str,
        quantity: int,
        order_type: str,
        limit_price,
        source: str,
        confidence: float,
        timestamp: datetime,
        action_prefix: str,
    ) -> Signal:
        return Signal(
            strategy=StrategyName.EVENT_DRIVEN,
            ticker=symbol,
            action=f"{action_prefix}_{side}",
            quantity=quantity,
            edge=SignalEdge.from_values(edge_before_fees=0, edge_after_fees=0),
            source=source,
            confidence=confidence,
            priority=1,
            legs=[
                SignalLegIntent(
                    instrument=InstrumentRef(symbol=symbol, venue="paper_equity"),
                    side=side,
                    order_type=_normalize_order_type(order_type),
                    limit_price=limit_price,
                    time_in_force="DAY",
                )
            ],
            ts=timestamp,
        )

    @staticmethod
    def _market_state_from_book(book, side: str, order_id: str) -> MarketStateInput:
        reference_price = book.ask_price if side == "buy" else book.bid_price
        return MarketStateInput(
            observed_at=book.ts,
            instrument=InstrumentRef(symbol=book.symbol, venue="paper_equity"),
            source="event_paper_book",
            reference_kind="execution_reference",
            reference_price=to_decimal(reference_price),
            provenance=book.source,
            best_bid=to_decimal(book.bid_price),
            best_ask=to_decimal(book.ask_price),
            bid_size=book.bid_levels[0].size if book.bid_levels else None,
            ask_size=book.ask_levels[0].size if book.ask_levels else None,
            order_id=order_id,
        )


def _stop_price(entry_price: float, side: str, stop_loss_pct: float) -> float:
    if side == "buy":
        return entry_price * (1.0 - stop_loss_pct)
    return entry_price * (1.0 + stop_loss_pct)


def _parse_metadata(raw: str | dict | None) -> dict:
    if raw is None:
        return {}
    if isinstance(raw, dict):
        return dict(raw)
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        return {}


def _normalize_order_type(order_type: str) -> str:
    if order_type == "passive":
        return "limit"
    if order_type == "marketable":
        return "market"
    return order_type
