from __future__ import annotations

import json
import math
from datetime import datetime, timedelta, timezone

from app.events.models import BrokerFill, TradeIdea
from app.execution.broker import PaperBroker, PaperExecutionResult
from app.persistence.contracts import (
    EventEntryQueueRecord,
    EventPaperExecutionAuditRecord,
    EventPositionRecord,
    EventTradeOutcomeRecord,
)
from app.persistence.event_repo import EventRepository


class EventExecutionEngine:
    def __init__(self, repo: EventRepository, broker: PaperBroker) -> None:
        self.repo = repo
        self.broker = broker

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
            metadata = row["metadata_json"]
            try:
                parsed_metadata = {} if metadata is None else json.loads(metadata)
            except json.JSONDecodeError:
                parsed_metadata = {}
            result = self.broker.place_order(
                row["symbol"],
                row["side"],
                int(row["quantity"]),
                signal_ts=datetime.fromisoformat(str(row["due_at"])),
                order_style=str(parsed_metadata.get("paper_order_style", "marketable")),
                limit_price=(
                    float(parsed_metadata["paper_limit_price"])
                    if parsed_metadata.get("paper_limit_price") is not None
                    else None
                ),
            )
            self._persist_execution_audit(row["id"], None, row["symbol"], row["side"], result)
            fill = result.fill
            if fill is None:
                failure_reason = result.status if result.status != "rejected" else _last_audit_reason(result)
                self.repo.update_event_entry_status(
                    row["id"],
                    "failed" if result.status == "rejected" else "unfilled",
                    {
                        "failure_reason": failure_reason,
                        "broker_status": result.status,
                        "submitted_at": result.submitted_at.isoformat(),
                    },
                )
                logger.info(
                    "event_order_failed",
                    extra={"event": {"symbol": row["symbol"], "queue_id": row["id"], "reason": failure_reason, "status": result.status}},
                )
                continue
            entry_status = "filled" if result.remaining_quantity == 0 else "partially_filled"
            self.repo.update_event_entry_status(
                row["id"],
                entry_status,
                {
                    "fill_price": fill.price,
                    "filled_at": fill.timestamp_utc.isoformat(),
                    "filled_quantity": fill.quantity,
                    "remaining_quantity": result.remaining_quantity,
                    "broker_status": result.status,
                },
            )
            stop_price = _stop_price(fill.price, row["side"], stop_loss_pct)
            self.repo.persist_event_position(
                EventPositionRecord(
                    trade_candidate_id=row["trade_candidate_id"],
                    news_event_id=int(parsed_metadata.get("news_event_id", 0)),
                    symbol=row["symbol"],
                    side=row["side"],
                    quantity=fill.quantity,
                    entry_price=fill.price,
                    stop_price=stop_price,
                    opened_at=fill.timestamp_utc.isoformat(),
                    planned_exit_at=(fill.timestamp_utc + timedelta(seconds=max_hold_seconds)).isoformat(),
                    status="open",
                    metadata_json={
                        "queue_id": row["id"],
                        "tranche_index": row["tranche_index"],
                        "total_tranches": row["total_tranches"],
                        "trade_metadata": parsed_metadata,
                        "requested_quantity": int(row["quantity"]),
                        "filled_quantity": fill.quantity,
                        "remaining_quantity": result.remaining_quantity,
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
                        "filled_quantity": fill.quantity,
                        "remaining_quantity": result.remaining_quantity,
                        "price": fill.price,
                        "queue_id": row["id"],
                        "status": result.status,
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
            current_price = self.broker.get_price(row["symbol"], liquidation_side=liquidation_side)
            planned_exit_at = datetime.fromisoformat(str(row["planned_exit_at"]))
            should_exit_for_time = planned_exit_at <= effective_now
            if current_price is None:
                continue
            stop_hit = (row["side"] == "buy" and current_price <= float(row["stop_price"])) or (
                row["side"] == "sell" and current_price >= float(row["stop_price"])
            )
            if not stop_hit and not should_exit_for_time:
                continue
            result = self.broker.place_order(
                row["symbol"],
                liquidation_side,
                int(row["quantity"]),
                signal_ts=effective_now,
            )
            self._persist_execution_audit(None, int(row["id"]), row["symbol"], liquidation_side, result)
            current = result.fill
            if current is None:
                logger.info(
                    "event_position_exit_failed",
                    extra={"event": {"position_id": row["id"], "symbol": row["symbol"], "reason": _last_audit_reason(result), "status": result.status}},
                )
                continue
            exit_reason = "stop_loss" if stop_hit else "time_exit"
            entry_price = float(row["entry_price"])
            ret = ((current.price / entry_price) - 1.0) if row["side"] == "buy" else ((entry_price / current.price) - 1.0)
            opened_at = datetime.fromisoformat(str(row["opened_at"]))
            holding_seconds = (current.timestamp_utc - opened_at).total_seconds()
            if result.remaining_quantity > 0:
                remaining_position = int(row["quantity"]) - current.quantity
                if remaining_position > 0:
                    self.repo.update_event_position_quantity(
                        int(row["id"]),
                        remaining_position,
                        {
                            "last_partial_exit_price": current.price,
                            "last_partial_exit_reason": exit_reason,
                            "last_partial_exit_quantity": current.quantity,
                            "remaining_quantity": remaining_position,
                            "last_partial_exit_at": current.timestamp_utc.isoformat(),
                        },
                    )
                    logger.info(
                        "event_position_partially_closed",
                        extra={
                            "event": {
                                "position_id": row["id"],
                                "symbol": row["symbol"],
                                "filled_quantity": current.quantity,
                                "remaining_quantity": remaining_position,
                                "exit_reason": exit_reason,
                            }
                        },
                    )
                    continue
            self.repo.close_event_position(int(row["id"]), "closed", {"exit_price": current.price, "exit_reason": exit_reason})
            self.repo.persist_event_trade_outcome(
                EventTradeOutcomeRecord(
                    position_id=int(row["id"]),
                    symbol=row["symbol"],
                    entry_price=entry_price,
                    exit_price=current.price,
                    return_pct=ret,
                    holding_seconds=holding_seconds,
                    exit_reason=exit_reason,
                    metadata_json={"side": row["side"]},
                    closed_at=current.timestamp_utc.isoformat(),
                )
            )
            logger.info("event_position_closed", extra={"event": {"position_id": row["id"], "symbol": row["symbol"], "exit_reason": exit_reason, "return_pct": ret}})
            closed += 1
        return closed

    def _persist_execution_audit(
        self,
        queue_id: int | None,
        position_id: int | None,
        symbol: str,
        side: str,
        result: PaperExecutionResult,
    ) -> None:
        for event in result.audit_events:
            self.repo.persist_event_paper_execution_audit(
                EventPaperExecutionAuditRecord(
                    queue_id=queue_id,
                    position_id=position_id,
                    symbol=symbol,
                    side=side,
                    requested_quantity=result.requested_quantity,
                    filled_quantity=result.filled_quantity,
                    remaining_quantity=result.remaining_quantity,
                    order_style=result.order_style,
                    status=result.status,
                    decision_state=event.get("state", "unknown"),
                    event_timestamp_utc=event.get("timestamp_utc", result.submitted_at.isoformat()),
                    price=event.get("price") or event.get("average_fill_price"),
                    metadata_json=event,
                )
            )


def _stop_price(entry_price: float, side: str, stop_loss_pct: float) -> float:
    if side == "buy":
        return entry_price * (1.0 - stop_loss_pct)
    return entry_price * (1.0 + stop_loss_pct)


def _last_audit_reason(result: PaperExecutionResult) -> str:
    for event in reversed(result.audit_events):
        reason = event.get("reason")
        if reason:
            return str(reason)
    return result.status
