from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal

from app.execution.models import ExecutionOutcome, OrderIntent
from app.execution.slippage import realized_slippage_bps
from app.market_data.snapshots import MarketStateInput
from app.models import Signal
from app.replay_execution import ExecutionReplayEngine


@dataclass(frozen=True, slots=True)
class ExecutionReport:
    summary: dict[str, object]
    by_strategy: dict[str, dict[str, object]]
    by_venue: dict[str, dict[str, object]]


def build_execution_report(
    *,
    signals: list[Signal],
    order_intents: list[OrderIntent],
    execution_outcomes: list[ExecutionOutcome],
    market_state_inputs: list[MarketStateInput] | None = None,
) -> ExecutionReport:
    signals_by_ref = {
        f"{signal.strategy.value}:{signal.ticker}:{signal.ts.isoformat()}": signal
        for signal in signals
    }
    intents_by_id = {intent.order_id: intent for intent in order_intents}
    strategy_stats: dict[str, dict[str, Decimal | int | float]] = defaultdict(
        lambda: {
            "orders": 0,
            "filled_orders": 0,
            "partial_fills": 0,
            "requested_quantity": Decimal("0"),
            "filled_quantity": Decimal("0"),
            "intended_edge_sum": Decimal("0"),
            "realized_edge_sum": Decimal("0"),
            "slippage_bps_sum": Decimal("0"),
            "latency_seconds_sum": Decimal("0"),
            "latency_count": 0,
        }
    )
    venue_stats: dict[str, dict[str, Decimal | int | float]] = defaultdict(
        lambda: {
            "orders": 0,
            "filled_orders": 0,
            "requested_quantity": Decimal("0"),
            "filled_quantity": Decimal("0"),
        }
    )
    market_state_by_order = {
        item.order_id: item
        for item in (market_state_inputs or [])
        if item.order_id is not None
    }
    for outcome in execution_outcomes:
        intent = intents_by_id.get(outcome.client_order_id)
        if intent is None:
            continue
        signal = signals_by_ref.get(intent.signal_ref)
        strategy = intent.strategy_name
        stats = strategy_stats[strategy]
        venue = intent.instrument.venue
        venue_stats[venue]["orders"] += 1
        venue_stats[venue]["requested_quantity"] += Decimal(outcome.requested_quantity)
        venue_stats[venue]["filled_quantity"] += Decimal(outcome.filled_quantity)
        stats["orders"] += 1
        stats["requested_quantity"] += Decimal(outcome.requested_quantity)
        stats["filled_quantity"] += Decimal(outcome.filled_quantity)
        if outcome.filled_quantity > 0:
            stats["filled_orders"] += 1
            venue_stats[venue]["filled_orders"] += 1
        if 0 < outcome.filled_quantity < outcome.requested_quantity:
            stats["partial_fills"] += 1
        if signal is not None:
            intended_edge = signal.edge.edge_after_fees
            stats["intended_edge_sum"] += intended_edge
            market_state = market_state_by_order.get(outcome.client_order_id)
            expected_price_cents = None
            if market_state is not None:
                expected_price_cents = market_state.reference_price_cents
            elif outcome.metadata.get("reference_price_cents") is not None:
                expected_price_cents = int(outcome.metadata["reference_price_cents"])
            elif outcome.metadata.get("expected_price_cents") is not None:
                expected_price_cents = int(outcome.metadata["expected_price_cents"])
            slippage_bps = realized_slippage_bps(
                expected_price_cents=expected_price_cents,
                realized_price_cents=outcome.price_cents,
                side=outcome.side,
            )
            stats["slippage_bps_sum"] += slippage_bps
            stats["realized_edge_sum"] += intended_edge - (slippage_bps / Decimal("10000"))
        latency = _latency_seconds(intent, outcome)
        if latency is not None:
            stats["latency_seconds_sum"] += latency
            stats["latency_count"] += 1
    by_strategy: dict[str, dict[str, object]] = {}
    for strategy, stats in strategy_stats.items():
        orders = int(stats["orders"])
        filled_orders = int(stats["filled_orders"])
        requested_quantity = Decimal(stats["requested_quantity"])
        filled_quantity = Decimal(stats["filled_quantity"])
        latency_count = int(stats["latency_count"])
        by_strategy[strategy] = {
            "orders": orders,
            "fill_rate": 0.0 if orders == 0 else filled_orders / orders,
            "quantity_fill_rate": 0.0 if requested_quantity == 0 else float(filled_quantity / requested_quantity),
            "partial_fills": int(stats["partial_fills"]),
            "avg_intended_edge": _avg_decimal(stats["intended_edge_sum"], orders),
            "avg_realized_edge": _avg_decimal(stats["realized_edge_sum"], orders),
            "avg_slippage_bps": _avg_decimal(stats["slippage_bps_sum"], orders),
            "avg_latency_seconds": 0.0 if latency_count == 0 else float(Decimal(stats["latency_seconds_sum"]) / Decimal(latency_count)),
        }
    by_venue: dict[str, dict[str, object]] = {}
    for venue, stats in venue_stats.items():
        orders = int(stats["orders"])
        requested_quantity = Decimal(stats["requested_quantity"])
        filled_quantity = Decimal(stats["filled_quantity"])
        by_venue[venue] = {
            "orders": orders,
            "fill_rate": 0.0 if orders == 0 else int(stats["filled_orders"]) / orders,
            "quantity_fill_rate": 0.0 if requested_quantity == 0 else float(filled_quantity / requested_quantity),
        }
    replay = ExecutionReplayEngine().replay(
        signals=signals,
        order_intents=order_intents,
        execution_outcomes=execution_outcomes,
        market_state_inputs=market_state_inputs,
        starting_cash=Decimal("0"),
    )
    marked_open_exposure = Decimal("0")
    marked_unrealized_pnl = Decimal("0")
    latest_by_instrument = {}
    for item in market_state_inputs or []:
        latest_by_instrument[(item.instrument.venue, item.instrument.symbol, item.instrument.contract_side)] = item
    for position in replay.snapshot.positions.values():
        key = (position.instrument.venue, position.instrument.symbol, position.instrument.contract_side)
        latest = latest_by_instrument.get(key)
        if latest is None:
            continue
        if position.quantity > 0 and latest.best_bid_cents is not None:
            mark_cents = Decimal(latest.best_bid_cents)
        elif position.quantity < 0 and latest.best_ask_cents is not None:
            mark_cents = Decimal(latest.best_ask_cents)
        else:
            mark_cents = Decimal(latest.reference_price_cents)
        marked_open_exposure += abs(position.quantity) * (mark_cents / Decimal("100"))
        marked_unrealized_pnl += position.quantity * ((mark_cents - position.average_price_cents) / Decimal("100"))
    summary = {
        "orders": sum(int(stats["orders"]) for stats in strategy_stats.values()),
        "filled_orders": sum(int(stats["filled_orders"]) for stats in strategy_stats.values()),
        "partial_fills": sum(int(stats["partial_fills"]) for stats in strategy_stats.values()),
        "realized_pnl": float(replay.snapshot.realized_pnl),
        "marked_unrealized_pnl": float(marked_unrealized_pnl),
        "open_exposure": float(marked_open_exposure),
    }
    return ExecutionReport(summary=summary, by_strategy=by_strategy, by_venue=by_venue)


def _latency_seconds(intent: OrderIntent, outcome: ExecutionOutcome) -> Decimal | None:
    if intent.created_at is None or outcome.created_at is None:
        return None
    return Decimal(str((outcome.created_at - intent.created_at).total_seconds()))


def _avg_decimal(total: Decimal | int | float, count: int) -> float:
    if count <= 0:
        return 0.0
    return float(Decimal(total) / Decimal(count))
