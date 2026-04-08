from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal

from app.core.money import to_decimal
from app.execution.models import ExecutionOutcome, OrderIntent
from app.instruments.models import InstrumentRef
from app.market_data.snapshots import MarketStateInput
from app.models import Signal


InstrumentKey = tuple[str, str, str | None]


@dataclass(frozen=True, slots=True)
class ReplayPosition:
    instrument: InstrumentRef
    quantity: Decimal
    average_price_cents: Decimal


@dataclass(frozen=True, slots=True)
class ReplaySnapshot:
    cash: Decimal
    equity: Decimal
    realized_pnl: Decimal
    positions: dict[InstrumentKey, ReplayPosition]


@dataclass(frozen=True, slots=True)
class ReplayResult:
    signals: list[Signal]
    order_intents: list[OrderIntent]
    execution_outcomes: list[ExecutionOutcome]
    snapshot: ReplaySnapshot


class ExecutionReplayEngine:
    def replay(
        self,
        *,
        signals: list[Signal],
        order_intents: list[OrderIntent],
        execution_outcomes: list[ExecutionOutcome],
        market_state_inputs: list[MarketStateInput] | None = None,
        starting_cash: Decimal | str | int | float = Decimal("0"),
    ) -> ReplayResult:
        cash = to_decimal(starting_cash)
        realized_pnl = Decimal("0")
        positions: dict[InstrumentKey, ReplayPosition] = {}
        last_mark_price_cents: dict[InstrumentKey, Decimal] = {}
        order_index = {intent.order_id: intent for intent in order_intents}
        market_state_by_order = {
            item.order_id: item
            for item in (market_state_inputs or [])
            if item.order_id is not None
        }
        latest_market_state: dict[InstrumentKey, MarketStateInput] = {}
        for item in sorted(market_state_inputs or [], key=lambda current: current.observed_at):
            latest_market_state[(item.instrument.venue, item.instrument.symbol, item.instrument.contract_side)] = item
        sorted_outcomes = sorted(execution_outcomes, key=lambda outcome: outcome.created_at)
        for outcome in sorted_outcomes:
            key = (outcome.instrument.venue, outcome.instrument.symbol, outcome.instrument.contract_side)
            market_state = market_state_by_order.get(outcome.client_order_id)
            if market_state is not None:
                last_mark_price_cents[key] = Decimal(market_state.reference_price_cents)
            else:
                expected_price = outcome.metadata.get("reference_price_cents")
                if expected_price is None:
                    expected_price = outcome.metadata.get("expected_price_cents")
                if expected_price is not None:
                    last_mark_price_cents[key] = Decimal(str(expected_price))
            if outcome.filled_quantity <= 0 or outcome.price_cents is None:
                continue
            fill_quantity = Decimal(outcome.filled_quantity)
            price_cents = Decimal(outcome.price_cents)
            position = positions.get(key)
            if outcome.side == "buy":
                cash -= fill_quantity * (price_cents / Decimal("100"))
                if position is None:
                    positions[key] = ReplayPosition(outcome.instrument, fill_quantity, price_cents)
                elif position.quantity < 0:
                    cover_quantity = min(fill_quantity, abs(position.quantity))
                    realized_pnl += (position.average_price_cents - price_cents) * cover_quantity / Decimal("100")
                    remaining_fill = fill_quantity - cover_quantity
                    new_quantity = position.quantity + cover_quantity
                    if new_quantity == 0:
                        positions.pop(key)
                    else:
                        positions[key] = ReplayPosition(outcome.instrument, new_quantity, position.average_price_cents)
                    if remaining_fill > 0:
                        positions[key] = ReplayPosition(outcome.instrument, remaining_fill, price_cents)
                else:
                    total_quantity = position.quantity + fill_quantity
                    weighted_cost = (position.average_price_cents * position.quantity) + (price_cents * fill_quantity)
                    positions[key] = ReplayPosition(outcome.instrument, total_quantity, weighted_cost / total_quantity)
            else:
                cash += fill_quantity * (price_cents / Decimal("100"))
                if position is None:
                    positions[key] = ReplayPosition(outcome.instrument, -fill_quantity, price_cents)
                elif position.quantity > 0:
                    sell_quantity = min(fill_quantity, position.quantity)
                    realized_pnl += (price_cents - position.average_price_cents) * sell_quantity / Decimal("100")
                    remaining_fill = fill_quantity - sell_quantity
                    new_quantity = position.quantity - sell_quantity
                    if new_quantity == 0:
                        positions.pop(key)
                    else:
                        positions[key] = ReplayPosition(outcome.instrument, new_quantity, position.average_price_cents)
                    if remaining_fill > 0:
                        positions[key] = ReplayPosition(outcome.instrument, -remaining_fill, price_cents)
                else:
                    total_quantity = abs(position.quantity) + fill_quantity
                    weighted_cost = (position.average_price_cents * abs(position.quantity)) + (price_cents * fill_quantity)
                    positions[key] = ReplayPosition(outcome.instrument, -total_quantity, weighted_cost / total_quantity)
            if outcome.client_order_id not in order_index:
                continue
        equity = cash
        for position in positions.values():
            key = (position.instrument.venue, position.instrument.symbol, position.instrument.contract_side)
            market_state = latest_market_state.get(key)
            if market_state is not None:
                if position.quantity > 0 and market_state.best_bid_cents is not None:
                    mark_cents = Decimal(market_state.best_bid_cents)
                elif position.quantity < 0 and market_state.best_ask_cents is not None:
                    mark_cents = Decimal(market_state.best_ask_cents)
                else:
                    mark_cents = Decimal(market_state.reference_price_cents)
            else:
                mark_cents = last_mark_price_cents.get(key, position.average_price_cents)
            equity += position.quantity * (mark_cents / Decimal("100"))
        return ReplayResult(
            signals=sorted(signals, key=lambda signal: signal.ts),
            order_intents=sorted(order_intents, key=lambda intent: intent.created_at),
            execution_outcomes=sorted_outcomes,
            snapshot=ReplaySnapshot(
                cash=cash,
                equity=equity,
                realized_pnl=realized_pnl,
                positions=positions,
            ),
        )
