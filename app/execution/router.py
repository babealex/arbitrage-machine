from __future__ import annotations

import logging
from decimal import Decimal

from app.core.money import quantize_cents, to_decimal
from app.execution.models import ExecutionOutcome, OrderIntent
from app.execution.equity_broker import EquityExecutionBroker
from app.execution.orders import build_order_intent_from_leg, reverse_leg, to_api_payload
from app.execution.slippage import apply_bps_slippage
from app.instruments.models import InstrumentRef
from app.kalshi.rest_client import KalshiRestClient
from app.market_data.snapshots import MarketStateInput
from app.models import Fill, Side, Signal
from app.persistence.contracts import KalshiOrderRecord
from app.persistence.kalshi_repo import KalshiRepository
from app.persistence.market_state_repo import MarketStateRepository
from app.portfolio.state import PortfolioState
from app.risk.manager import RiskManager


class ExecutionRouter:
    def __init__(self, client: KalshiRestClient, repo: KalshiRepository, portfolio: PortfolioState, risk: RiskManager, logger: logging.Logger, live_trading: bool, paper_equity_slippage_bps: Decimal | str | int | float = 12, kalshi_paper_slippage_bps: Decimal | str | int | float = 2, market_state_repo: MarketStateRepository | None = None, equity_broker: EquityExecutionBroker | None = None) -> None:
        self.client = client
        self.repo = repo
        self.portfolio = portfolio
        self.risk = risk
        self.logger = logger
        self.live_trading = live_trading
        self.paper_equity_slippage_bps = to_decimal(paper_equity_slippage_bps)
        self.kalshi_paper_slippage_bps = to_decimal(kalshi_paper_slippage_bps)
        self.market_state_repo = market_state_repo
        self.equity_broker = equity_broker

    def execute_signal(self, signal: Signal, *, extra_source_metadata: dict[str, object] | None = None) -> bool:
        if not self.risk.approve_signal(signal, self.portfolio.snapshot):
            self.logger.info("signal_rejected_by_risk", extra={"event": {"ticker": signal.ticker, "strategy": signal.strategy.value, "edge": signal.probability_edge}})
            return False
        self.repo.persist_signal(signal)
        self.logger.info("order_batch_started", extra={"event": {"ticker": signal.ticker, "strategy": signal.strategy.value, "legs": signal.legs, "quantity": signal.quantity}})
        signal_ref = f"{signal.strategy.value}:{signal.ticker}:{signal.ts.isoformat()}"
        intents = [
            build_order_intent_from_leg(
                leg,
                signal.quantity,
                signal.strategy.value,
                signal_ref,
                f"{signal.strategy.value}:{signal.ticker}:{index}:{signal.quantity}:{leg.instrument.symbol}:{leg.side}:{leg.price_cents}",
                signal.ts,
            )
            for index, leg in enumerate(signal.legs)
        ]
        success, _ = self.execute_order_intents(
            intents,
            source_metadata={
                "signal": signal.strategy.value,
                "signal_ref": signal_ref,
                "intended_edge": format(signal.edge.edge_after_fees, "f"),
                **(extra_source_metadata or {}),
            },
            flatten_on_failure=True,
        )
        return success

    def execute_order_intents(
        self,
        intents: list[OrderIntent],
        *,
        source_metadata: dict[str, object] | None = None,
        market_prices: dict[str, Decimal] | None = None,
        market_state_inputs: list[MarketStateInput] | None = None,
        flatten_on_failure: bool = False,
    ) -> tuple[bool, list[ExecutionOutcome]]:
        completed: list[OrderIntent] = []
        outcomes: list[ExecutionOutcome] = []
        metadata = dict(source_metadata or {})
        persisted_market_state = list(market_state_inputs or [])
        for intent in intents:
            matched_market_state = next((row for row in persisted_market_state if row.order_id == intent.order_id), None)
            if not any(item.order_id == intent.order_id for item in persisted_market_state):
                auto_state = self._build_default_market_state_input(intent, market_prices or {})
                if auto_state is not None:
                    persisted_market_state.append(auto_state)
                    matched_market_state = auto_state
            if self.market_state_repo is not None:
                for item in [row for row in persisted_market_state if row.order_id == intent.order_id]:
                    self.market_state_repo.persist_market_state_input(item)
            self.repo.persist_order(KalshiOrderRecord(intent=intent, status="created", metadata=metadata))
            self.logger.info("order_attempt", extra={"event": {"sleeve_name": metadata.get("sleeve_name"), "ticker": intent.instrument.symbol, "venue": intent.instrument.venue, "side": intent.side, "order_type": intent.order_type, "limit_price": intent.limit_price_cents, "quantity": format(intent.quantity, 'f')}})
            try:
                outcome = self._execute_intent(intent, metadata, market_prices or {}, matched_market_state)
                outcomes.append(outcome)
                completed.append(intent)
                self.repo.persist_execution_outcome(outcome)
                if intent.instrument.venue == "kalshi" and outcome.price_cents is not None:
                    fill_side = outcome.instrument.contract_side or "yes"
                    self.repo.persist_fill(Fill(order_id=intent.order_id, ticker=intent.instrument.symbol, quantity=int(outcome.filled_quantity), price=outcome.price_cents, side=Side(fill_side)))
                if intent.instrument.venue == "kalshi" and self.live_trading:
                    self.portfolio.reconcile(self.repo)
                if outcome.status in {"rejected", "limit_not_crossed"}:
                    raise RuntimeError(outcome.status)
            except Exception as exc:
                self.risk.record_failed_trade()
                rejected = ExecutionOutcome(
                    client_order_id=intent.order_id,
                    instrument=intent.instrument,
                    side=intent.side,
                    action=intent.order_type,
                    status="rejected",
                    requested_quantity=int(intent.quantity),
                    filled_quantity=0,
                    price_cents=intent.limit_price_cents,
                    time_in_force=intent.time_in_force,
                    is_flatten=False,
                    metadata={**metadata, "error": str(exc)},
                )
                self.repo.persist_order(KalshiOrderRecord(intent=intent, status="rejected", metadata={"error": str(exc), **metadata}))
                self.repo.persist_execution_outcome(rejected)
                outcomes.append(rejected)
                self.logger.error("order_failed", extra={"event": {"sleeve_name": metadata.get("sleeve_name"), "ticker": intent.instrument.symbol, "venue": intent.instrument.venue, "error": str(exc)}})
                if flatten_on_failure:
                    self._flatten(completed, metadata)
                return False, outcomes
        return True, outcomes

    def _flatten(self, completed: list[OrderIntent], metadata: dict[str, object]) -> None:
        for intent_to_flatten in reversed(completed):
            if intent_to_flatten.instrument.venue != "kalshi":
                continue
            reverse = reverse_leg(
                build_placeholder_leg_from_intent(intent_to_flatten)
            )
            for attempt in range(2):
                flatten_intent = build_order_intent_from_leg(
                    reverse,
                    int(intent_to_flatten.quantity),
                    str(metadata.get("signal", "unknown")),
                    intent_to_flatten.signal_ref,
                    f"flatten:{intent_to_flatten.instrument.symbol}:{attempt}",
                )
                self.repo.persist_order(KalshiOrderRecord(intent=flatten_intent, status="flatten_created", metadata=metadata))
                self.logger.warning("flatten_attempt", extra={"event": {"ticker": flatten_intent.instrument.symbol, "side": flatten_intent.side, "attempt": attempt + 1}})
                if not self.live_trading:
                    self.repo.persist_order(KalshiOrderRecord(intent=flatten_intent, status="flatten_simulated", metadata=metadata))
                    self.repo.persist_execution_outcome(
                        ExecutionOutcome(
                            client_order_id=flatten_intent.order_id,
                            instrument=flatten_intent.instrument,
                            side=flatten_intent.side,
                            action=flatten_intent.order_type,
                            status="flatten_simulated",
                            requested_quantity=int(flatten_intent.quantity),
                            filled_quantity=0,
                            price_cents=flatten_intent.limit_price_cents,
                            time_in_force=flatten_intent.time_in_force,
                            is_flatten=True,
                            metadata=metadata,
                        )
                    )
                    break
                try:
                    response = self.client.create_order(to_api_payload(flatten_intent))
                    self.repo.persist_order(KalshiOrderRecord(intent=flatten_intent, status="flatten_accepted", metadata=response))
                    self.repo.persist_execution_outcome(
                        ExecutionOutcome(
                            client_order_id=flatten_intent.order_id,
                            instrument=flatten_intent.instrument,
                            side=flatten_intent.side,
                            action=flatten_intent.order_type,
                            status="flatten_accepted",
                            requested_quantity=int(flatten_intent.quantity),
                            filled_quantity=0,
                            price_cents=flatten_intent.limit_price_cents,
                            time_in_force=flatten_intent.time_in_force,
                            is_flatten=True,
                            metadata=response,
                        )
                    )
                    self.logger.warning("flatten_success", extra={"event": {"ticker": flatten_intent.instrument.symbol, "response": response}})
                    self.portfolio.reconcile(self.repo)
                    break
                except Exception as exc:
                    self.repo.persist_order(KalshiOrderRecord(intent=flatten_intent, status="flatten_rejected", metadata={"error": str(exc), **metadata}))
                    self.repo.persist_execution_outcome(
                        ExecutionOutcome(
                            client_order_id=flatten_intent.order_id,
                            instrument=flatten_intent.instrument,
                            side=flatten_intent.side,
                            action=flatten_intent.order_type,
                            status="flatten_rejected",
                            requested_quantity=int(flatten_intent.quantity),
                            filled_quantity=0,
                            price_cents=flatten_intent.limit_price_cents,
                            time_in_force=flatten_intent.time_in_force,
                            is_flatten=True,
                            metadata={"error": str(exc)},
                        )
                    )
                    self.logger.error("flatten_failed", extra={"event": {"ticker": flatten_intent.instrument.symbol, "error": str(exc), "attempt": attempt + 1}})

    def _execute_intent(self, intent: OrderIntent, metadata: dict[str, object], market_prices: dict[str, Decimal], market_state: MarketStateInput | None) -> ExecutionOutcome:
        if intent.instrument.venue == "live_equity":
            return self._execute_live_equity_intent(intent, metadata, market_state)
        if intent.instrument.venue == "paper_equity":
            return self._execute_paper_equity_intent(intent, metadata, market_prices, market_state)
        return self._execute_kalshi_intent(intent, metadata)

    def _execute_live_equity_intent(
        self,
        intent: OrderIntent,
        metadata: dict[str, object],
        market_state: MarketStateInput | None,
    ) -> ExecutionOutcome:
        if not self.live_trading:
            raise RuntimeError("live_equity_intent_in_paper_mode")
        if self.equity_broker is None:
            raise RuntimeError("live_equity_broker_missing")
        outcome = self.equity_broker.execute_order(
            intent,
            source_metadata=metadata,
            market_state=market_state,
        )
        self.repo.persist_order(KalshiOrderRecord(intent=intent, status=outcome.status, metadata=outcome.metadata))
        return outcome

    def _execute_kalshi_intent(self, intent: OrderIntent, metadata: dict[str, object]) -> ExecutionOutcome:
        if not self.live_trading:
            self.repo.persist_order(KalshiOrderRecord(intent=intent, status="simulated", metadata=metadata))
            expected_price = intent.limit_price
            fill_price = expected_price
            if expected_price is not None:
                slippage_fill = apply_bps_slippage(
                    reference_price=expected_price,
                    side=intent.side,
                    slippage_bps=self.kalshi_paper_slippage_bps,
                )
                if intent.side == "buy":
                    fill_price = min(slippage_fill, expected_price)
                else:
                    fill_price = max(slippage_fill, expected_price)
            price_cents = None if fill_price is None else int((fill_price * Decimal("100")).to_integral_value())
            return ExecutionOutcome(
                client_order_id=intent.order_id,
                instrument=intent.instrument,
                side=intent.side,
                action=intent.order_type,
                status="simulated",
                requested_quantity=int(intent.quantity),
                filled_quantity=int(intent.quantity),
                price_cents=price_cents,
                time_in_force=intent.time_in_force,
                is_flatten=False,
                created_at=intent.created_at,
                metadata={
                    **metadata,
                    "reference_price_cents": intent.limit_price_cents,
                    "slippage_bps": format(self.kalshi_paper_slippage_bps, "f"),
                },
            )
        response = self.client.create_order(to_api_payload(intent))
        self.repo.persist_order(KalshiOrderRecord(intent=intent, status="accepted", metadata=response))
        order_id = str(response.get("order", {}).get("order_id", response.get("order_id", intent.order_id)))
        final_status = self.client.get_order(order_id) if order_id else response
        self.repo.persist_order(KalshiOrderRecord(intent=intent, status=str(final_status.get("status", "accepted")), metadata=final_status))
        return ExecutionOutcome(
            client_order_id=intent.order_id,
            instrument=intent.instrument,
            side=intent.side,
            action=intent.order_type,
            status=str(final_status.get("status", "accepted")),
            requested_quantity=int(intent.quantity),
            filled_quantity=int(intent.quantity),
            price_cents=intent.limit_price_cents,
            time_in_force=intent.time_in_force,
            is_flatten=False,
            metadata=final_status,
        )

    def _execute_paper_equity_intent(self, intent: OrderIntent, metadata: dict[str, object], market_prices: dict[str, Decimal], market_state: MarketStateInput | None) -> ExecutionOutcome:
        market_price = market_prices.get(intent.instrument.symbol)
        if market_price is None:
            raise RuntimeError(f"missing_market_price:{intent.instrument.symbol}")
        slippage = self.paper_equity_slippage_bps / Decimal("10000")
        fill_price = market_price * (Decimal("1") + slippage if intent.side == "buy" else Decimal("1") - slippage)
        fill_price = apply_bps_slippage(reference_price=market_price, side=intent.side, slippage_bps=self.paper_equity_slippage_bps)
        if intent.order_type == "limit" and intent.limit_price is not None:
            if intent.side == "buy" and fill_price > intent.limit_price:
                self.repo.persist_order(KalshiOrderRecord(intent=intent, status="limit_not_crossed", metadata=metadata))
                return ExecutionOutcome(
                    client_order_id=intent.order_id,
                    instrument=intent.instrument,
                    side=intent.side,
                    action=intent.order_type,
                    status="limit_not_crossed",
                    requested_quantity=int(intent.quantity),
                    filled_quantity=0,
                    price_cents=intent.limit_price_cents,
                    time_in_force=intent.time_in_force,
                    is_flatten=False,
                    metadata=metadata,
                )
            if intent.side == "sell" and fill_price < intent.limit_price:
                self.repo.persist_order(KalshiOrderRecord(intent=intent, status="limit_not_crossed", metadata=metadata))
                return ExecutionOutcome(
                    client_order_id=intent.order_id,
                    instrument=intent.instrument,
                    side=intent.side,
                    action=intent.order_type,
                    status="limit_not_crossed",
                    requested_quantity=int(intent.quantity),
                    filled_quantity=0,
                    price_cents=intent.limit_price_cents,
                    time_in_force=intent.time_in_force,
                    is_flatten=False,
                    metadata=metadata,
                )
        available_quantity = int(intent.quantity)
        if market_state is not None:
            visible_size = market_state.ask_size if intent.side == "buy" else market_state.bid_size
            if visible_size is not None:
                available_quantity = max(0, min(available_quantity, int(visible_size)))
        if available_quantity <= 0:
            self.repo.persist_order(KalshiOrderRecord(intent=intent, status="unfilled", metadata=metadata))
            return ExecutionOutcome.from_paper_equity_fill(
                client_order_id=intent.order_id,
                symbol=intent.instrument.symbol,
                side=intent.side,
                requested_quantity=int(intent.quantity),
                filled_quantity=0,
                price=None,
                status="unfilled",
                created_at=intent.created_at,
                metadata={
                    **metadata,
                    "strategy_name": intent.strategy_name,
                    "signal_ref": intent.signal_ref,
                    "reference_price_cents": int((quantize_cents(market_price) * Decimal("100")).to_integral_value()),
                    "slippage_bps": format(self.paper_equity_slippage_bps, "f"),
                },
            )
        status = "filled" if available_quantity >= int(intent.quantity) else "partial_fill"
        self.repo.persist_order(KalshiOrderRecord(intent=intent, status=status, metadata=metadata))
        return ExecutionOutcome.from_paper_equity_fill(
            client_order_id=intent.order_id,
            symbol=intent.instrument.symbol,
            side=intent.side,
            requested_quantity=int(intent.quantity),
            filled_quantity=available_quantity,
            price=fill_price,
            status=status,
            created_at=intent.created_at,
            metadata={
                **metadata,
                "strategy_name": intent.strategy_name,
                "signal_ref": intent.signal_ref,
                "reference_price_cents": int((quantize_cents(market_price) * Decimal("100")).to_integral_value()),
                "slippage_bps": format(self.paper_equity_slippage_bps, "f"),
            },
        )

    def _build_default_market_state_input(
        self,
        intent: OrderIntent,
        market_prices: dict[str, Decimal],
    ) -> MarketStateInput | None:
        if intent.instrument.venue == "paper_equity":
            market_price = market_prices.get(intent.instrument.symbol)
            if market_price is None:
                return None
            return MarketStateInput(
                observed_at=intent.created_at,
                instrument=intent.instrument,
                source="router_market_price",
                reference_kind="execution_reference",
                reference_price=market_price,
                provenance="execution_router",
                order_id=intent.order_id,
            )
        if intent.instrument.venue == "kalshi" and intent.limit_price is not None:
            return MarketStateInput(
                observed_at=intent.created_at,
                instrument=intent.instrument,
                source="router_limit_price",
                reference_kind="execution_reference",
                reference_price=intent.limit_price,
                provenance="execution_router",
                best_bid=intent.limit_price if intent.side == "sell" else None,
                best_ask=intent.limit_price if intent.side == "buy" else None,
                order_id=intent.order_id,
            )
        return None


def build_placeholder_leg_from_intent(intent: OrderIntent):
    from app.execution.models import SignalLegIntent

    return SignalLegIntent(
        instrument=intent.instrument,
        side=intent.side,
        order_type=intent.order_type,
        limit_price=intent.limit_price,
        time_in_force=intent.time_in_force,
    )
