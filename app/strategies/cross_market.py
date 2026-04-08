from __future__ import annotations

from decimal import Decimal
from datetime import datetime, timezone

from app.execution.models import SignalLegIntent
from app.instruments.models import InstrumentRef
from app.market_data.fees import balance_aligned_fee_dollars, edge_threshold
from app.models import Market, OrderBookSnapshot, Signal, SignalEdge, StrategyEvaluation, StrategyName
from app.strategies.base import Strategy


class CrossMarketStrategy(Strategy):
    def __init__(self, min_edge: float, slippage_buffer: float, min_depth: int, min_time_to_expiry_seconds: int, max_position_pct: float, directional_allowed: bool) -> None:
        self.min_edge = min_edge
        self.slippage_buffer = slippage_buffer
        self.min_depth = min_depth
        self.min_time_to_expiry_seconds = min_time_to_expiry_seconds
        self.max_position_pct = max_position_pct
        self.directional_allowed = directional_allowed

    def evaluate(self, market: Market, mapped_probability: float | None, orderbook: OrderBookSnapshot | None, equity: float) -> tuple[list[Signal], list[StrategyEvaluation]]:
        if mapped_probability is None or market.yes_ask is None or market.no_ask is None or orderbook is None:
            return [], [
                StrategyEvaluation(
                    strategy=StrategyName.CROSS_MARKET,
                    ticker=market.ticker,
                    generated=False,
                    reason="missing_probability_or_orderbook",
                    edge_before_fees=0.0,
                    edge_after_fees=0.0,
                    fee_estimate=0.0,
                    quantity=0,
                    metadata={"mapped_probability_present": mapped_probability is not None, "orderbook_present": orderbook is not None},
                )
            ]
        if market.expiry is not None:
            remaining = (market.expiry - datetime.now(timezone.utc)).total_seconds()
            if remaining < self.min_time_to_expiry_seconds:
                return [], [self._evaluation(market.ticker, False, "too_close_to_expiry", 0.0, 0.0, 0.0, 0, {"seconds_to_expiry": remaining})]

        yes_exec_prob = Decimal(market.yes_ask) / Decimal("100")
        no_exec_prob = Decimal(market.no_ask) / Decimal("100")
        mapped_probability_decimal = Decimal(str(mapped_probability))
        yes_fee = balance_aligned_fee_dollars(yes_exec_prob, side="buy")
        no_fee = balance_aligned_fee_dollars(no_exec_prob, side="buy")
        yes_gate = edge_threshold(
            edge_before_fees=mapped_probability_decimal - yes_exec_prob,
            fee_estimate=yes_fee,
            execution_buffer=Decimal(str(self.slippage_buffer)),
        )
        no_external_prob = Decimal("1") - mapped_probability_decimal
        no_gate = edge_threshold(
            edge_before_fees=no_external_prob - no_exec_prob,
            fee_estimate=no_fee,
            execution_buffer=Decimal(str(self.slippage_buffer)),
        )

        candidates = [
            ("buy_yes", yes_gate, orderbook.best_yes_ask_size(), "yes", int(market.yes_ask)),
            ("buy_no", no_gate, orderbook.best_no_ask_size(), "no", int(market.no_ask)),
        ]
        if not self.directional_allowed:
            candidates = [candidate for candidate in candidates if candidate[0] == "buy_yes"]
        best_action, best_gate, depth, leg_side, leg_price = max(candidates, key=lambda item: item[1].edge_after_fees)
        metadata = {
            "mapped_probability": mapped_probability,
            "kalshi_yes_prob": float(yes_exec_prob),
            "kalshi_no_prob": float(no_exec_prob),
            "yes_fee": float(yes_fee),
            "no_fee": float(no_fee),
            "slippage_buffer": self.slippage_buffer,
            "yes_edge": float(yes_gate.edge_after_fees),
            "no_edge": float(no_gate.edge_after_fees),
            "yes_edge_before_fees": float(yes_gate.edge_before_fees),
            "no_edge_before_fees": float(no_gate.edge_before_fees),
            "best_depth": depth,
        }
        if depth < self.min_depth:
            return [], [self._evaluation(market.ticker, False, "insufficient_depth", float(best_gate.edge_before_fees), float(best_gate.edge_after_fees), float(best_gate.fee_estimate), 0, metadata)]
        if not best_gate.passes_probability(self.min_edge):
            return [], [self._evaluation(market.ticker, False, "edge_below_threshold", float(best_gate.edge_before_fees), float(best_gate.edge_after_fees), float(best_gate.fee_estimate), 0, metadata)]
        price_prob = float(yes_exec_prob if best_action == "buy_yes" else no_exec_prob)
        qty = min(self.size_from_equity(equity, self.max_position_pct, price_prob), depth)
        signal = Signal(
            strategy=StrategyName.CROSS_MARKET,
            ticker=market.ticker,
            action=best_action,
            quantity=qty,
            edge=SignalEdge.from_values(
                edge_before_fees=best_gate.edge_before_fees,
                edge_after_fees=best_gate.edge_after_fees,
                fee_estimate=best_gate.fee_estimate,
                execution_buffer=self.slippage_buffer,
            ),
            source="cross_market_probability_map",
            confidence=1.0,
            priority=1,
            legs=[
                SignalLegIntent(
                    instrument=InstrumentRef(symbol=market.ticker, contract_side=leg_side),
                    side="buy",
                    order_type="limit",
                    limit_price=Decimal(leg_price) / Decimal("100"),
                    time_in_force="IOC",
                )
            ],
        )
        return [signal], [self._evaluation(market.ticker, True, "signal_generated", float(best_gate.edge_before_fees), float(best_gate.edge_after_fees), float(best_gate.fee_estimate), qty, metadata)]

    def generate(self, market: Market, mapped_probability: float | None, orderbook: OrderBookSnapshot | None, equity: float) -> list[Signal]:
        signals, _ = self.evaluate(market, mapped_probability, orderbook, equity)
        return signals

    def _evaluation(self, ticker: str, generated: bool, reason: str, edge_before_fees: float, edge_after_fees: float, fee: float, quantity: int, metadata: dict) -> StrategyEvaluation:
        return StrategyEvaluation(
            strategy=StrategyName.CROSS_MARKET,
            ticker=ticker,
            generated=generated,
            reason=reason,
            edge_before_fees=edge_before_fees,
            edge_after_fees=edge_after_fees,
            fee_estimate=fee,
            quantity=quantity,
            metadata=metadata,
        )


def fee_from_probability(probability: float) -> float:
    return float(balance_aligned_fee_dollars(Decimal(str(probability)), side="buy"))
