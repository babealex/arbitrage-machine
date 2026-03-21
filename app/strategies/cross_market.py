from __future__ import annotations

from datetime import datetime, timezone

from app.models import Market, OrderBookSnapshot, Signal, StrategyEvaluation, StrategyName
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

        yes_exec_prob = market.yes_ask / 100.0
        no_exec_prob = market.no_ask / 100.0
        yes_fee = fee_from_probability(yes_exec_prob)
        no_fee = fee_from_probability(no_exec_prob)
        yes_edge_before_fees = mapped_probability - yes_exec_prob
        no_external_prob = 1.0 - mapped_probability
        no_edge_before_fees = no_external_prob - no_exec_prob
        yes_edge = yes_edge_before_fees - yes_fee - self.slippage_buffer
        no_edge = no_edge_before_fees - no_fee - self.slippage_buffer

        candidates = [
            ("buy_yes", yes_edge, yes_edge_before_fees, yes_fee, orderbook.best_yes_ask_size(), "yes", int(market.yes_ask)),
            ("buy_no", no_edge, no_edge_before_fees, no_fee, orderbook.best_no_ask_size(), "no", int(market.no_ask)),
        ]
        if not self.directional_allowed:
            candidates = [candidate for candidate in candidates if candidate[0] == "buy_yes"]
        best_action, best_edge, best_before_fees, fee, depth, leg_side, leg_price = max(candidates, key=lambda item: item[1])
        metadata = {
            "mapped_probability": mapped_probability,
            "kalshi_yes_prob": yes_exec_prob,
            "kalshi_no_prob": no_exec_prob,
            "yes_fee": yes_fee,
            "no_fee": no_fee,
            "slippage_buffer": self.slippage_buffer,
            "yes_edge": yes_edge,
            "no_edge": no_edge,
            "best_depth": depth,
        }
        if depth < self.min_depth:
            return [], [self._evaluation(market.ticker, False, "insufficient_depth", best_before_fees, best_edge, fee, 0, metadata)]
        if best_edge < self.min_edge:
            return [], [self._evaluation(market.ticker, False, "edge_below_threshold", best_before_fees, best_edge, fee, 0, metadata)]
        price_prob = yes_exec_prob if best_action == "buy_yes" else no_exec_prob
        qty = min(self.size_from_equity(equity, self.max_position_pct, price_prob), depth)
        signal = Signal(
                strategy=StrategyName.CROSS_MARKET,
                ticker=market.ticker,
                action=best_action,
                probability_edge=best_edge,
                expected_edge_bps=best_edge * 10_000,
                quantity=qty,
                legs=[{"ticker": market.ticker, "action": "buy", "side": leg_side, "price": leg_price, "tif": "IOC"}],
                metadata=metadata,
            )
        return [signal], [self._evaluation(market.ticker, True, "signal_generated", best_before_fees, best_edge, fee, qty, metadata)]

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
    return 0.07 * probability * (1.0 - probability)
