from __future__ import annotations

from decimal import Decimal

from app.execution.models import SignalLegIntent
from app.instruments.models import InstrumentRef
from app.market_data.fees import balance_aligned_fee_dollars, edge_threshold
from app.models import Market, Signal, SignalEdge, StrategyEvaluation, StrategyName
from app.strategies.base import Strategy


class PartitionStrategy(Strategy):
    def __init__(self, min_edge_bps: float, slippage_buffer_bps: float, max_position_pct: float, min_depth: int = 1, min_coverage: float = 0.98) -> None:
        self.min_edge_bps = min_edge_bps
        self.slippage_buffer_bps = slippage_buffer_bps
        self.max_position_pct = max_position_pct
        self.min_depth = min_depth
        self.min_coverage = min_coverage

    def evaluate(self, markets: list[Market], equity: float, all_event_markets: list[Market] | None = None) -> tuple[list[Signal], list[StrategyEvaluation]]:
        all_event_markets = all_event_markets or markets
        members = [market.ticker for market in markets]
        all_members = [market.ticker for market in all_event_markets]
        event_ticker = all_event_markets[0].event_ticker if all_event_markets else (markets[0].event_ticker if markets else "unknown")
        if len(all_event_markets) < 2:
            return [], [
                StrategyEvaluation(
                    strategy=StrategyName.PARTITION,
                    ticker=event_ticker,
                    generated=False,
                    reason="singleton_group",
                    edge_before_fees=0.0,
                    edge_after_fees=0.0,
                    fee_estimate=0.0,
                    quantity=0,
                    metadata={"members": members, "all_members": all_members},
                )
            ]
        metadata = _partition_metadata(all_event_markets, markets, self.min_coverage)
        if not _looks_exhaustive(all_event_markets):
            return [], [
                StrategyEvaluation(
                    strategy=StrategyName.PARTITION,
                    ticker=event_ticker,
                    generated=False,
                    reason="not_exhaustive_candidate",
                    edge_before_fees=0.0,
                    edge_after_fees=0.0,
                    fee_estimate=0.0,
                    quantity=0,
                    metadata=metadata,
                )
            ]
        if metadata["dead_markets"]:
            return [], [
                StrategyEvaluation(
                    strategy=StrategyName.PARTITION,
                    ticker=event_ticker,
                    generated=False,
                    reason="invalid_partition_due_to_dead_markets",
                    edge_before_fees=0.0,
                    edge_after_fees=0.0,
                    fee_estimate=0.0,
                    quantity=0,
                    metadata=metadata,
                )
            ]
        if metadata["summed_probability_all_legs"] < self.min_coverage:
            return [], [
                StrategyEvaluation(
                    strategy=StrategyName.PARTITION,
                    ticker=event_ticker,
                    generated=False,
                    reason="incomplete_partition_coverage",
                    edge_before_fees=1.0 - metadata["summed_probability_all_legs"],
                    edge_after_fees=1.0 - metadata["summed_probability_all_legs"],
                    fee_estimate=0.0,
                    quantity=0,
                    metadata=metadata,
                )
            ]
        if any(market.yes_ask is None for market in markets):
            return [], [
                StrategyEvaluation(
                    strategy=StrategyName.PARTITION,
                    ticker=event_ticker,
                    generated=False,
                    reason="missing_quotes",
                    edge_before_fees=0.0,
                    edge_after_fees=0.0,
                    fee_estimate=0.0,
                    quantity=0,
                    metadata=metadata,
                )
            ]
        summed = metadata["summed_probability_tradeable_legs"]
        fee_cost = sum(float(balance_aligned_fee_dollars(Decimal(market.yes_ask) / Decimal("100"), side="buy")) for market in markets)
        gate = edge_threshold(
            edge_before_fees=Decimal(str(1.0 - summed)),
            fee_estimate=Decimal(str(fee_cost)),
            execution_buffer=Decimal(str(self.slippage_buffer_bps)) / Decimal("10000"),
        )
        edge_bps = float(gate.edge_after_fees * Decimal("10000"))
        max_price = max((market.yes_ask or 0) / 100.0 for market in markets)
        qty = self.size_from_equity(equity, self.max_position_pct, max_price)
        if qty < self.min_depth:
            return [], [
                StrategyEvaluation(
                    strategy=StrategyName.PARTITION,
                    ticker=event_ticker,
                    generated=False,
                    reason="insufficient_depth",
                    edge_before_fees=float(gate.edge_before_fees),
                    edge_after_fees=float(gate.edge_after_fees),
                    fee_estimate=float(gate.fee_estimate),
                    quantity=qty,
                    metadata=metadata,
                )
            ]
        if not gate.passes_bps(self.min_edge_bps):
            return [], [
                StrategyEvaluation(
                    strategy=StrategyName.PARTITION,
                    ticker=event_ticker,
                    generated=False,
                    reason="edge_below_threshold",
                    edge_before_fees=float(gate.edge_before_fees),
                    edge_after_fees=float(gate.edge_after_fees),
                    fee_estimate=float(gate.fee_estimate),
                    quantity=qty,
                    metadata=metadata,
                )
            ]
        signal = Signal(
                strategy=StrategyName.PARTITION,
                ticker=event_ticker,
                action="buy_partition",
                quantity=qty,
                edge=SignalEdge.from_values(
                    edge_before_fees=gate.edge_before_fees,
                    edge_after_fees=gate.edge_after_fees,
                    fee_estimate=gate.fee_estimate,
                    execution_buffer=Decimal(str(self.slippage_buffer_bps)) / Decimal("10000"),
                ),
                source="partition_cover",
                confidence=1.0,
                priority=1,
                legs=[
                    SignalLegIntent(
                        instrument=InstrumentRef(symbol=market.ticker, contract_side="yes"),
                        side="buy",
                        order_type="limit",
                        limit_price=Decimal(int(market.yes_ask)) / Decimal("100"),
                        time_in_force="FOK",
                    )
                    for market in markets
                ],
            )
        evaluation = StrategyEvaluation(
            strategy=StrategyName.PARTITION,
            ticker=event_ticker,
            generated=True,
            reason="signal_generated",
            edge_before_fees=float(gate.edge_before_fees),
            edge_after_fees=float(gate.edge_after_fees),
            fee_estimate=float(gate.fee_estimate),
            quantity=qty,
            metadata=metadata,
        )
        return [signal], [evaluation]

    def generate(self, markets: list[Market], equity: float) -> list[Signal]:
        signals, _ = self.evaluate(markets, equity)
        return signals


def _looks_exhaustive(markets: list[Market]) -> bool:
    text = " ".join([market.title for market in markets]).lower()
    return any(token in text for token in ("range", "between", "rate", "inflation", "cpi", "fed"))


def _partition_metadata(all_event_markets: list[Market], tradeable_markets: list[Market], min_coverage: float) -> dict:
    all_members = [market.ticker for market in all_event_markets]
    tradeable_members = [market.ticker for market in tradeable_markets]
    dead_markets = [market.ticker for market in all_event_markets if market.yes_ask is None]
    summed_probability_all_legs = sum((market.yes_ask or 0) for market in all_event_markets) / 100.0
    summed_probability_tradeable_legs = sum((market.yes_ask or 0) for market in tradeable_markets) / 100.0
    missing_probability_mass = max(0.0, min_coverage - summed_probability_all_legs)
    return {
        "all_members": all_members,
        "members": tradeable_members,
        "tradeable_members": tradeable_members,
        "dead_markets": dead_markets,
        "summed_probability_all_legs": summed_probability_all_legs,
        "summed_probability_tradeable_legs": summed_probability_tradeable_legs,
        "missing_probability_mass": missing_probability_mass,
        "quote_context_verified": len(dead_markets) == 0 and len(tradeable_members) == len(all_members),
    }
