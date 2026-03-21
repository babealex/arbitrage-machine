from __future__ import annotations

from app.models import Signal, StrategyEvaluation, StrategyName
from app.strategies.base import Strategy


class MomentumStubStrategy(Strategy):
    def evaluate(self, *args, **kwargs) -> tuple[list[Signal], list[StrategyEvaluation]]:
        return [], [
            StrategyEvaluation(
                strategy=StrategyName.MOMENTUM,
                ticker="momentum_stub",
                generated=False,
                reason="not_implemented",
                edge_before_fees=0.0,
                edge_after_fees=0.0,
                fee_estimate=0.0,
                quantity=0,
                metadata={},
            )
        ]

    def generate(self, *args, **kwargs) -> list[Signal]:
        return []
