from __future__ import annotations

from abc import ABC, abstractmethod

from app.models import Signal, StrategyEvaluation


class Strategy(ABC):
    @abstractmethod
    def generate(self, *args, **kwargs) -> list[Signal]:
        raise NotImplementedError

    @abstractmethod
    def evaluate(self, *args, **kwargs) -> tuple[list[Signal], list[StrategyEvaluation]]:
        raise NotImplementedError

    @staticmethod
    def size_from_equity(equity: float, max_position_pct: float, price_prob: float) -> int:
        if price_prob <= 0:
            return 0
        max_notional = equity * max_position_pct
        contracts = int(max_notional / max(price_prob, 0.01))
        return max(1, contracts)
