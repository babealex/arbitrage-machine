from __future__ import annotations

import math
from dataclasses import dataclass

from app.events.models import TradeIdea
from app.replay.market_data import SymbolReplaySnapshot


@dataclass(slots=True)
class SimulatedTrade:
    symbol: str
    direction: str
    quantity: int
    entry_price: float
    exit_prices: dict[str, float | None]
    returns: dict[str, float | None]
    notional: float
    metadata: dict


class ReplayTradeSimulator:
    def __init__(self, capital: float, risk_fraction: float) -> None:
        self.capital = capital
        self.risk_fraction = risk_fraction

    def simulate(self, idea: TradeIdea, snapshot: SymbolReplaySnapshot) -> SimulatedTrade | None:
        if not snapshot.metadata.get("event_within_tradable_session", True):
            return None
        if snapshot.metadata.get("entry_blocked_reason"):
            return None
        entry_price = snapshot.event_time_price.price
        if entry_price is None or entry_price <= 0:
            return None
        quantity = max(1, math.floor((self.capital * self.risk_fraction) / entry_price))
        exit_prices = {
            "5m": snapshot.plus_5m.price,
            "15m": snapshot.plus_15m.price,
            "60m": snapshot.plus_60m.price,
            "eod": snapshot.eod.price,
        }
        returns = {horizon: _directional_return(idea.side, entry_price, price) for horizon, price in exit_prices.items()}
        return SimulatedTrade(
            symbol=idea.symbol,
            direction=idea.side,
            quantity=quantity,
            entry_price=entry_price,
            exit_prices=exit_prices,
            returns=returns,
            notional=quantity * entry_price,
            metadata={"mapping": idea.metadata, "thesis": idea.thesis, "snapshot_metadata": snapshot.metadata},
        )


def _directional_return(side: str, entry_price: float, exit_price: float | None) -> float | None:
    if exit_price is None or entry_price <= 0:
        return None
    if side == "buy":
        return (exit_price / entry_price) - 1.0
    return (entry_price / exit_price) - 1.0
