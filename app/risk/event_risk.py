from __future__ import annotations

import math


class EventRiskManager:
    def __init__(self, starting_capital: float, max_risk_per_trade_pct: float, stop_loss_pct: float) -> None:
        self.starting_capital = starting_capital
        self.max_risk_per_trade_pct = max_risk_per_trade_pct
        self.stop_loss_pct = stop_loss_pct

    def size_for_price(self, price: float | None) -> int:
        if price is None or price <= 0 or self.stop_loss_pct <= 0:
            return 0
        max_risk_dollars = self.starting_capital * self.max_risk_per_trade_pct
        per_share_risk = price * self.stop_loss_pct
        if per_share_risk <= 0:
            return 0
        return max(0, math.floor(max_risk_dollars / per_share_risk))
