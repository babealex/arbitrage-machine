from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
import logging
from typing import Callable

from app.models import PortfolioSnapshot, Signal
from app.risk.models import RiskTransitionEvent


@dataclass(slots=True)
class RiskState:
    day: date
    failed_trades: int = 0
    killed: bool = False
    reasons: list[str] = field(default_factory=list)


class RiskManager:
    def __init__(
        self,
        max_daily_loss_pct: float,
        max_total_exposure_pct: float,
        max_position_per_trade_pct: float,
        max_failed_trades_per_day: int,
        logger: logging.Logger | None = None,
        event_recorder: Callable[[RiskTransitionEvent], None] | None = None,
    ) -> None:
        self.max_daily_loss_pct = max_daily_loss_pct
        self.max_total_exposure_pct = max_total_exposure_pct
        self.max_position_per_trade_pct = max_position_per_trade_pct
        self.max_failed_trades_per_day = max_failed_trades_per_day
        self.state = RiskState(day=date.today())
        self.logger = logger
        self.event_recorder = event_recorder

    def _roll_day(self) -> None:
        if self.state.day != date.today():
            self.state = RiskState(day=date.today())

    def kill(self, reason: str, details: dict | None = None) -> None:
        self.state.killed = True
        if reason not in self.state.reasons:
            self.state.reasons.append(reason)
        if self.logger is not None:
            self.logger.error("kill_switch_triggered", extra={"event": {"reason": reason, "details": details or {}}})
        if self.event_recorder is not None:
            self.event_recorder(RiskTransitionEvent(state="killed", reason=reason, details=details or {}))

    def record_failed_trade(self) -> None:
        self._roll_day()
        self.state.failed_trades += 1
        if self.event_recorder is not None:
            self.event_recorder(
                RiskTransitionEvent(
                    state="active",
                    reason="failed_trade_recorded",
                    details={"failed_trades": self.state.failed_trades},
                )
            )
        if self.state.failed_trades >= self.max_failed_trades_per_day:
            self.kill("failed_trades_limit", {"failed_trades": self.state.failed_trades})

    def approve_signal(self, signal: Signal, snapshot: PortfolioSnapshot) -> bool:
        self._roll_day()
        if self.state.killed:
            return False
        if snapshot.equity <= 0:
            self.kill("non_positive_equity", {"equity": snapshot.equity})
            return False
        if snapshot.daily_pnl < 0 and abs(snapshot.daily_pnl) >= snapshot.equity * self.max_daily_loss_pct:
            self.kill("daily_loss_limit", {"daily_pnl": snapshot.daily_pnl, "equity": snapshot.equity})
            return False
        trade_notional = signal.quantity * max(abs(signal.probability_edge), 0.01)
        if trade_notional > snapshot.equity * self.max_position_per_trade_pct:
            return False
        current_exposure = sum(position.exposure for position in snapshot.positions.values())
        if current_exposure + signal.quantity > snapshot.equity * self.max_total_exposure_pct:
            return False
        return True
