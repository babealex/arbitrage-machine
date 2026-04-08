from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal


@dataclass(frozen=True, slots=True)
class TradeCandidate:
    candidate_id: str
    strategy_family: str
    source_kind: str
    instrument_key: str
    sleeve: str
    regime_name: str
    observed_at: datetime
    expected_pnl_after_costs: Decimal
    cvar_95_loss: Decimal
    uncertainty_penalty: Decimal
    liquidity_penalty: Decimal
    horizon_days: int
    regime_stability: Decimal
    notional_reference: Decimal
    max_loss_estimate: Decimal
    correlation_bucket: str
    state_ok: bool = True


@dataclass(frozen=True, slots=True)
class AllocationDecision:
    run_id: str
    candidate_id: str
    strategy_family: str
    source_kind: str
    sleeve: str
    regime_name: str
    observed_at: datetime
    score: Decimal
    approved: bool
    rejection_reason: str | None
    target_notional: Decimal
    max_loss_budget: Decimal
    expected_pnl_after_costs: Decimal
    cvar_95_loss: Decimal
    uncertainty_penalty: Decimal
    liquidity_penalty: Decimal
    horizon_days: int
    correlation_bucket: str
    execution_price: Decimal | None = None
    slippage_estimate: Decimal | None = None
    fill_status: str | None = None


@dataclass(frozen=True, slots=True)
class CapitalSleeve:
    name: str
    capital_cap_fraction: Decimal
    max_trade_fraction: Decimal
    max_loss_budget_fraction: Decimal


@dataclass(frozen=True, slots=True)
class PortfolioRiskState:
    observed_at: datetime
    portfolio_equity: Decimal
    daily_realized_pnl: Decimal
    current_gross_notional: Decimal
    gross_notional_cap: Decimal
    allocated_notional_by_sleeve: dict[str, Decimal] = field(default_factory=dict)
    allocated_notional_by_bucket: dict[str, Decimal] = field(default_factory=dict)
    bucket_cap_fraction: Decimal = Decimal("0.25")
    daily_loss_stop_fraction: Decimal = Decimal("0.03")

    @property
    def daily_loss_stop_active(self) -> bool:
        return self.daily_realized_pnl <= -(self.portfolio_equity * self.daily_loss_stop_fraction)


@dataclass(frozen=True, slots=True)
class AllocationRun:
    run_id: str
    observed_at: datetime
    portfolio_equity: Decimal
    daily_realized_pnl: Decimal
    daily_loss_stop_active: bool
    gross_notional_cap: Decimal
    candidate_count: int
    approved_count: int
    rejected_count: int
    allocated_capital: Decimal = Decimal("0")
    expected_portfolio_ev: Decimal = Decimal("0")
    realized_pnl: Decimal | None = None
