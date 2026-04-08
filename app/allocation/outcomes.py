from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal


@dataclass(frozen=True, slots=True)
class AllocationOutcomeRecord:
    candidate_id: str
    allocation_run_id: str
    observed_at: datetime
    outcome_horizon_days: int
    realized_pnl: Decimal
    realized_return_bps: Decimal | None
    realized_max_drawdown: Decimal | None
    realized_vol_or_risk_proxy: Decimal | None
    expected_pnl_at_decision: Decimal
    expected_cvar_at_decision: Decimal
    uncertainty_penalty_at_decision: Decimal
    sleeve: str
    correlation_bucket: str
    regime_name: str
    approved: bool


@dataclass(frozen=True, slots=True)
class AllocationAdjustment:
    scope_type: str
    scope_key: str
    observed_at: datetime
    sample_size: int
    pnl_bias_multiplier: Decimal
    tail_risk_multiplier: Decimal
    uncertainty_multiplier_adjustment: Decimal


@dataclass(frozen=True, slots=True)
class CalibrationSummary:
    observed_at: datetime
    total_outcomes: int
    approved_count: int
    rejected_count: int
    hit_rate: Decimal
    avg_expected_pnl: Decimal
    avg_realized_pnl: Decimal
    avg_tail_error: Decimal
    avg_prediction_error: Decimal
