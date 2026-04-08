from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from decimal import Decimal

from app.allocation.outcomes import AllocationAdjustment, AllocationOutcomeRecord, CalibrationSummary


def build_calibration_summary(observed_at, outcomes: list[AllocationOutcomeRecord]) -> CalibrationSummary:
    approved = [item for item in outcomes if item.approved]
    hit_rate = Decimal("0")
    if approved:
        hit_rate = Decimal(sum(1 for item in approved if item.realized_pnl > 0)) / Decimal(len(approved))
    avg_expected = _avg([item.expected_pnl_at_decision for item in approved])
    avg_realized = _avg([item.realized_pnl for item in approved])
    avg_tail_error = _avg([(abs(item.realized_max_drawdown or Decimal("0")) - abs(item.expected_cvar_at_decision)) for item in approved])
    avg_prediction_error = _avg([(item.realized_pnl - item.expected_pnl_at_decision) for item in approved])
    return CalibrationSummary(
        observed_at=observed_at,
        total_outcomes=len(outcomes),
        approved_count=len(approved),
        rejected_count=len([item for item in outcomes if not item.approved]),
        hit_rate=hit_rate,
        avg_expected_pnl=avg_expected,
        avg_realized_pnl=avg_realized,
        avg_tail_error=avg_tail_error,
        avg_prediction_error=avg_prediction_error,
    )


def build_adjustments(observed_at, outcomes: list[AllocationOutcomeRecord]) -> list[AllocationAdjustment]:
    grouped: dict[tuple[str, str], list[AllocationOutcomeRecord]] = defaultdict(list)
    for outcome in outcomes:
        grouped[("sleeve", outcome.sleeve)].append(outcome)
        grouped[("bucket", outcome.correlation_bucket)].append(outcome)
        grouped[("regime", outcome.regime_name)].append(outcome)
    adjustments: list[AllocationAdjustment] = []
    for (scope_type, scope_key), values in sorted(grouped.items()):
        approved = [value for value in values if value.approved]
        if not approved:
            continue
        avg_expected = _avg([value.expected_pnl_at_decision for value in approved])
        avg_realized = _avg([value.realized_pnl for value in approved])
        pnl_bias = _clip_decimal(Decimal("1") if avg_expected == 0 else max(Decimal("0.5"), min(Decimal("1.5"), avg_realized / avg_expected)))
        tail_ratio = _avg([
            (abs(value.realized_max_drawdown or Decimal("0")) / abs(value.expected_cvar_at_decision))
            for value in approved
            if value.expected_cvar_at_decision != 0
        ])
        if tail_ratio == 0:
            tail_ratio = Decimal("1")
        tail_mult = _clip_decimal(tail_ratio, lower=Decimal("0.75"), upper=Decimal("1.50"))
        prediction_error_ratio = _avg([
            abs(value.realized_pnl - value.expected_pnl_at_decision) / max(Decimal("1"), abs(value.expected_pnl_at_decision))
            for value in approved
        ])
        uncertainty_adj = _clip_decimal(Decimal("1") + prediction_error_ratio, lower=Decimal("0.75"), upper=Decimal("1.50"))
        adjustments.append(
            AllocationAdjustment(
                scope_type=scope_type,
                scope_key=scope_key,
                observed_at=observed_at,
                sample_size=len(approved),
                pnl_bias_multiplier=pnl_bias,
                tail_risk_multiplier=tail_mult,
                uncertainty_multiplier_adjustment=uncertainty_adj,
            )
        )
    return adjustments


def _avg(values: list[Decimal]) -> Decimal:
    if not values:
        return Decimal("0")
    return sum(values, Decimal("0")) / Decimal(len(values))


def _clip_decimal(value: Decimal, *, lower: Decimal = Decimal("0.50"), upper: Decimal = Decimal("1.50")) -> Decimal:
    return max(lower, min(upper, value))
