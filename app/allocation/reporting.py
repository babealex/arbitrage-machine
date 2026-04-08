from __future__ import annotations

from collections import defaultdict
from decimal import Decimal

from app.allocation.outcomes import AllocationAdjustment, AllocationOutcomeRecord
from app.allocation.models import AllocationDecision, AllocationRun


def build_allocation_report(
    *,
    run: AllocationRun | None,
    decisions: list[AllocationDecision],
    outcomes: list[AllocationOutcomeRecord],
    adjustments: list[AllocationAdjustment],
) -> dict:
    by_sleeve = defaultdict(lambda: {"approved": 0, "rejected": 0, "target_notional": Decimal("0"), "realized_pnl": Decimal("0")})
    by_strategy = defaultdict(lambda: {"approved": 0, "rejected": 0, "realized_pnl": Decimal("0"), "hit_count": 0, "outcome_count": 0})
    rejection_reasons = defaultdict(int)
    by_regime = defaultdict(lambda: {"realized_pnl": Decimal("0"), "count": 0})
    by_bucket = defaultdict(lambda: {"realized_pnl": Decimal("0"), "count": 0})
    for decision in decisions:
        bucket = by_sleeve[decision.sleeve]
        strategy_bucket = by_strategy[decision.strategy_family]
        if decision.approved:
            bucket["approved"] += 1
            bucket["target_notional"] += decision.target_notional
            strategy_bucket["approved"] += 1
        else:
            bucket["rejected"] += 1
            strategy_bucket["rejected"] += 1
            rejection_reasons[decision.rejection_reason or "unknown"] += 1
    expected_total = Decimal("0")
    realized_total = Decimal("0")
    tail_total = Decimal("0")
    for outcome in outcomes:
        expected_total += outcome.expected_pnl_at_decision
        realized_total += outcome.realized_pnl
        tail_total += outcome.realized_max_drawdown or Decimal("0")
        by_sleeve[outcome.sleeve]["realized_pnl"] += outcome.realized_pnl
        strategy_rows = [decision for decision in decisions if decision.candidate_id == outcome.candidate_id]
        if strategy_rows:
            strategy_bucket = by_strategy[strategy_rows[0].strategy_family]
            strategy_bucket["realized_pnl"] += outcome.realized_pnl
            strategy_bucket["outcome_count"] += 1
            if outcome.realized_pnl > 0:
                strategy_bucket["hit_count"] += 1
        by_regime[outcome.regime_name]["realized_pnl"] += outcome.realized_pnl
        by_regime[outcome.regime_name]["count"] += 1
        by_bucket[outcome.correlation_bucket]["realized_pnl"] += outcome.realized_pnl
        by_bucket[outcome.correlation_bucket]["count"] += 1
    return {
        "run": None if run is None else {
            "run_id": run.run_id,
            "candidate_count": run.candidate_count,
            "approved_count": run.approved_count,
            "rejected_count": run.rejected_count,
            "daily_loss_stop_active": run.daily_loss_stop_active,
            "allocated_capital": format(run.allocated_capital, "f"),
            "expected_portfolio_ev": format(run.expected_portfolio_ev, "f"),
            "realized_pnl": None if run.realized_pnl is None else format(run.realized_pnl, "f"),
        },
        "capital_by_sleeve": {
            key: {
                "approved": value["approved"],
                "rejected": value["rejected"],
                "target_notional": format(value["target_notional"], "f"),
                "realized_pnl": format(value["realized_pnl"], "f"),
            }
            for key, value in by_sleeve.items()
        },
        "rejections_by_reason": dict(rejection_reasons),
        "expected_vs_realized": {
            "expected_pnl": format(expected_total, "f"),
            "realized_pnl": format(realized_total, "f"),
            "realized_tail": format(tail_total, "f"),
            "allocation_efficiency": format(Decimal("0") if expected_total == 0 else realized_total / expected_total, "f"),
        },
        "by_strategy": {
            key: {
                "approved": value["approved"],
                "rejected": value["rejected"],
                "realized_pnl": format(value["realized_pnl"], "f"),
                "hit_rate": format(Decimal("0") if value["outcome_count"] == 0 else Decimal(value["hit_count"]) / Decimal(value["outcome_count"]), "f"),
            }
            for key, value in by_strategy.items()
        },
        "realized_by_regime": {key: {"realized_pnl": format(value["realized_pnl"], "f"), "count": value["count"]} for key, value in by_regime.items()},
        "realized_by_bucket": {key: {"realized_pnl": format(value["realized_pnl"], "f"), "count": value["count"]} for key, value in by_bucket.items()},
        "current_adjustments": [
            {
                "scope_type": item.scope_type,
                "scope_key": item.scope_key,
                "sample_size": item.sample_size,
                "pnl_bias_multiplier": format(item.pnl_bias_multiplier, "f"),
                "tail_risk_multiplier": format(item.tail_risk_multiplier, "f"),
                "uncertainty_multiplier_adjustment": format(item.uncertainty_multiplier_adjustment, "f"),
            }
            for item in adjustments
        ],
        "top_overconfident_patterns": [
            {
                "candidate_id": item.candidate_id,
                "sleeve": item.sleeve,
                "regime": item.regime_name,
                "prediction_error": format(item.realized_pnl - item.expected_pnl_at_decision, "f"),
            }
            for item in sorted(outcomes, key=lambda row: row.realized_pnl - row.expected_pnl_at_decision)[:5]
        ],
        "top_underallocated_patterns": [
            {
                "candidate_id": item.candidate_id,
                "sleeve": item.sleeve,
                "regime": item.regime_name,
                "realized_pnl": format(item.realized_pnl, "f"),
            }
            for item in sorted([row for row in outcomes if not row.approved], key=lambda row: row.realized_pnl, reverse=True)[:5]
        ],
    }
