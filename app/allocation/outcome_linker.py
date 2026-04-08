from __future__ import annotations

from decimal import Decimal

from app.allocation.models import AllocationDecision
from app.allocation.outcomes import AllocationOutcomeRecord
from app.execution.models import ExecutionOutcome
from app.market_data.snapshots import MarketStateInput
from app.options.outcomes import OptionOutcomeEvaluation


def link_execution_outcomes(
    *,
    decisions: list[AllocationDecision],
    execution_outcomes: list[ExecutionOutcome],
    market_state_inputs: list[MarketStateInput],
    outcome_horizon_days: int = 1,
) -> list[AllocationOutcomeRecord]:
    decision_map = {decision.candidate_id: decision for decision in decisions}
    market_state_by_order = {}
    for row in market_state_inputs:
        if row.order_id is None:
            continue
        current = market_state_by_order.get(row.order_id)
        if current is None or row.observed_at > current.observed_at:
            market_state_by_order[row.order_id] = row
    outcomes: list[AllocationOutcomeRecord] = []
    for execution in execution_outcomes:
        candidate_id = str(execution.metadata.get("allocation_candidate_id", ""))
        run_id = str(execution.metadata.get("allocation_run_id", ""))
        if not candidate_id or candidate_id not in decision_map:
            continue
        decision = decision_map[candidate_id]
        if execution.price_cents is None or execution.filled_quantity <= 0:
            realized_pnl = Decimal("0")
            realized_return_bps = Decimal("0")
            risk_proxy = Decimal("0")
        else:
            fill_price = Decimal(execution.price_cents) / Decimal("100")
            state = market_state_by_order.get(execution.client_order_id)
            mark_price = fill_price if state is None else state.reference_price
            direction = Decimal("1") if execution.side == "buy" else Decimal("-1")
            realized_pnl = (mark_price - fill_price) * Decimal(execution.filled_quantity) * direction
            realized_return_bps = Decimal("0") if decision.target_notional == 0 else (realized_pnl / decision.target_notional) * Decimal("10000")
            risk_proxy = abs(mark_price - fill_price)
        outcomes.append(
            AllocationOutcomeRecord(
                candidate_id=decision.candidate_id,
                allocation_run_id=run_id or decision.run_id,
                observed_at=execution.created_at,
                outcome_horizon_days=outcome_horizon_days,
                realized_pnl=realized_pnl,
                realized_return_bps=realized_return_bps,
                realized_max_drawdown=min(Decimal("0"), realized_pnl),
                realized_vol_or_risk_proxy=risk_proxy,
                expected_pnl_at_decision=decision.expected_pnl_after_costs,
                expected_cvar_at_decision=decision.cvar_95_loss,
                uncertainty_penalty_at_decision=decision.uncertainty_penalty,
                sleeve=decision.sleeve,
                correlation_bucket=decision.correlation_bucket,
                regime_name=decision.regime_name,
                approved=decision.approved,
            )
        )
    return outcomes


def link_options_outcomes(
    *,
    decisions: list[AllocationDecision],
    option_outcomes: list[OptionOutcomeEvaluation],
) -> list[AllocationOutcomeRecord]:
    decision_map = {decision.candidate_id: decision for decision in decisions}
    results: list[AllocationOutcomeRecord] = []
    for outcome in option_outcomes:
        candidate_id = f"options:{outcome.underlying}:{outcome.decision_observed_at.isoformat()}:{outcome.selected_structure_name}"
        decision = decision_map.get(candidate_id)
        if decision is None:
            continue
        horizon_days = 1 if outcome.evaluation_horizon == "1d" else 3 if outcome.evaluation_horizon == "3d" else decision.horizon_days
        results.append(
            AllocationOutcomeRecord(
                candidate_id=candidate_id,
                allocation_run_id=decision.run_id,
                observed_at=outcome.evaluated_at,
                outcome_horizon_days=horizon_days,
                realized_pnl=outcome.realized_pnl or Decimal("0"),
                realized_return_bps=outcome.realized_return * Decimal("10000"),
                realized_max_drawdown=outcome.max_drawdown_realized,
                realized_vol_or_risk_proxy=outcome.realized_vol,
                expected_pnl_at_decision=decision.expected_pnl_after_costs,
                expected_cvar_at_decision=decision.cvar_95_loss,
                uncertainty_penalty_at_decision=decision.uncertainty_penalty,
                sleeve=decision.sleeve,
                correlation_bucket=decision.correlation_bucket,
                regime_name=decision.regime_name,
                approved=decision.approved,
            )
        )
    return results
