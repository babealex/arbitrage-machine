from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal

from app.options.optimizer import StructureScore
from app.options.outcomes import OptionOutcomeEvaluation
from app.options.selection import StructureSelectionRecord


@dataclass(frozen=True, slots=True)
class OptionsDecisionReport:
    underlying: str
    regime_name: str
    candidate_count: int
    selected_structure_name: str
    selected_objective_value: Decimal
    selected_expected_pnl: Decimal
    selected_cvar_95_loss: Decimal
    selected_model_uncertainty_penalty: Decimal
    best_alternative_name: str | None
    best_alternative_objective_value: Decimal | None
    realized_pnl: Decimal | None
    prediction_error: Decimal | None
    latest_outcome_horizon: str | None


def build_decision_report(
    *,
    selection: StructureSelectionRecord,
    candidate_scores: list[StructureScore],
    latest_outcome: OptionOutcomeEvaluation | None = None,
) -> OptionsDecisionReport:
    alternative = candidate_scores[1] if len(candidate_scores) > 1 else None
    selected = candidate_scores[0]
    return OptionsDecisionReport(
        underlying=selection.underlying,
        regime_name=selection.regime_name,
        candidate_count=selection.candidate_count,
        selected_structure_name=selection.selected_structure_name,
        selected_objective_value=selection.objective_value,
        selected_expected_pnl=selected.summary.expected_pnl,
        selected_cvar_95_loss=selected.summary.cvar_95_loss,
        selected_model_uncertainty_penalty=selected.summary.model_uncertainty_penalty or Decimal("0"),
        best_alternative_name=None if alternative is None else alternative.structure_name,
        best_alternative_objective_value=None if alternative is None else alternative.objective_value,
        realized_pnl=None if latest_outcome is None else latest_outcome.realized_pnl,
        prediction_error=None if latest_outcome is None else latest_outcome.predicted_pnl_error,
        latest_outcome_horizon=None if latest_outcome is None else latest_outcome.evaluation_horizon,
    )
