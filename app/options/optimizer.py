from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal

from app.options.models import OptionChainSnapshot, OptionStateVector
from app.options.scenario_engine import ScenarioAssumptions, StructureScenarioSummary, summarize_structure
from app.options.structures import OptionStructure


@dataclass(frozen=True, slots=True)
class StructureScore:
    structure_name: str
    objective_value: Decimal
    summary: StructureScenarioSummary


def evaluate_candidates(
    *,
    snapshot: OptionChainSnapshot,
    state_vector: OptionStateVector,
    candidates: list[OptionStructure],
    assumptions: ScenarioAssumptions,
    tail_penalty: Decimal,
    uncertainty_penalty: Decimal,
) -> list[StructureScore]:
    scores: list[StructureScore] = []
    for structure in candidates:
        summary = summarize_structure(
            snapshot=snapshot,
            state_vector=state_vector,
            structure=structure,
            assumptions=assumptions,
        )
        scores.append(
            StructureScore(
                structure_name=structure.name,
                objective_value=_objective_value(
                    summary=summary,
                    tail_penalty=tail_penalty,
                    uncertainty_penalty=uncertainty_penalty,
                ),
                summary=summary,
            )
        )
    return sorted(scores, key=lambda item: item.objective_value, reverse=True)


def select_best_structure(
    *,
    snapshot: OptionChainSnapshot,
    state_vector: OptionStateVector,
    candidates: list[OptionStructure],
    assumptions: ScenarioAssumptions,
    tail_penalty: Decimal,
    uncertainty_penalty: Decimal,
) -> StructureScore | None:
    scores = evaluate_candidates(
        snapshot=snapshot,
        state_vector=state_vector,
        candidates=candidates,
        assumptions=assumptions,
        tail_penalty=tail_penalty,
        uncertainty_penalty=uncertainty_penalty,
    )
    return scores[0] if scores else None


def _objective_value(
    *,
    summary: StructureScenarioSummary,
    tail_penalty: Decimal,
    uncertainty_penalty: Decimal,
) -> Decimal:
    tail_cost = abs(min(summary.cvar_95_loss, Decimal("0")))
    robustness_cost = summary.model_uncertainty_penalty or Decimal("0")
    return summary.expected_pnl - (tail_penalty * tail_cost) - (uncertainty_penalty * robustness_cost)
