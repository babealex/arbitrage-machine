from __future__ import annotations

from dataclasses import dataclass
from typing import Callable

from app.allocation.engine import AllocationEngine
from app.allocation.models import AllocationDecision, AllocationRun, PortfolioRiskState, TradeCandidate
from app.allocation.outcomes import AllocationAdjustment
from app.persistence.allocation_repo import AllocationRepository


@dataclass(frozen=True, slots=True)
class PendingAllocationAction:
    candidate: TradeCandidate
    execute: Callable[[AllocationDecision], bool] | None = None


class AllocationCoordinator:
    def __init__(self, engine: AllocationEngine, repo: AllocationRepository) -> None:
        self.engine = engine
        self.repo = repo

    def run(
        self,
        *,
        pending: list[PendingAllocationAction],
        risk_state: PortfolioRiskState,
        adjustments: list[AllocationAdjustment] | None = None,
    ) -> tuple[AllocationRun, list[AllocationDecision], dict[str, bool]]:
        run, decisions = self.engine.run(
            candidates=[item.candidate for item in pending],
            risk_state=risk_state,
            adjustments=adjustments or [],
        )
        self.repo.persist_run(run)
        self.repo.persist_decisions(decisions)
        execution_results: dict[str, bool] = {}
        action_by_candidate = {item.candidate.candidate_id: item for item in pending}
        for decision in decisions:
            action = action_by_candidate[decision.candidate_id]
            if not decision.approved or action.execute is None:
                execution_results[decision.candidate_id] = False
                continue
            execution_results[decision.candidate_id] = action.execute(decision)
        return run, decisions, execution_results
