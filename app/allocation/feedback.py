from __future__ import annotations

from dataclasses import dataclass

from app.allocation.calibration import build_adjustments, build_calibration_summary
from app.allocation.outcome_linker import link_execution_outcomes, link_options_outcomes
from app.allocation.outcomes import AllocationAdjustment, AllocationOutcomeRecord, CalibrationSummary
from app.persistence.allocation_repo import AllocationRepository
from app.persistence.kalshi_repo import KalshiRepository
from app.persistence.market_state_repo import MarketStateRepository
from app.persistence.options_repo import OptionsRepository


@dataclass(frozen=True, slots=True)
class AllocationFeedbackResult:
    summary: CalibrationSummary
    outcomes: list[AllocationOutcomeRecord]
    adjustments: list[AllocationAdjustment]


class AllocationFeedbackEngine:
    def __init__(
        self,
        *,
        allocation_repo: AllocationRepository,
        kalshi_repo: KalshiRepository,
        market_state_repo: MarketStateRepository,
        options_repo: OptionsRepository,
    ) -> None:
        self.allocation_repo = allocation_repo
        self.kalshi_repo = kalshi_repo
        self.market_state_repo = market_state_repo
        self.options_repo = options_repo

    def refresh_latest_run(self) -> AllocationFeedbackResult | None:
        run = self.allocation_repo.load_latest_run()
        if run is None:
            return None
        return self.refresh_run(run.run_id)

    def refresh_run(self, run_id: str) -> AllocationFeedbackResult | None:
        run = self.allocation_repo.load_run(run_id)
        if run is None:
            return None
        decisions = self.allocation_repo.load_decisions(run.run_id)
        if not decisions:
            return None
        outcomes = self._link_outcomes(decisions)
        self.allocation_repo.persist_outcomes(outcomes)
        all_outcomes = self.allocation_repo.load_outcomes()
        summary = build_calibration_summary(run.observed_at, all_outcomes)
        adjustments = build_adjustments(run.observed_at, all_outcomes)
        self.allocation_repo.persist_adjustments(adjustments)
        return AllocationFeedbackResult(summary=summary, outcomes=outcomes, adjustments=adjustments)

    def _link_outcomes(self, decisions) -> list[AllocationOutcomeRecord]:
        execution_outcomes = link_execution_outcomes(
            decisions=decisions,
            execution_outcomes=self.kalshi_repo.load_execution_outcomes(),
            market_state_inputs=self.market_state_repo.load_market_state_inputs(),
        )
        option_outcomes = link_options_outcomes(
            decisions=decisions,
            option_outcomes=self.options_repo.load_all_selection_outcomes(),
        )
        # Keep the latest per candidate/horizon when both execution and options paths exist.
        keyed: dict[tuple[str, int], AllocationOutcomeRecord] = {}
        for outcome in execution_outcomes + option_outcomes:
            key = (outcome.candidate_id, outcome.outcome_horizon_days)
            previous = keyed.get(key)
            if previous is None or outcome.observed_at >= previous.observed_at:
                keyed[key] = outcome
        return [keyed[key] for key in sorted(keyed.keys(), key=lambda item: (item[0], item[1]))]
