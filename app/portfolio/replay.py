from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal

from app.allocation.models import AllocationDecision, AllocationRun
from app.execution.models import ExecutionOutcome, OrderIntent
from app.market_data.snapshots import MarketStateInput
from app.persistence.allocation_repo import AllocationRepository
from app.persistence.kalshi_repo import KalshiRepository
from app.persistence.market_state_repo import MarketStateRepository
from app.persistence.portfolio_cycle_repo import PortfolioCycleRepository
from app.replay_execution import ExecutionReplayEngine, ReplayResult
from app.persistence.contracts import PortfolioCycleSnapshotRecord


@dataclass(frozen=True, slots=True)
class PortfolioReplayResult:
    cycle_snapshot: PortfolioCycleSnapshotRecord | None
    run: AllocationRun
    decisions: list[AllocationDecision]
    order_intents: list[OrderIntent]
    execution_outcomes: list[ExecutionOutcome]
    market_state_inputs: list[MarketStateInput]
    replay: ReplayResult


class PortfolioReplayEngine:
    def __init__(
        self,
        *,
        allocation_repo: AllocationRepository,
        kalshi_repo: KalshiRepository,
        market_state_repo: MarketStateRepository,
        portfolio_cycle_repo: PortfolioCycleRepository | None = None,
        execution_replay_engine: ExecutionReplayEngine | None = None,
    ) -> None:
        self.allocation_repo = allocation_repo
        self.kalshi_repo = kalshi_repo
        self.market_state_repo = market_state_repo
        self.portfolio_cycle_repo = portfolio_cycle_repo
        self.execution_replay_engine = execution_replay_engine or ExecutionReplayEngine()

    def replay_run(self, run_id: str) -> PortfolioReplayResult:
        run = self.allocation_repo.load_run(run_id)
        if run is None:
            raise ValueError(f"unknown_allocation_run:{run_id}")
        decisions = self.allocation_repo.load_decisions(run_id)
        candidate_ids = {decision.candidate_id for decision in decisions}
        order_intents = [
            intent
            for intent in self.kalshi_repo.load_order_intents()
            if any(
                outcome.client_order_id == intent.order_id
                for outcome in self._run_execution_outcomes(run_id, candidate_ids)
            )
        ]
        execution_outcomes = self._run_execution_outcomes(run_id, candidate_ids)
        market_state_inputs = [
            item
            for item in self.market_state_repo.load_market_state_inputs()
            if item.order_id is not None and any(outcome.client_order_id == item.order_id for outcome in execution_outcomes)
        ]
        replay = self.execution_replay_engine.replay(
            signals=self.kalshi_repo.load_signals(),
            order_intents=order_intents,
            execution_outcomes=execution_outcomes,
            market_state_inputs=market_state_inputs,
            starting_cash=run.portfolio_equity,
        )
        return PortfolioReplayResult(
            cycle_snapshot=None if self.portfolio_cycle_repo is None else self.portfolio_cycle_repo.load_snapshot(run_id),
            run=run,
            decisions=decisions,
            order_intents=order_intents,
            execution_outcomes=execution_outcomes,
            market_state_inputs=market_state_inputs,
            replay=replay,
        )

    def _run_execution_outcomes(self, run_id: str, candidate_ids: set[str]) -> list[ExecutionOutcome]:
        return [
            outcome
            for outcome in self.kalshi_repo.load_execution_outcomes()
            if str(outcome.metadata.get("allocation_run_id", "")) == run_id
            and str(outcome.metadata.get("allocation_candidate_id", "")) in candidate_ids
        ]
