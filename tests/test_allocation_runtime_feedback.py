from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

from app.allocation.adapters import (
    candidate_from_kalshi_signal,
    candidate_from_options_selection,
    candidate_from_trend_target,
)
from app.allocation.calibration import build_adjustments
from app.allocation.coordinator import AllocationCoordinator, PendingAllocationAction
from app.allocation.engine import AllocationEngine
from app.allocation.feedback import AllocationFeedbackEngine
from app.allocation.models import AllocationDecision, PortfolioRiskState
from app.allocation.reporting import build_allocation_report
from app.db import Database
from app.execution.models import ExecutionOutcome, SignalLegIntent
from app.instruments.models import InstrumentRef
from app.market_data.snapshots import MarketStateInput
from app.models import Signal, SignalEdge, StrategyName
from app.options.outcomes import OptionOutcomeEvaluation
from app.options.scenario_engine import StructureScenarioSummary
from app.options.selection import StructureSelectionRecord
from app.persistence.allocation_repo import AllocationRepository
from app.persistence.kalshi_repo import KalshiRepository
from app.persistence.market_state_repo import MarketStateRepository
from app.persistence.options_repo import OptionsRepository
from app.strategies.trend.models import TrendTarget


def test_mixed_candidates_are_admitted_by_allocator_before_execution(tmp_path: Path) -> None:
    db = Database(tmp_path / "allocation_runtime.db")
    allocation_repo = AllocationRepository(db)
    coordinator = AllocationCoordinator(AllocationEngine(), allocation_repo)
    executed: list[str] = []

    pending = [
        PendingAllocationAction(candidate=_kalshi_candidate(), execute=lambda decision: _record_execute(executed, decision.candidate_id)),
        PendingAllocationAction(candidate=_trend_candidate(), execute=lambda decision: _record_execute(executed, decision.candidate_id)),
        PendingAllocationAction(candidate=_options_candidate(expected_pnl=Decimal("0.5"), cvar=Decimal("200")), execute=lambda decision: _record_execute(executed, decision.candidate_id)),
    ]

    run, decisions, execution_results = coordinator.run(
        pending=pending,
        risk_state=_risk_state(),
        adjustments=[],
    )

    assert run.candidate_count == 3
    assert len(decisions) == 3
    rejected = {decision.candidate_id for decision in decisions if not decision.approved}
    approved = {decision.candidate_id for decision in decisions if decision.approved}
    assert approved
    assert rejected
    assert set(executed) == approved
    assert all(execution_results[candidate_id] for candidate_id in approved)
    assert all(execution_results[candidate_id] is False for candidate_id in rejected)
    assert allocation_repo.load_run(run.run_id) is not None
    assert len(allocation_repo.load_decisions(run.run_id)) == 3


def test_feedback_links_decisions_to_realized_outcomes_and_round_trips(tmp_path: Path) -> None:
    db = Database(tmp_path / "allocation_feedback.db")
    allocation_repo = AllocationRepository(db)
    kalshi_repo = KalshiRepository(db)
    market_state_repo = MarketStateRepository(db)
    options_repo = OptionsRepository(db)
    coordinator = AllocationCoordinator(AllocationEngine(), allocation_repo)

    pending = [
        PendingAllocationAction(candidate=_kalshi_candidate(), execute=lambda _decision: True),
        PendingAllocationAction(candidate=_options_candidate(), execute=None),
    ]
    run, decisions, _ = coordinator.run(
        pending=pending,
        risk_state=_risk_state(),
        adjustments=[],
    )

    approved = {decision.candidate_id: decision for decision in decisions if decision.approved}
    kalshi_decision = next(decision for decision in approved.values() if decision.source_kind == "kalshi_signal")
    options_decision = next(decision for decision in approved.values() if decision.source_kind == "options_selection")

    kalshi_repo.persist_execution_outcome(
        ExecutionOutcome(
            client_order_id="kalshi-order-1",
            instrument=InstrumentRef(symbol="FED-1", venue="kalshi", contract_side="yes"),
            side="buy",
            action="limit",
            status="filled",
            requested_quantity=2,
            filled_quantity=2,
            price_cents=40,
            time_in_force="IOC",
            is_flatten=False,
            created_at=kalshi_decision.observed_at,
            metadata={
                "allocation_run_id": kalshi_decision.run_id,
                "allocation_candidate_id": kalshi_decision.candidate_id,
            },
        )
    )
    market_state_repo.persist_market_state_input(
        MarketStateInput(
            observed_at=kalshi_decision.observed_at,
            instrument=InstrumentRef(symbol="FED-1", venue="kalshi", contract_side="yes"),
            source="unit_test_mark",
            reference_kind="mark",
            reference_price=Decimal("0.47"),
            provenance="test",
            best_bid=Decimal("0.46"),
            best_ask=Decimal("0.48"),
            order_id="kalshi-order-1",
        )
    )

    selection = _options_selection()
    options_repo.persist_structure_selection(selection)
    options_repo.persist_selection_outcome(
        OptionOutcomeEvaluation(
            underlying=selection.underlying,
            decision_observed_at=selection.observed_at,
            evaluation_horizon="1d",
            evaluated_at=selection.due_at_1d,
            selected_structure_name=selection.selected_structure_name,
            expected_pnl=Decimal("25"),
            realized_return=Decimal("0.08"),
            realized_vol=Decimal("0.18"),
            realized_iv_change=Decimal("-0.03"),
            realized_pnl=Decimal("18"),
            max_drawdown_realized=Decimal("-12"),
            predicted_pnl_error=Decimal("-7"),
            move_error=Decimal("0.01"),
            vol_error=Decimal("0.02"),
            iv_error=Decimal("-0.01"),
        )
    )

    feedback = AllocationFeedbackEngine(
        allocation_repo=allocation_repo,
        kalshi_repo=kalshi_repo,
        market_state_repo=market_state_repo,
        options_repo=options_repo,
    ).refresh_latest_run()

    assert feedback is not None
    loaded_outcomes = allocation_repo.load_outcomes_for_run(run.run_id)
    assert {outcome.candidate_id for outcome in loaded_outcomes} == {kalshi_decision.candidate_id, options_decision.candidate_id}
    assert any(outcome.realized_pnl == Decimal("0.14") for outcome in loaded_outcomes)
    assert any(outcome.realized_pnl == Decimal("18") for outcome in loaded_outcomes)
    assert allocation_repo.load_latest_adjustments()


def test_calibration_adjustments_are_clipped_and_increase_after_underperformance() -> None:
    decision = _approved_decision("candidate-a")
    outcomes = [
        _allocation_outcome(decision, realized_pnl=Decimal("-20"), realized_drawdown=Decimal("-180")),
        _allocation_outcome(decision, realized_pnl=Decimal("-10"), realized_drawdown=Decimal("-160"), candidate_suffix="b"),
    ]

    adjustments = build_adjustments(decision.observed_at, outcomes)
    sleeve_adjustment = next(item for item in adjustments if item.scope_type == "sleeve")

    assert sleeve_adjustment.tail_risk_multiplier > Decimal("1")
    assert sleeve_adjustment.uncertainty_multiplier_adjustment > Decimal("1")
    assert sleeve_adjustment.tail_risk_multiplier <= Decimal("1.50")
    assert sleeve_adjustment.uncertainty_multiplier_adjustment <= Decimal("1.50")
    assert sleeve_adjustment.pnl_bias_multiplier >= Decimal("0.50")

    positive = build_adjustments(
        decision.observed_at,
        [
            _allocation_outcome(decision, realized_pnl=Decimal("200"), realized_drawdown=Decimal("-20")),
            _allocation_outcome(decision, realized_pnl=Decimal("220"), realized_drawdown=Decimal("-25"), candidate_suffix="c"),
        ],
    )
    positive_sleeve = next(item for item in positive if item.scope_type == "sleeve")
    assert positive_sleeve.pnl_bias_multiplier <= Decimal("1.50")


def test_allocation_report_includes_sleeves_rejections_and_expected_vs_realized(tmp_path: Path) -> None:
    db = Database(tmp_path / "allocation_report.db")
    allocation_repo = AllocationRepository(db)
    coordinator = AllocationCoordinator(AllocationEngine(), allocation_repo)

    run, decisions, _ = coordinator.run(
        pending=[
            PendingAllocationAction(candidate=_kalshi_candidate(), execute=lambda _decision: True),
            PendingAllocationAction(candidate=_options_candidate(expected_pnl=Decimal("1"), cvar=Decimal("300")), execute=None),
        ],
        risk_state=_risk_state(),
        adjustments=[],
    )
    allocation_repo.persist_outcomes([
        _allocation_outcome(
            next(decision for decision in decisions if decision.approved),
            realized_pnl=Decimal("12"),
            realized_drawdown=Decimal("-20"),
        )
    ])
    adjustments = build_adjustments(run.observed_at, allocation_repo.load_outcomes())
    allocation_repo.persist_adjustments(adjustments)

    report = build_allocation_report(
        run=allocation_repo.load_latest_run(),
        decisions=allocation_repo.load_decisions(run.run_id),
        outcomes=allocation_repo.load_outcomes_for_run(run.run_id),
        adjustments=allocation_repo.load_latest_adjustments(),
    )

    assert "capital_by_sleeve" in report
    assert "rejections_by_reason" in report
    assert "expected_vs_realized" in report
    assert report["run"]["candidate_count"] == 2
    assert report["expected_vs_realized"]["realized_pnl"] == "12"


def _record_execute(executed: list[str], candidate_id: str) -> bool:
    executed.append(candidate_id)
    return True


def _risk_state() -> PortfolioRiskState:
    return PortfolioRiskState(
        observed_at=datetime(2026, 3, 22, 12, 0, tzinfo=timezone.utc),
        portfolio_equity=Decimal("100000"),
        daily_realized_pnl=Decimal("0"),
        current_gross_notional=Decimal("10000"),
        gross_notional_cap=Decimal("100000"),
        allocated_notional_by_sleeve={},
        allocated_notional_by_bucket={},
    )


def _kalshi_candidate():
    signal = Signal(
        strategy=StrategyName.CROSS_MARKET,
        ticker="FED-1",
        action="buy_yes",
        quantity=2,
        edge=SignalEdge.from_values(edge_before_fees="0.04", edge_after_fees="0.03", fee_estimate="0.01"),
        source="cross_market_probability_map",
        confidence=0.8,
        priority=1,
        legs=[SignalLegIntent(instrument=InstrumentRef(symbol="FED-1", venue="kalshi", contract_side="yes"), side="buy", order_type="limit", limit_price=Decimal("0.40"), time_in_force="IOC")],
        ts=datetime(2026, 3, 22, 12, 0, tzinfo=timezone.utc),
    )
    return candidate_from_kalshi_signal(
        signal,
        cvar_95_loss=Decimal("60"),
        uncertainty_penalty=Decimal("0.20"),
        liquidity_penalty=Decimal("0.05"),
        horizon_days=14,
        correlation_bucket="macro",
    )


def _trend_candidate():
    return candidate_from_trend_target(
        TrendTarget(
            symbol="SPY",
            momentum_return=Decimal("0.03"),
            realized_volatility=Decimal("0.15"),
            target_weight=Decimal("0.20"),
        ),
        observed_at=datetime(2026, 3, 22, 12, 0, tzinfo=timezone.utc),
        portfolio_equity=Decimal("100000"),
    )


def _options_candidate(*, expected_pnl: Decimal = Decimal("25"), cvar: Decimal = Decimal("60")):
    return candidate_from_options_selection(_options_selection(), _options_summary(expected_pnl=expected_pnl, cvar=cvar))


def _options_selection() -> StructureSelectionRecord:
    return StructureSelectionRecord(
        underlying="SPY",
        observed_at=datetime(2026, 3, 22, 12, 0, tzinfo=timezone.utc),
        source="options_test",
        regime_name="event_term_structure",
        due_at_1d=datetime(2026, 3, 23, 12, 0, tzinfo=timezone.utc),
        due_at_3d=datetime(2026, 3, 25, 12, 0, tzinfo=timezone.utc),
        due_at_expiry=datetime(2026, 4, 17, 12, 0, tzinfo=timezone.utc),
        candidate_count=3,
        selected_structure_name="call_calendar_500",
        objective_value=Decimal("25"),
        horizon_days=7,
        path_count=200,
        seed=1,
        annual_drift=Decimal("0"),
        annual_volatility=Decimal("0.10"),
        event_day=1,
        event_jump_mean=Decimal("0"),
        event_jump_stddev=Decimal("0.02"),
        iv_mean_reversion=Decimal("0.15"),
        iv_regime_level=Decimal("0.20"),
        iv_volatility=Decimal("0.10"),
        event_crush=Decimal("0.06"),
        exit_spread_fraction=Decimal("0.50"),
        slippage_bps=Decimal("10"),
        stop_out_loss=None,
        tail_penalty=Decimal("0.50"),
        uncertainty_penalty=Decimal("1.00"),
    )


def _options_summary(*, expected_pnl: Decimal, cvar: Decimal) -> StructureScenarioSummary:
    return StructureScenarioSummary(
        underlying="SPY",
        observed_at=datetime(2026, 3, 22, 12, 0, tzinfo=timezone.utc),
        source="options_test",
        structure_name="call_calendar_500",
        horizon_days=7,
        path_count=200,
        seed=1,
        expected_pnl=expected_pnl,
        probability_of_profit=Decimal("0.62"),
        cvar_95_loss=-abs(cvar),
        expected_max_drawdown=Decimal("40"),
        stop_out_probability=Decimal("0.15"),
        predicted_daily_vol=Decimal("12"),
        expected_entry_cost=Decimal("200"),
        stress_expected_pnl=expected_pnl - Decimal("10"),
        model_uncertainty_penalty=Decimal("10"),
    )


def _approved_decision(candidate_id: str):
    return AllocationDecision(
        run_id="alloc:test",
        candidate_id=candidate_id,
        strategy_family="cross_market",
        source_kind="kalshi_signal",
        sleeve="opportunistic",
        regime_name="cross_market_probability_map",
        observed_at=datetime(2026, 3, 22, 12, 0, tzinfo=timezone.utc),
        score=Decimal("0.20"),
        approved=True,
        rejection_reason=None,
        target_notional=Decimal("100"),
        max_loss_budget=Decimal("60"),
        expected_pnl_after_costs=Decimal("20"),
        cvar_95_loss=Decimal("100"),
        uncertainty_penalty=Decimal("0.20"),
        liquidity_penalty=Decimal("0.05"),
        horizon_days=14,
        correlation_bucket="macro",
    )


def _allocation_outcome(decision, *, realized_pnl: Decimal, realized_drawdown: Decimal, candidate_suffix: str = ""):
    from app.allocation.outcomes import AllocationOutcomeRecord

    return AllocationOutcomeRecord(
        candidate_id=decision.candidate_id + candidate_suffix,
        allocation_run_id=decision.run_id,
        observed_at=decision.observed_at,
        outcome_horizon_days=1,
        realized_pnl=realized_pnl,
        realized_return_bps=Decimal("25"),
        realized_max_drawdown=realized_drawdown,
        realized_vol_or_risk_proxy=Decimal("5"),
        expected_pnl_at_decision=decision.expected_pnl_after_costs,
        expected_cvar_at_decision=decision.cvar_95_loss,
        uncertainty_penalty_at_decision=decision.uncertainty_penalty,
        sleeve=decision.sleeve,
        correlation_bucket=decision.correlation_bucket,
        regime_name=decision.regime_name,
        approved=decision.approved,
    )
