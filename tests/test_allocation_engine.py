from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

from app.allocation.adapters import (
    candidate_from_kalshi_signal,
    candidate_from_options_selection,
    candidate_from_trend_target,
)
from app.allocation.engine import AllocationEngine
from app.allocation.models import PortfolioRiskState, TradeCandidate
from app.db import Database
from app.execution.models import SignalLegIntent
from app.instruments.models import InstrumentRef
from app.models import Signal, SignalEdge, StrategyName
from app.options.scenario_engine import StructureScenarioSummary
from app.options.selection import StructureSelectionRecord
from app.persistence.allocation_repo import AllocationRepository
from app.strategies.trend.models import TrendTarget


def test_higher_edge_same_tail_gets_higher_allocation() -> None:
    engine = AllocationEngine()
    risk = _risk_state()
    low = _candidate("low", expected="20", cvar="100")
    high = _candidate("high", expected="40", cvar="100")
    run, decisions = engine.run(candidates=[low, high], risk_state=risk)
    approved = {d.candidate_id: d for d in decisions if d.approved}
    assert run.approved_count == 2
    assert approved["high"].target_notional > approved["low"].target_notional


def test_higher_uncertainty_lowers_allocation() -> None:
    engine = AllocationEngine()
    risk = _risk_state()
    stable = _candidate("stable", expected="30", cvar="90", uncertainty="0.10")
    noisy = _candidate("noisy", expected="30", cvar="90", uncertainty="0.80")
    _run, decisions = engine.run(candidates=[stable, noisy], risk_state=risk)
    approved = {d.candidate_id: d for d in decisions if d.approved}
    assert approved["stable"].target_notional > approved["noisy"].target_notional


def test_correlated_exposure_cap_rejects_additional_trade() -> None:
    engine = AllocationEngine()
    risk = PortfolioRiskState(
        observed_at=datetime(2026, 3, 22, 12, 0, tzinfo=timezone.utc),
        portfolio_equity=Decimal("100000"),
        daily_realized_pnl=Decimal("0"),
        current_gross_notional=Decimal("10000"),
        gross_notional_cap=Decimal("100000"),
        allocated_notional_by_sleeve={"opportunistic": Decimal("0")},
        allocated_notional_by_bucket={"macro": Decimal("25000")},
        bucket_cap_fraction=Decimal("0.25"),
    )
    candidate = _candidate("macro_2", expected="35", cvar="80", bucket="macro")
    _run, decisions = engine.run(candidates=[candidate], risk_state=risk)
    assert decisions[0].approved is False
    assert decisions[0].rejection_reason == "correlated_exposure_limit"


def test_sleeve_caps_work() -> None:
    engine = AllocationEngine()
    risk = PortfolioRiskState(
        observed_at=datetime(2026, 3, 22, 12, 0, tzinfo=timezone.utc),
        portfolio_equity=Decimal("100000"),
        daily_realized_pnl=Decimal("0"),
        current_gross_notional=Decimal("10000"),
        gross_notional_cap=Decimal("100000"),
        allocated_notional_by_sleeve={"attack": Decimal("15000")},
        allocated_notional_by_bucket={},
    )
    candidate = _candidate("attack_1", sleeve="attack", expected="40", cvar="120")
    _run, decisions = engine.run(candidates=[candidate], risk_state=risk)
    assert decisions[0].approved is False
    assert decisions[0].rejection_reason == "sleeve_exhausted"


def test_daily_loss_stop_blocks_new_allocations() -> None:
    engine = AllocationEngine()
    risk = PortfolioRiskState(
        observed_at=datetime(2026, 3, 22, 12, 0, tzinfo=timezone.utc),
        portfolio_equity=Decimal("100000"),
        daily_realized_pnl=Decimal("-4000"),
        current_gross_notional=Decimal("10000"),
        gross_notional_cap=Decimal("100000"),
        allocated_notional_by_sleeve={},
        allocated_notional_by_bucket={},
    )
    candidate = _candidate("blocked", expected="30", cvar="100")
    run, decisions = engine.run(candidates=[candidate], risk_state=risk)
    assert run.daily_loss_stop_active is True
    assert decisions[0].approved is False
    assert decisions[0].rejection_reason == "daily_loss_stop_active"


def test_allocation_records_round_trip_and_mixed_candidates_score_together(tmp_path: Path) -> None:
    db = Database(tmp_path / "allocation.db")
    repo = AllocationRepository(db)
    engine = AllocationEngine()
    risk = _risk_state()

    kalshi_signal = Signal(
        strategy=StrategyName.MONOTONICITY,
        ticker="KXHIGHINFLATION",
        action="buy_yes",
        quantity=10,
        edge=SignalEdge.from_values(edge_before_fees="0.08", edge_after_fees="0.05", fee_estimate="0.01"),
        source="macro_dislocation",
        confidence=0.8,
        priority=1,
        legs=[SignalLegIntent(instrument=InstrumentRef(symbol="KXHIGHINFLATION", venue="kalshi", contract_side="yes"), side="buy", order_type="limit", limit_price=Decimal("0.45"), time_in_force="day")],
        ts=datetime(2026, 3, 22, 12, 0, tzinfo=timezone.utc),
    )
    trend_target = TrendTarget(symbol="SPY", momentum_return=Decimal("0.03"), realized_volatility=Decimal("0.15"), target_weight=Decimal("0.20"))
    selection = StructureSelectionRecord(
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
    summary = StructureScenarioSummary(
        underlying="SPY",
        observed_at=selection.observed_at,
        source="options_test",
        structure_name="call_calendar_500",
        horizon_days=7,
        path_count=200,
        seed=1,
        expected_pnl=Decimal("25"),
        probability_of_profit=Decimal("0.62"),
        cvar_95_loss=Decimal("-60"),
        expected_max_drawdown=Decimal("40"),
        stop_out_probability=Decimal("0.15"),
        predicted_daily_vol=Decimal("12"),
        expected_entry_cost=Decimal("200"),
        stress_expected_pnl=Decimal("15"),
        model_uncertainty_penalty=Decimal("10"),
    )
    candidates = [
        candidate_from_kalshi_signal(kalshi_signal, cvar_95_loss=Decimal("75"), uncertainty_penalty=Decimal("0.20"), liquidity_penalty=Decimal("0.05"), horizon_days=14, correlation_bucket="macro"),
        candidate_from_trend_target(trend_target, observed_at=datetime(2026, 3, 22, 12, 0, tzinfo=timezone.utc), portfolio_equity=Decimal("100000")),
        candidate_from_options_selection(selection, summary),
    ]
    run, decisions = engine.run(candidates=candidates, risk_state=risk)
    repo.persist_run(run)
    repo.persist_decisions(decisions)
    loaded_run = repo.load_run(run.run_id)
    loaded_decisions = repo.load_decisions(run.run_id)
    assert loaded_run is not None
    assert loaded_run.candidate_count == 3
    assert len(loaded_decisions) == 3
    assert {decision.source_kind for decision in loaded_decisions} == {"kalshi_signal", "trend_target", "options_selection"}


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


def _candidate(
    candidate_id: str,
    *,
    sleeve: str = "opportunistic",
    expected: str = "30",
    cvar: str = "100",
    uncertainty: str = "0.20",
    bucket: str = "bucket_a",
) -> TradeCandidate:
    return TradeCandidate(
        candidate_id=candidate_id,
        strategy_family="test",
        source_kind="test",
        instrument_key=candidate_id,
        sleeve=sleeve,
        regime_name="balanced",
        observed_at=datetime(2026, 3, 22, 12, 0, tzinfo=timezone.utc),
        expected_pnl_after_costs=Decimal(expected),
        cvar_95_loss=Decimal(cvar),
        uncertainty_penalty=Decimal(uncertainty),
        liquidity_penalty=Decimal("0.05"),
        horizon_days=10,
        regime_stability=Decimal("0.80"),
        notional_reference=Decimal("1000"),
        max_loss_estimate=Decimal(cvar),
        correlation_bucket=bucket,
        state_ok=True,
    )
