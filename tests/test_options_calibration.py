from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

from app.db import Database
from app.options.calibration import build_calibration_report
from app.options.outcomes import OptionOutcomeEvaluation
from app.options.selection import StructureSelectionRecord
from app.persistence.options_repo import OptionsRepository


def test_calibration_report_aggregates_bias_by_regime_structure_and_horizon(tmp_path: Path) -> None:
    db = Database(tmp_path / "options_calibration.db")
    repo = OptionsRepository(db)
    observed_at = datetime(2026, 3, 20, 15, 30, tzinfo=timezone.utc)
    selections = [
        _selection(observed_at=observed_at, regime_name="event_term_structure", structure_name="call_calendar_500"),
        _selection(observed_at=observed_at + timedelta(days=7), regime_name="downside_skew", structure_name="put_vertical_500_475"),
    ]
    outcomes = [
        _outcome(observed_at=observed_at, structure_name="call_calendar_500", horizon="1d", pnl="25", prediction_error="5", iv_error="-0.03", move_error="0.00", vol_error="0.01"),
        _outcome(observed_at=observed_at, structure_name="call_calendar_500", horizon="3d", pnl="18", prediction_error="-2", iv_error="-0.02", move_error="0.00", vol_error="0.00"),
        _outcome(observed_at=observed_at + timedelta(days=7), structure_name="put_vertical_500_475", horizon="1d", pnl="-12", prediction_error="-6", iv_error="0.01", move_error="-0.02", vol_error="0.03"),
    ]

    for selection in selections:
        repo.persist_structure_selection(selection)
    for outcome in outcomes:
        repo.persist_selection_outcome(outcome)

    report = build_calibration_report(
        selections=repo.load_all_structure_selections(),
        outcomes=repo.load_all_selection_outcomes(),
    )

    assert report.total_observations == 3
    regime_map = {bucket.key: bucket for bucket in report.by_regime}
    assert regime_map["event_term_structure"].observations == 2
    assert regime_map["event_term_structure"].avg_iv_error == Decimal("-0.025")
    assert regime_map["downside_skew"].avg_prediction_error == Decimal("-6")
    structure_map = {bucket.key: bucket for bucket in report.by_structure}
    assert structure_map["call_calendar_500"].avg_realized_pnl == Decimal("21.5")
    horizon_map = {bucket.key: bucket for bucket in report.by_horizon}
    assert horizon_map["1d"].observations == 2
    assert horizon_map["1d"].avg_vol_error == Decimal("0.02")


def _selection(*, observed_at: datetime, regime_name: str, structure_name: str) -> StructureSelectionRecord:
    return StructureSelectionRecord(
        underlying="SPY",
        observed_at=observed_at,
        source="calibration_test",
        regime_name=regime_name,
        due_at_1d=observed_at + timedelta(days=1),
        due_at_3d=observed_at + timedelta(days=3),
        due_at_expiry=observed_at + timedelta(days=28),
        candidate_count=2,
        selected_structure_name=structure_name,
        objective_value=Decimal("20"),
        horizon_days=3,
        path_count=100,
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
        slippage_bps=Decimal("0"),
        stop_out_loss=None,
        tail_penalty=Decimal("0.50"),
        uncertainty_penalty=Decimal("1.00"),
    )


def _outcome(
    *,
    observed_at: datetime,
    structure_name: str,
    horizon: str,
    pnl: str,
    prediction_error: str,
    iv_error: str,
    move_error: str,
    vol_error: str,
) -> OptionOutcomeEvaluation:
    return OptionOutcomeEvaluation(
        underlying="SPY",
        decision_observed_at=observed_at,
        evaluation_horizon=horizon,
        evaluated_at=observed_at + timedelta(days=1 if horizon == "1d" else 3),
        selected_structure_name=structure_name,
        expected_pnl=Decimal("20"),
        realized_return=Decimal("0.05"),
        realized_vol=Decimal("0.20"),
        realized_iv_change=Decimal(iv_error),
        realized_pnl=Decimal(pnl),
        max_drawdown_realized=Decimal("-5"),
        predicted_pnl_error=Decimal(prediction_error),
        move_error=Decimal(move_error),
        vol_error=Decimal(vol_error),
        iv_error=Decimal(iv_error),
    )
