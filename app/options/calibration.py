from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from decimal import Decimal

from app.options.outcomes import OptionOutcomeEvaluation
from app.options.selection import StructureSelectionRecord


@dataclass(frozen=True, slots=True)
class CalibrationBucket:
    key: str
    observations: int
    avg_prediction_error: Decimal
    avg_move_error: Decimal
    avg_vol_error: Decimal
    avg_iv_error: Decimal
    avg_realized_pnl: Decimal
    avg_realized_return: Decimal


@dataclass(frozen=True, slots=True)
class OptionsCalibrationReport:
    total_observations: int
    by_regime: list[CalibrationBucket]
    by_structure: list[CalibrationBucket]
    by_horizon: list[CalibrationBucket]


def build_calibration_report(
    *,
    selections: list[StructureSelectionRecord],
    outcomes: list[OptionOutcomeEvaluation],
) -> OptionsCalibrationReport:
    regime_lookup = {(selection.underlying, selection.observed_at, selection.selected_structure_name): selection.regime_name for selection in selections}
    by_regime: dict[str, list[OptionOutcomeEvaluation]] = defaultdict(list)
    by_structure: dict[str, list[OptionOutcomeEvaluation]] = defaultdict(list)
    by_horizon: dict[str, list[OptionOutcomeEvaluation]] = defaultdict(list)

    for outcome in outcomes:
        key = (outcome.underlying, outcome.decision_observed_at, outcome.selected_structure_name)
        regime_name = regime_lookup.get(key, "unknown")
        by_regime[regime_name].append(outcome)
        by_structure[outcome.selected_structure_name].append(outcome)
        by_horizon[outcome.evaluation_horizon].append(outcome)

    return OptionsCalibrationReport(
        total_observations=len(outcomes),
        by_regime=_build_buckets(by_regime),
        by_structure=_build_buckets(by_structure),
        by_horizon=_build_buckets(by_horizon),
    )


def _build_buckets(grouped: dict[str, list[OptionOutcomeEvaluation]]) -> list[CalibrationBucket]:
    buckets: list[CalibrationBucket] = []
    for key, values in sorted(grouped.items()):
        buckets.append(
            CalibrationBucket(
                key=key,
                observations=len(values),
                avg_prediction_error=_avg(value.predicted_pnl_error for value in values),
                avg_move_error=_avg(value.move_error for value in values),
                avg_vol_error=_avg(value.vol_error for value in values),
                avg_iv_error=_avg(value.iv_error for value in values),
                avg_realized_pnl=_avg(value.realized_pnl for value in values),
                avg_realized_return=_avg(value.realized_return for value in values),
            )
        )
    return buckets


def _avg(values) -> Decimal:
    defined = [value for value in values if value is not None]
    if not defined:
        return Decimal("0")
    return sum(defined, Decimal("0")) / Decimal(len(defined))
