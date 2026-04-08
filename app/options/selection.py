from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from decimal import Decimal

from app.options.optimizer import StructureScore, evaluate_candidates
from app.options.models import OptionChainSnapshot, OptionStateVector
from app.options.regimes import classify_regime
from app.options.scenario_engine import ScenarioAssumptions
from app.options.structures import OptionStructure


@dataclass(frozen=True, slots=True)
class StructureSelectionRecord:
    underlying: str
    observed_at: datetime
    source: str
    regime_name: str
    due_at_1d: datetime
    due_at_3d: datetime
    due_at_expiry: datetime
    candidate_count: int
    selected_structure_name: str
    objective_value: Decimal
    horizon_days: int
    path_count: int
    seed: int
    annual_drift: Decimal
    annual_volatility: Decimal | None
    event_day: int | None
    event_jump_mean: Decimal
    event_jump_stddev: Decimal
    iv_mean_reversion: Decimal
    iv_regime_level: Decimal | None
    iv_volatility: Decimal
    event_crush: Decimal
    exit_spread_fraction: Decimal
    slippage_bps: Decimal
    stop_out_loss: Decimal | None
    tail_penalty: Decimal
    uncertainty_penalty: Decimal
    realized_return: Decimal | None = None
    realized_vol: Decimal | None = None
    realized_iv_change: Decimal | None = None
    realized_pnl: Decimal | None = None
    max_drawdown_realized: Decimal | None = None
    prediction_error: Decimal | None = None
    move_error: Decimal | None = None
    vol_error: Decimal | None = None
    iv_error: Decimal | None = None


def select_and_record_best_structure(
    *,
    snapshot: OptionChainSnapshot,
    state_vector: OptionStateVector,
    candidates: list[OptionStructure],
    assumptions: ScenarioAssumptions,
    tail_penalty: Decimal,
    uncertainty_penalty: Decimal,
) -> tuple[StructureScore | None, StructureSelectionRecord | None]:
    scores = evaluate_candidates(
        snapshot=snapshot,
        state_vector=state_vector,
        candidates=candidates,
        assumptions=assumptions,
        tail_penalty=tail_penalty,
        uncertainty_penalty=uncertainty_penalty,
    )
    best = scores[0] if scores else None
    if best is None:
        return None, None
    regime = classify_regime(state_vector)
    selected_structure = next(candidate for candidate in candidates if candidate.name == best.structure_name)
    selected_expiry = max(leg.contract.expiration_date for leg in selected_structure.legs)
    return best, StructureSelectionRecord(
        underlying=snapshot.underlying,
        observed_at=snapshot.observed_at,
        source=snapshot.source,
        regime_name=regime.name,
        due_at_1d=snapshot.observed_at + timedelta(days=1),
        due_at_3d=snapshot.observed_at + timedelta(days=3),
        due_at_expiry=datetime.combine(selected_expiry, snapshot.observed_at.timetz()),
        candidate_count=len(scores),
        selected_structure_name=best.structure_name,
        objective_value=best.objective_value,
        horizon_days=assumptions.horizon_days,
        path_count=assumptions.path_count,
        seed=assumptions.seed,
        annual_drift=assumptions.annual_drift,
        annual_volatility=assumptions.annual_volatility,
        event_day=assumptions.event_day,
        event_jump_mean=assumptions.event_jump_mean,
        event_jump_stddev=assumptions.event_jump_stddev,
        iv_mean_reversion=assumptions.iv_mean_reversion,
        iv_regime_level=assumptions.iv_regime_level,
        iv_volatility=assumptions.iv_volatility,
        event_crush=assumptions.event_crush,
        exit_spread_fraction=assumptions.exit_spread_fraction,
        slippage_bps=assumptions.slippage_bps,
        stop_out_loss=assumptions.stop_out_loss,
        tail_penalty=tail_penalty,
        uncertainty_penalty=uncertainty_penalty,
    )
