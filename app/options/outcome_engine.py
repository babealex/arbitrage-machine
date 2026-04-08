from __future__ import annotations

from datetime import timedelta
from decimal import Decimal

from app.options.candidates import generate_candidates
from app.options.models import OptionChainSnapshot
from app.options.outcomes import OptionOutcomeEvaluation
from app.options.scenario_engine import _entry_cost, _structure_mark_value
from app.options.selection import StructureSelectionRecord
from app.options.state_encoder import build_state_vector


class OptionOutcomeEngine:
    HORIZONS = ("1d", "3d", "expiry")

    def evaluate_selection(
        self,
        *,
        selection: StructureSelectionRecord,
        entry_snapshot: OptionChainSnapshot,
        future_snapshot: OptionChainSnapshot,
    ) -> OptionOutcomeEvaluation:
        _expiry, _chain, entry_state = build_state_vector(entry_snapshot)
        candidates = generate_candidates(snapshot=entry_snapshot, state_vector=entry_state)
        structure = next((candidate for candidate in candidates if candidate.name == selection.selected_structure_name), None)
        if structure is None:
            raise KeyError(f"unable to reconstruct structure {selection.selected_structure_name}")
        entry_quotes = {quote.contract.contract_key(): quote for quote in entry_snapshot.quotes}
        future_quotes = {quote.contract.contract_key(): quote for quote in future_snapshot.quotes}
        entry_cost = _entry_cost(structure=structure, quotes_by_key=entry_quotes)
        future_state = build_state_vector(future_snapshot)[2]
        realized_value = _realized_structure_value(
            structure=structure,
            future_snapshot=future_snapshot,
            future_quotes=future_quotes,
        )
        realized_pnl = realized_value - entry_cost
        realized_return = Decimal("0") if entry_cost == 0 else realized_pnl / entry_cost
        realized_vol = future_snapshot.realized_vol_20d
        realized_iv_change = None
        if entry_state.atm_iv_near is not None and future_state.atm_iv_near is not None:
            realized_iv_change = future_state.atm_iv_near - entry_state.atm_iv_near
        move_error = None
        if selection.annual_drift is not None:
            realized_move = (future_snapshot.spot_price / entry_snapshot.spot_price) - Decimal("1")
            expected_move = selection.annual_drift * (Decimal(_days_between(entry_snapshot, future_snapshot)) / Decimal("252"))
            move_error = realized_move - expected_move
        vol_error = None
        if selection.annual_volatility is not None and future_snapshot.realized_vol_20d is not None:
            vol_error = future_snapshot.realized_vol_20d - selection.annual_volatility
        predicted_error = realized_pnl - (Decimal("0") if selection.objective_value is None else selection.objective_value)
        return OptionOutcomeEvaluation(
            underlying=selection.underlying,
            decision_observed_at=selection.observed_at,
            evaluation_horizon=_horizon_name(selection=selection, future_snapshot=future_snapshot, structure=structure),
            evaluated_at=future_snapshot.observed_at,
            selected_structure_name=selection.selected_structure_name,
            expected_pnl=selection.objective_value,
            realized_return=realized_return,
            realized_vol=realized_vol,
            realized_iv_change=realized_iv_change,
            realized_pnl=realized_pnl,
            max_drawdown_realized=min(Decimal("0"), realized_pnl),
            predicted_pnl_error=predicted_error,
            move_error=move_error,
            vol_error=vol_error,
            iv_error=realized_iv_change,
        )

    def evaluate_due_horizons(
        self,
        *,
        selection: StructureSelectionRecord,
        entry_snapshot: OptionChainSnapshot,
        future_snapshots: list[OptionChainSnapshot],
    ) -> list[OptionOutcomeEvaluation]:
        due = self.due_times(selection)
        future_by_day = {snapshot.observed_at.date(): snapshot for snapshot in future_snapshots}
        outcomes: list[OptionOutcomeEvaluation] = []
        for horizon, due_at in due.items():
            future_snapshot = future_by_day.get(due_at.date())
            if future_snapshot is None:
                continue
            outcome = self.evaluate_selection(
                selection=selection,
                entry_snapshot=entry_snapshot,
                future_snapshot=future_snapshot,
            )
            if outcome.evaluation_horizon != horizon and horizon != "expiry":
                outcome = OptionOutcomeEvaluation(
                    underlying=outcome.underlying,
                    decision_observed_at=outcome.decision_observed_at,
                    evaluation_horizon=horizon,
                    evaluated_at=outcome.evaluated_at,
                    selected_structure_name=outcome.selected_structure_name,
                    expected_pnl=outcome.expected_pnl,
                    realized_return=outcome.realized_return,
                    realized_vol=outcome.realized_vol,
                    realized_iv_change=outcome.realized_iv_change,
                    realized_pnl=outcome.realized_pnl,
                    max_drawdown_realized=outcome.max_drawdown_realized,
                    predicted_pnl_error=outcome.predicted_pnl_error,
                    move_error=outcome.move_error,
                    vol_error=outcome.vol_error,
                    iv_error=outcome.iv_error,
                )
            outcomes.append(outcome)
        return outcomes

    @staticmethod
    def due_times(selection: StructureSelectionRecord) -> dict[str, object]:
        return {
            "1d": selection.due_at_1d,
            "3d": selection.due_at_3d,
            "expiry": selection.due_at_expiry,
        }


def _realized_structure_value(
    *,
    structure,
    future_snapshot: OptionChainSnapshot,
    future_quotes: dict[str, object],
) -> Decimal:
    return _structure_mark_value(
        structure=structure,
        spot=future_snapshot.spot_price,
        quote_time=future_snapshot.observed_at,
        quotes_by_key=future_quotes,
        risk_free_rate=future_snapshot.risk_free_rate,
        dividend_yield=future_snapshot.dividend_yield,
        iv_shift=Decimal("0"),
        exit_spread_fraction=Decimal("0.50"),
        slippage_bps=Decimal("0"),
    )


def _horizon_name(*, selection: StructureSelectionRecord, future_snapshot: OptionChainSnapshot, structure) -> str:
    if future_snapshot.observed_at.date() >= max(leg.contract.expiration_date for leg in structure.legs):
        return "expiry"
    days = _days_between_from_dt(selection.observed_at, future_snapshot.observed_at)
    if days <= 1:
        return "1d"
    return "3d"


def _days_between(entry_snapshot: OptionChainSnapshot, future_snapshot: OptionChainSnapshot) -> int:
    return _days_between_from_dt(entry_snapshot.observed_at, future_snapshot.observed_at)


def _days_between_from_dt(start, end) -> int:
    return max(0, (end.date() - start.date()).days)
