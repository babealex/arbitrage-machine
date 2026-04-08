from __future__ import annotations

from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path

from app.db import Database
from app.options.candidates import generate_candidates
from app.options.outcome_engine import OptionOutcomeEngine
from app.options.optimizer import evaluate_candidates
from app.options.models import OptionChainSnapshot, option_quote
from app.options.reporting import build_decision_report
from app.options.selection import select_and_record_best_structure
from app.options.state_encoder import build_state_vector
from app.options.scenario_engine import ScenarioAssumptions
from app.persistence.options_repo import OptionsRepository


def test_outcome_engine_tracks_realized_metrics_and_round_trips(tmp_path: Path) -> None:
    db = Database(tmp_path / "options_outcomes.db")
    repo = OptionsRepository(db)
    engine = OptionOutcomeEngine()
    entry_snapshot = _entry_snapshot()
    future_snapshot = _future_snapshot()
    _expiry, _chain, state_vector = build_state_vector(entry_snapshot)
    candidates = generate_candidates(snapshot=entry_snapshot, state_vector=state_vector)
    assumptions = ScenarioAssumptions(
        horizon_days=3,
        path_count=150,
        seed=1201,
        annual_drift=Decimal("0"),
        annual_volatility=Decimal("0.10"),
        event_day=1,
        event_crush=Decimal("0.06"),
    )
    scores = evaluate_candidates(
        snapshot=entry_snapshot,
        state_vector=state_vector,
        candidates=candidates,
        assumptions=assumptions,
        tail_penalty=Decimal("0.50"),
        uncertainty_penalty=Decimal("1.00"),
    )
    best, selection = select_and_record_best_structure(
        snapshot=entry_snapshot,
        state_vector=state_vector,
        candidates=candidates,
        assumptions=assumptions,
        tail_penalty=Decimal("0.50"),
        uncertainty_penalty=Decimal("1.00"),
    )

    assert best is not None
    assert selection is not None
    repo.persist_candidate_scores(
        underlying=entry_snapshot.underlying,
        observed_at=entry_snapshot.observed_at,
        source=entry_snapshot.source,
        regime_name=selection.regime_name,
        scores=scores,
    )
    repo.persist_structure_selection(selection)
    outcome = engine.evaluate_selection(
        selection=selection,
        entry_snapshot=entry_snapshot,
        future_snapshot=future_snapshot,
    )
    repo.persist_selection_outcome(outcome)
    loaded_outcomes = repo.load_selection_outcomes(underlying="SPY", decision_observed_at=entry_snapshot.observed_at)
    loaded_selection = repo.load_structure_selection(underlying="SPY", observed_at=entry_snapshot.observed_at)

    assert len(loaded_outcomes) == 1
    assert loaded_outcomes[0].evaluation_horizon == "1d"
    assert loaded_outcomes[0].realized_pnl is not None
    assert loaded_outcomes[0].predicted_pnl_error is not None
    assert loaded_selection is not None
    assert loaded_selection.realized_pnl == loaded_outcomes[0].realized_pnl
    report = build_decision_report(selection=selection, candidate_scores=scores, latest_outcome=loaded_outcomes[0])
    assert report.realized_pnl == loaded_outcomes[0].realized_pnl
    assert report.latest_outcome_horizon == "1d"


def test_outcome_engine_evaluates_due_horizons() -> None:
    engine = OptionOutcomeEngine()
    entry_snapshot = _entry_snapshot()
    _expiry, _chain, state_vector = build_state_vector(entry_snapshot)
    candidates = generate_candidates(snapshot=entry_snapshot, state_vector=state_vector)
    best, selection = select_and_record_best_structure(
        snapshot=entry_snapshot,
        state_vector=state_vector,
        candidates=candidates,
        assumptions=ScenarioAssumptions(
            horizon_days=3,
            path_count=100,
            seed=44,
            annual_drift=Decimal("0"),
            annual_volatility=Decimal("0.10"),
            event_day=1,
            event_crush=Decimal("0.06"),
        ),
        tail_penalty=Decimal("0.50"),
        uncertainty_penalty=Decimal("1.00"),
    )
    assert best is not None
    assert selection is not None
    outcomes = engine.evaluate_due_horizons(
        selection=selection,
        entry_snapshot=entry_snapshot,
        future_snapshots=[_future_snapshot(), _future_snapshot_3d(), _future_snapshot_expiry()],
    )

    assert [outcome.evaluation_horizon for outcome in outcomes] == ["1d", "3d", "expiry"]


def _entry_snapshot() -> OptionChainSnapshot:
    observed_at = datetime(2026, 3, 20, 15, 30, tzinfo=timezone.utc)
    return OptionChainSnapshot(
        underlying="SPY",
        observed_at=observed_at,
        source="event_chain",
        spot_price=Decimal("500"),
        forward_price=Decimal("501.25"),
        risk_free_rate=Decimal("0.045"),
        dividend_yield=Decimal("0.012"),
        realized_vol_20d=Decimal("0.18"),
        quotes=[
            option_quote(underlying="SPY", expiration_date=date(2026, 3, 27), strike="475", option_type="put", observed_at=observed_at, source="event_chain", bid="12.4", ask="12.8", implied_volatility="0.42", open_interest=500),
            option_quote(underlying="SPY", expiration_date=date(2026, 3, 27), strike="500", option_type="put", observed_at=observed_at, source="event_chain", bid="18.2", ask="18.6", implied_volatility="0.40", open_interest=1000),
            option_quote(underlying="SPY", expiration_date=date(2026, 3, 27), strike="500", option_type="call", observed_at=observed_at, source="event_chain", bid="18.4", ask="18.8", implied_volatility="0.38", open_interest=1000),
            option_quote(underlying="SPY", expiration_date=date(2026, 3, 27), strike="525", option_type="call", observed_at=observed_at, source="event_chain", bid="11.4", ask="11.8", implied_volatility="0.36", open_interest=600),
            option_quote(underlying="SPY", expiration_date=date(2026, 4, 17), strike="475", option_type="put", observed_at=observed_at, source="event_chain", bid="11.8", ask="12.3", implied_volatility="0.20", open_interest=700),
            option_quote(underlying="SPY", expiration_date=date(2026, 4, 17), strike="500", option_type="put", observed_at=observed_at, source="event_chain", bid="16.4", ask="16.9", implied_volatility="0.18", open_interest=900),
            option_quote(underlying="SPY", expiration_date=date(2026, 4, 17), strike="500", option_type="call", observed_at=observed_at, source="event_chain", bid="16.6", ask="17.1", implied_volatility="0.18", open_interest=900),
            option_quote(underlying="SPY", expiration_date=date(2026, 4, 17), strike="525", option_type="call", observed_at=observed_at, source="event_chain", bid="11.2", ask="11.7", implied_volatility="0.19", open_interest=500),
        ],
    )


def _future_snapshot() -> OptionChainSnapshot:
    observed_at = datetime(2026, 3, 21, 15, 30, tzinfo=timezone.utc)
    return OptionChainSnapshot(
        underlying="SPY",
        observed_at=observed_at,
        source="event_chain",
        spot_price=Decimal("503"),
        forward_price=Decimal("503.25"),
        risk_free_rate=Decimal("0.045"),
        dividend_yield=Decimal("0.012"),
        realized_vol_20d=Decimal("0.21"),
        quotes=[
            option_quote(underlying="SPY", expiration_date=date(2026, 3, 27), strike="475", option_type="put", observed_at=observed_at, source="event_chain", bid="11.0", ask="11.4", implied_volatility="0.36", open_interest=500),
            option_quote(underlying="SPY", expiration_date=date(2026, 3, 27), strike="500", option_type="put", observed_at=observed_at, source="event_chain", bid="15.3", ask="15.7", implied_volatility="0.34", open_interest=1000),
            option_quote(underlying="SPY", expiration_date=date(2026, 3, 27), strike="500", option_type="call", observed_at=observed_at, source="event_chain", bid="17.0", ask="17.4", implied_volatility="0.32", open_interest=1000),
            option_quote(underlying="SPY", expiration_date=date(2026, 3, 27), strike="525", option_type="call", observed_at=observed_at, source="event_chain", bid="10.2", ask="10.6", implied_volatility="0.30", open_interest=600),
            option_quote(underlying="SPY", expiration_date=date(2026, 4, 17), strike="475", option_type="put", observed_at=observed_at, source="event_chain", bid="11.5", ask="12.0", implied_volatility="0.22", open_interest=700),
            option_quote(underlying="SPY", expiration_date=date(2026, 4, 17), strike="500", option_type="put", observed_at=observed_at, source="event_chain", bid="16.7", ask="17.2", implied_volatility="0.20", open_interest=900),
            option_quote(underlying="SPY", expiration_date=date(2026, 4, 17), strike="500", option_type="call", observed_at=observed_at, source="event_chain", bid="17.4", ask="17.9", implied_volatility="0.20", open_interest=900),
            option_quote(underlying="SPY", expiration_date=date(2026, 4, 17), strike="525", option_type="call", observed_at=observed_at, source="event_chain", bid="12.0", ask="12.5", implied_volatility="0.21", open_interest=500),
        ],
    )


def _future_snapshot_3d() -> OptionChainSnapshot:
    observed_at = datetime(2026, 3, 23, 15, 30, tzinfo=timezone.utc)
    return OptionChainSnapshot(
        underlying="SPY",
        observed_at=observed_at,
        source="event_chain",
        spot_price=Decimal("501"),
        forward_price=Decimal("501.10"),
        risk_free_rate=Decimal("0.045"),
        dividend_yield=Decimal("0.012"),
        realized_vol_20d=Decimal("0.20"),
        quotes=[
            option_quote(underlying="SPY", expiration_date=date(2026, 3, 27), strike="475", option_type="put", observed_at=observed_at, source="event_chain", bid="10.2", ask="10.6", implied_volatility="0.30", open_interest=500),
            option_quote(underlying="SPY", expiration_date=date(2026, 3, 27), strike="500", option_type="put", observed_at=observed_at, source="event_chain", bid="13.5", ask="13.9", implied_volatility="0.28", open_interest=1000),
            option_quote(underlying="SPY", expiration_date=date(2026, 3, 27), strike="500", option_type="call", observed_at=observed_at, source="event_chain", bid="14.3", ask="14.7", implied_volatility="0.26", open_interest=1000),
            option_quote(underlying="SPY", expiration_date=date(2026, 3, 27), strike="525", option_type="call", observed_at=observed_at, source="event_chain", bid="8.1", ask="8.5", implied_volatility="0.24", open_interest=600),
            option_quote(underlying="SPY", expiration_date=date(2026, 4, 17), strike="475", option_type="put", observed_at=observed_at, source="event_chain", bid="11.4", ask="11.9", implied_volatility="0.21", open_interest=700),
            option_quote(underlying="SPY", expiration_date=date(2026, 4, 17), strike="500", option_type="put", observed_at=observed_at, source="event_chain", bid="16.3", ask="16.8", implied_volatility="0.19", open_interest=900),
            option_quote(underlying="SPY", expiration_date=date(2026, 4, 17), strike="500", option_type="call", observed_at=observed_at, source="event_chain", bid="16.9", ask="17.4", implied_volatility="0.19", open_interest=900),
            option_quote(underlying="SPY", expiration_date=date(2026, 4, 17), strike="525", option_type="call", observed_at=observed_at, source="event_chain", bid="11.8", ask="12.3", implied_volatility="0.20", open_interest=500),
        ],
    )


def _future_snapshot_expiry() -> OptionChainSnapshot:
    observed_at = datetime(2026, 4, 17, 15, 30, tzinfo=timezone.utc)
    return OptionChainSnapshot(
        underlying="SPY",
        observed_at=observed_at,
        source="event_chain",
        spot_price=Decimal("507"),
        forward_price=Decimal("507"),
        risk_free_rate=Decimal("0.045"),
        dividend_yield=Decimal("0.012"),
        realized_vol_20d=Decimal("0.19"),
        quotes=[
            option_quote(underlying="SPY", expiration_date=date(2026, 3, 27), strike="475", option_type="put", observed_at=observed_at, source="event_chain", bid="0.01", ask="0.02", implied_volatility="0.01", open_interest=500),
            option_quote(underlying="SPY", expiration_date=date(2026, 3, 27), strike="500", option_type="put", observed_at=observed_at, source="event_chain", bid="0.01", ask="0.02", implied_volatility="0.01", open_interest=1000),
            option_quote(underlying="SPY", expiration_date=date(2026, 3, 27), strike="500", option_type="call", observed_at=observed_at, source="event_chain", bid="7.00", ask="7.10", implied_volatility="0.01", open_interest=1000),
            option_quote(underlying="SPY", expiration_date=date(2026, 3, 27), strike="525", option_type="call", observed_at=observed_at, source="event_chain", bid="0.01", ask="0.02", implied_volatility="0.01", open_interest=600),
            option_quote(underlying="SPY", expiration_date=date(2026, 4, 17), strike="475", option_type="put", observed_at=observed_at, source="event_chain", bid="0.01", ask="0.02", implied_volatility="0.01", open_interest=700),
            option_quote(underlying="SPY", expiration_date=date(2026, 4, 17), strike="500", option_type="put", observed_at=observed_at, source="event_chain", bid="0.01", ask="0.02", implied_volatility="0.01", open_interest=900),
            option_quote(underlying="SPY", expiration_date=date(2026, 4, 17), strike="500", option_type="call", observed_at=observed_at, source="event_chain", bid="7.00", ask="7.10", implied_volatility="0.01", open_interest=900),
            option_quote(underlying="SPY", expiration_date=date(2026, 4, 17), strike="525", option_type="call", observed_at=observed_at, source="event_chain", bid="0.01", ask="0.02", implied_volatility="0.01", open_interest=500),
        ],
    )
