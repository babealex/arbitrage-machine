from __future__ import annotations

from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path

from app.db import Database
from app.options.candidates import generate_candidates
from app.options.optimizer import evaluate_candidates
from app.options.models import OptionChainSnapshot, option_quote
from app.options.regimes import classify_regime
from app.options.reporting import build_decision_report
from app.options.selection import select_and_record_best_structure
from app.options.state_encoder import build_state_vector
from app.options.scenario_engine import ScenarioAssumptions
from app.persistence.options_repo import OptionsRepository


def test_candidate_scores_round_trip_and_report_builds(tmp_path: Path) -> None:
    db = Database(tmp_path / "options_reporting.db")
    repo = OptionsRepository(db)
    snapshot = _event_snapshot()
    _expiry, _chain, state_vector = build_state_vector(snapshot)
    candidates = generate_candidates(snapshot=snapshot, state_vector=state_vector)
    assumptions = ScenarioAssumptions(
        horizon_days=3,
        path_count=150,
        seed=909,
        annual_drift=Decimal("0"),
        annual_volatility=Decimal("0.10"),
        event_day=1,
        event_crush=Decimal("0.06"),
    )
    scores = evaluate_candidates(
        snapshot=snapshot,
        state_vector=state_vector,
        candidates=candidates,
        assumptions=assumptions,
        tail_penalty=Decimal("0.50"),
        uncertainty_penalty=Decimal("1.00"),
    )
    best, selection = select_and_record_best_structure(
        snapshot=snapshot,
        state_vector=state_vector,
        candidates=candidates,
        assumptions=assumptions,
        tail_penalty=Decimal("0.50"),
        uncertainty_penalty=Decimal("1.00"),
    )

    assert best is not None
    assert selection is not None
    repo.persist_candidate_scores(
        underlying=snapshot.underlying,
        observed_at=snapshot.observed_at,
        source=snapshot.source,
        regime_name=classify_regime(state_vector).name,
        scores=scores,
    )
    repo.persist_structure_selection(selection)
    loaded_scores = repo.load_candidate_scores(underlying="SPY", observed_at=snapshot.observed_at)
    loaded_selection = repo.load_structure_selection(underlying="SPY", observed_at=snapshot.observed_at)

    assert len(loaded_scores) == len(scores)
    assert loaded_scores[0].structure_name == scores[0].structure_name
    assert loaded_scores[0].objective_value == scores[0].objective_value
    assert loaded_selection is not None
    report = build_decision_report(selection=loaded_selection, candidate_scores=loaded_scores)
    assert report.selected_structure_name == loaded_selection.selected_structure_name
    assert report.regime_name == "event_term_structure"
    assert report.candidate_count == loaded_selection.candidate_count
    assert report.selected_expected_pnl == loaded_scores[0].summary.expected_pnl


def _event_snapshot() -> OptionChainSnapshot:
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
