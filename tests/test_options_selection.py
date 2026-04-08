from __future__ import annotations

from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path

from app.db import Database
from app.options.candidates import CandidateFilterConfig, generate_candidates
from app.options.models import OptionChainSnapshot, option_quote
from app.options.regimes import classify_regime
from app.options.selection import select_and_record_best_structure
from app.options.state_encoder import build_state_vector
from app.options.scenario_engine import ScenarioAssumptions
from app.persistence.options_repo import OptionsRepository


def test_candidate_generation_stays_small_and_filters_illiquid_quotes() -> None:
    snapshot = _balanced_snapshot()
    _expiry, _chain, state_vector = build_state_vector(snapshot)

    candidates = generate_candidates(snapshot=snapshot, state_vector=state_vector)

    assert [candidate.name for candidate in candidates] == [
        "long_straddle_500",
        "call_vertical_500_525",
    ]

    illiquid_snapshot = OptionChainSnapshot(
        underlying=snapshot.underlying,
        observed_at=snapshot.observed_at,
        source=snapshot.source,
        spot_price=snapshot.spot_price,
        forward_price=snapshot.forward_price,
        risk_free_rate=snapshot.risk_free_rate,
        dividend_yield=snapshot.dividend_yield,
        realized_vol_20d=snapshot.realized_vol_20d,
        quotes=[
            option_quote(
                underlying=quote.contract.underlying,
                expiration_date=quote.contract.expiration_date,
                strike=quote.contract.strike,
                option_type=quote.contract.option_type,
                observed_at=quote.observed_at,
                source=quote.source,
                bid=quote.bid,
                ask=quote.ask,
                implied_volatility=quote.implied_volatility,
                open_interest=20 if quote.contract.strike == Decimal("500") else quote.open_interest,
            )
            for quote in snapshot.quotes
        ],
    )

    filtered = generate_candidates(
        snapshot=illiquid_snapshot,
        state_vector=state_vector,
        config=CandidateFilterConfig(min_open_interest=100),
    )

    assert filtered == []


def test_candidate_generation_shifts_with_regime() -> None:
    balanced_snapshot = _balanced_snapshot()
    _expiry, _chain, balanced_state = build_state_vector(balanced_snapshot)
    balanced_candidates = generate_candidates(snapshot=balanced_snapshot, state_vector=balanced_state)

    assert classify_regime(balanced_state).name == "balanced"
    assert [candidate.name for candidate in balanced_candidates] == [
        "long_straddle_500",
        "call_vertical_500_525",
    ]

    skew_snapshot = _skew_snapshot()
    _expiry, _chain, skew_state = build_state_vector(skew_snapshot)
    skew_candidates = generate_candidates(snapshot=skew_snapshot, state_vector=skew_state)

    assert classify_regime(skew_state).name == "downside_skew"
    assert [candidate.name for candidate in skew_candidates] == [
        "call_vertical_500_525",
        "put_vertical_500_475",
    ]


def test_selection_record_round_trip_and_stores_assumptions(tmp_path: Path) -> None:
    db = Database(tmp_path / "options_selection.db")
    repo = OptionsRepository(db)
    snapshot = _event_snapshot()
    _expiry, _chain, state_vector = build_state_vector(snapshot)
    candidates = generate_candidates(snapshot=snapshot, state_vector=state_vector)
    assumptions = ScenarioAssumptions(
        horizon_days=3,
        path_count=200,
        seed=515,
        annual_drift=Decimal("0"),
        annual_volatility=Decimal("0.10"),
        event_day=1,
        event_crush=Decimal("0.06"),
    )

    best, record = select_and_record_best_structure(
        snapshot=snapshot,
        state_vector=state_vector,
        candidates=candidates,
        assumptions=assumptions,
        tail_penalty=Decimal("0.50"),
        uncertainty_penalty=Decimal("1.00"),
    )

    assert best is not None
    assert record is not None
    repo.persist_structure_selection(record)
    loaded = repo.load_structure_selection(underlying="SPY", observed_at=snapshot.observed_at)

    assert loaded == record
    assert loaded.selected_structure_name == best.structure_name
    assert loaded.event_crush == Decimal("0.06")
    assert loaded.regime_name == "event_term_structure"
    assert loaded.tail_penalty == Decimal("0.50")


def _sample_snapshot() -> OptionChainSnapshot:
    return _event_snapshot()


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


def _balanced_snapshot() -> OptionChainSnapshot:
    observed_at = datetime(2026, 3, 20, 15, 30, tzinfo=timezone.utc)
    return OptionChainSnapshot(
        underlying="SPY",
        observed_at=observed_at,
        source="balanced_chain",
        spot_price=Decimal("500"),
        forward_price=Decimal("500"),
        risk_free_rate=Decimal("0.045"),
        dividend_yield=Decimal("0.012"),
        realized_vol_20d=Decimal("0.18"),
        quotes=[
            option_quote(underlying="SPY", expiration_date=date(2026, 3, 27), strike="475", option_type="put", observed_at=observed_at, source="balanced_chain", bid="8.5", ask="8.9", implied_volatility="0.19", open_interest=500),
            option_quote(underlying="SPY", expiration_date=date(2026, 3, 27), strike="500", option_type="put", observed_at=observed_at, source="balanced_chain", bid="12.0", ask="12.4", implied_volatility="0.18", open_interest=1000),
            option_quote(underlying="SPY", expiration_date=date(2026, 3, 27), strike="500", option_type="call", observed_at=observed_at, source="balanced_chain", bid="12.2", ask="12.6", implied_volatility="0.18", open_interest=1000),
            option_quote(underlying="SPY", expiration_date=date(2026, 3, 27), strike="525", option_type="call", observed_at=observed_at, source="balanced_chain", bid="7.2", ask="7.5", implied_volatility="0.18", open_interest=600),
            option_quote(underlying="SPY", expiration_date=date(2026, 4, 17), strike="500", option_type="call", observed_at=observed_at, source="balanced_chain", bid="16.0", ask="16.4", implied_volatility="0.18", open_interest=900),
        ],
    )


def _skew_snapshot() -> OptionChainSnapshot:
    observed_at = datetime(2026, 3, 20, 15, 30, tzinfo=timezone.utc)
    return OptionChainSnapshot(
        underlying="SPY",
        observed_at=observed_at,
        source="skew_chain",
        spot_price=Decimal("500"),
        forward_price=Decimal("500"),
        risk_free_rate=Decimal("0.045"),
        dividend_yield=Decimal("0.012"),
        realized_vol_20d=Decimal("0.18"),
        quotes=[
            option_quote(underlying="SPY", expiration_date=date(2026, 3, 27), strike="475", option_type="put", observed_at=observed_at, source="skew_chain", bid="10.8", ask="11.2", implied_volatility="0.27", open_interest=700),
            option_quote(underlying="SPY", expiration_date=date(2026, 3, 27), strike="500", option_type="put", observed_at=observed_at, source="skew_chain", bid="13.2", ask="13.6", implied_volatility="0.21", open_interest=1000),
            option_quote(underlying="SPY", expiration_date=date(2026, 3, 27), strike="500", option_type="call", observed_at=observed_at, source="skew_chain", bid="11.8", ask="12.2", implied_volatility="0.16", open_interest=1000),
            option_quote(underlying="SPY", expiration_date=date(2026, 3, 27), strike="525", option_type="call", observed_at=observed_at, source="skew_chain", bid="6.4", ask="6.7", implied_volatility="0.14", open_interest=600),
            option_quote(underlying="SPY", expiration_date=date(2026, 4, 17), strike="500", option_type="call", observed_at=observed_at, source="skew_chain", bid="15.5", ask="15.9", implied_volatility="0.17", open_interest=900),
        ],
    )
