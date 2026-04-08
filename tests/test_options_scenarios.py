from __future__ import annotations

from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path

from app.db import Database
from app.options.models import OptionChainSnapshot, option_quote
from app.options.optimizer import select_best_structure
from app.options.scenario_engine import ScenarioAssumptions, summarize_structure
from app.options.state_encoder import build_state_vector
from app.options.structures import call_calendar, call_vertical_spread, long_straddle
from app.persistence.options_repo import OptionsRepository


def test_structure_scenario_summary_is_deterministic_and_persistable(tmp_path: Path) -> None:
    db = Database(tmp_path / "options_phase3.db")
    repo = OptionsRepository(db)
    snapshot = _sample_snapshot()
    _expiry, _chain, state_vector = build_state_vector(snapshot)
    structure = call_vertical_spread(
        underlying="SPY",
        expiration_date=date(2026, 4, 17),
        long_strike=Decimal("500"),
        short_strike=Decimal("525"),
        quantity=1,
    )
    assumptions = ScenarioAssumptions(
        horizon_days=10,
        path_count=200,
        seed=7,
        annual_drift=Decimal("0.06"),
        annual_volatility=Decimal("0.18"),
        slippage_bps=Decimal("10"),
        stop_out_loss=Decimal("-250"),
    )

    summary_one = summarize_structure(
        snapshot=snapshot,
        state_vector=state_vector,
        structure=structure,
        assumptions=assumptions,
    )
    summary_two = summarize_structure(
        snapshot=snapshot,
        state_vector=state_vector,
        structure=structure,
        assumptions=assumptions,
    )
    repo.persist_structure_scenario_summary(summary_one)
    loaded = repo.load_structure_scenario_summary(
        underlying="SPY",
        observed_at=snapshot.observed_at,
        structure_name=structure.name,
    )

    assert summary_one == summary_two
    assert loaded == summary_one
    assert loaded is not None
    assert loaded.model_uncertainty_penalty is not None
    assert loaded.expected_entry_cost > Decimal("0")
    assert loaded.observed_at == snapshot.observed_at


def test_bullish_vertical_scores_better_with_positive_drift_than_negative_drift() -> None:
    snapshot = _sample_snapshot()
    _expiry, _chain, state_vector = build_state_vector(snapshot)
    structure = call_vertical_spread(
        underlying="SPY",
        expiration_date=date(2026, 4, 17),
        long_strike=Decimal("500"),
        short_strike=Decimal("525"),
        quantity=1,
    )
    bullish = summarize_structure(
        snapshot=snapshot,
        state_vector=state_vector,
        structure=structure,
        assumptions=ScenarioAssumptions(
            horizon_days=10,
            path_count=300,
            seed=21,
            annual_drift=Decimal("0.12"),
            annual_volatility=Decimal("0.16"),
        ),
    )
    bearish = summarize_structure(
        snapshot=snapshot,
        state_vector=state_vector,
        structure=structure,
        assumptions=ScenarioAssumptions(
            horizon_days=10,
            path_count=300,
            seed=21,
            annual_drift=Decimal("-0.12"),
            annual_volatility=Decimal("0.16"),
        ),
    )

    assert bullish.expected_pnl > bearish.expected_pnl
    assert bullish.probability_of_profit >= bearish.probability_of_profit


def test_event_crush_hurts_long_straddle_relative_to_no_crush() -> None:
    snapshot = _sample_snapshot()
    _expiry, _chain, state_vector = build_state_vector(snapshot)
    structure = long_straddle(
        underlying="SPY",
        expiration_date=date(2026, 3, 27),
        strike=Decimal("500"),
        quantity=1,
    )
    no_crush = summarize_structure(
        snapshot=snapshot,
        state_vector=state_vector,
        structure=structure,
        assumptions=ScenarioAssumptions(
            horizon_days=3,
            path_count=250,
            seed=99,
            annual_drift=Decimal("0"),
            annual_volatility=Decimal("0.10"),
            event_day=1,
            event_crush=Decimal("0"),
        ),
    )
    with_crush = summarize_structure(
        snapshot=snapshot,
        state_vector=state_vector,
        structure=structure,
        assumptions=ScenarioAssumptions(
            horizon_days=3,
            path_count=250,
            seed=99,
            annual_drift=Decimal("0"),
            annual_volatility=Decimal("0.10"),
            event_day=1,
            event_crush=Decimal("0.05"),
        ),
    )

    assert with_crush.expected_pnl < no_crush.expected_pnl
    assert with_crush.cvar_95_loss <= no_crush.cvar_95_loss


def test_optimizer_prefers_calendar_over_straddle_when_event_crush_is_large() -> None:
    snapshot = _sample_snapshot()
    _expiry, _chain, state_vector = build_state_vector(snapshot)
    best = select_best_structure(
        snapshot=snapshot,
        state_vector=state_vector,
        candidates=[
            long_straddle(
                underlying="SPY",
                expiration_date=date(2026, 3, 27),
                strike=Decimal("500"),
                quantity=1,
            ),
            call_calendar(
                underlying="SPY",
                near_expiration_date=date(2026, 3, 27),
                far_expiration_date=date(2026, 4, 17),
                strike=Decimal("500"),
                quantity=1,
            ),
        ],
        assumptions=ScenarioAssumptions(
            horizon_days=3,
            path_count=250,
            seed=303,
            annual_drift=Decimal("0"),
            annual_volatility=Decimal("0.10"),
            event_day=1,
            event_crush=Decimal("0.06"),
        ),
        tail_penalty=Decimal("0.50"),
        uncertainty_penalty=Decimal("1.00"),
    )

    assert best is not None
    assert best.structure_name == "call_calendar_500"


def _sample_snapshot() -> OptionChainSnapshot:
    observed_at = datetime(2026, 3, 20, 15, 30, tzinfo=timezone.utc)
    return OptionChainSnapshot(
        underlying="SPY",
        observed_at=observed_at,
        source="test_chain",
        spot_price=Decimal("500"),
        forward_price=Decimal("501.25"),
        risk_free_rate=Decimal("0.045"),
        dividend_yield=Decimal("0.012"),
        realized_vol_20d=Decimal("0.18"),
        quotes=[
            option_quote(underlying="SPY", expiration_date=date(2026, 3, 27), strike="475", option_type="put", observed_at=observed_at, source="test_chain", bid="9.8", ask="10.2", implied_volatility="0.28"),
            option_quote(underlying="SPY", expiration_date=date(2026, 3, 27), strike="500", option_type="put", observed_at=observed_at, source="test_chain", bid="14.8", ask="15.2", implied_volatility="0.22"),
            option_quote(underlying="SPY", expiration_date=date(2026, 3, 27), strike="500", option_type="call", observed_at=observed_at, source="test_chain", bid="15.0", ask="15.4", implied_volatility="0.20"),
            option_quote(underlying="SPY", expiration_date=date(2026, 3, 27), strike="525", option_type="call", observed_at=observed_at, source="test_chain", bid="9.0", ask="9.4", implied_volatility="0.24"),
            option_quote(underlying="SPY", expiration_date=date(2026, 4, 17), strike="475", option_type="put", observed_at=observed_at, source="test_chain", bid="13.0", ask="13.5", implied_volatility="0.30"),
            option_quote(underlying="SPY", expiration_date=date(2026, 4, 17), strike="500", option_type="put", observed_at=observed_at, source="test_chain", bid="20.0", ask="20.5", implied_volatility="0.24"),
            option_quote(underlying="SPY", expiration_date=date(2026, 4, 17), strike="500", option_type="call", observed_at=observed_at, source="test_chain", bid="20.3", ask="20.8", implied_volatility="0.23"),
            option_quote(underlying="SPY", expiration_date=date(2026, 4, 17), strike="525", option_type="call", observed_at=observed_at, source="test_chain", bid="14.5", ask="15.0", implied_volatility="0.26"),
        ],
    )
