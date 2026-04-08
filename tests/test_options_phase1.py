from __future__ import annotations

from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path

from app.db import Database
from app.options.models import OptionChainSnapshot, option_quote
from app.options.state_encoder import build_state_vector
from app.persistence.options_repo import OptionsRepository


def test_option_chain_and_surface_features_round_trip(tmp_path: Path) -> None:
    db = Database(tmp_path / "options_phase1.db")
    repo = OptionsRepository(db)
    observed_at = datetime(2026, 3, 20, 15, 30, tzinfo=timezone.utc)
    snapshot = OptionChainSnapshot(
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
    repo.persist_chain_snapshot(snapshot)
    expiry_features, chain_features, state_vector = build_state_vector(snapshot)
    repo.persist_expiry_surface_features(expiry_features)
    repo.persist_chain_surface_features(chain_features)
    repo.persist_state_vector(state_vector)

    loaded_snapshot = repo.load_chain_snapshot(underlying="SPY", observed_at=observed_at)
    loaded_expiry = repo.load_expiry_surface_features(underlying="SPY", observed_at=observed_at)
    loaded_chain = repo.load_chain_surface_features(underlying="SPY", observed_at=observed_at)
    loaded_state = repo.load_state_vector(underlying="SPY", observed_at=observed_at)

    assert len(loaded_snapshot.quotes) == 8
    assert loaded_snapshot.quotes[0].contract.underlying == "SPY"
    assert loaded_snapshot.forward_price == Decimal("501.25")
    assert loaded_snapshot.realized_vol_20d == Decimal("0.18")
    assert len(loaded_expiry) == 2
    assert loaded_expiry[0].atm_iv == Decimal("0.21")
    assert loaded_expiry[0].put_skew_95 == Decimal("0.07")
    assert loaded_expiry[0].call_skew_105 == Decimal("0.03")
    assert loaded_chain is not None
    assert loaded_chain.expiries_count == 2
    assert loaded_chain.atm_iv_near == Decimal("0.21")
    assert loaded_chain.atm_iv_next == Decimal("0.235")
    assert loaded_chain.event_premium_proxy is not None
    assert loaded_state is not None
    assert loaded_state.forward_price == Decimal("501.25")
    assert loaded_state.vrp_proxy_near == Decimal("0.0117")
    assert loaded_state.total_variance_term_monotonic is True


def test_surface_feature_builder_handles_missing_iv_gracefully() -> None:
    observed_at = datetime(2026, 3, 20, 15, 30, tzinfo=timezone.utc)
    snapshot = OptionChainSnapshot(
        underlying="QQQ",
        observed_at=observed_at,
        source="test_chain",
        spot_price=Decimal("430"),
        quotes=[
            option_quote(underlying="QQQ", expiration_date=date(2026, 3, 27), strike="430", option_type="call", observed_at=observed_at, source="test_chain", implied_volatility=None),
            option_quote(underlying="QQQ", expiration_date=date(2026, 3, 27), strike="430", option_type="put", observed_at=observed_at, source="test_chain", implied_volatility="0.19"),
        ],
    )
    expiry_features, chain_features, state_vector = build_state_vector(snapshot)

    assert len(expiry_features) == 1
    assert expiry_features[0].atm_iv == Decimal("0.19")
    assert expiry_features[0].call_skew_105 is None
    assert chain_features.atm_iv_next is None
    assert state_vector.total_variance_term_monotonic is True


def test_state_vector_flags_non_monotonic_total_variance() -> None:
    observed_at = datetime(2026, 3, 20, 15, 30, tzinfo=timezone.utc)
    snapshot = OptionChainSnapshot(
        underlying="IWM",
        observed_at=observed_at,
        source="test_chain",
        spot_price=Decimal("210"),
        realized_vol_20d=Decimal("0.21"),
        quotes=[
            option_quote(underlying="IWM", expiration_date=date(2026, 3, 27), strike="210", option_type="put", observed_at=observed_at, source="test_chain", implied_volatility="0.28"),
            option_quote(underlying="IWM", expiration_date=date(2026, 3, 27), strike="210", option_type="call", observed_at=observed_at, source="test_chain", implied_volatility="0.28"),
            option_quote(underlying="IWM", expiration_date=date(2026, 4, 17), strike="210", option_type="put", observed_at=observed_at, source="test_chain", implied_volatility="0.12"),
            option_quote(underlying="IWM", expiration_date=date(2026, 4, 17), strike="210", option_type="call", observed_at=observed_at, source="test_chain", implied_volatility="0.12"),
        ],
    )
    expiry_features, _chain_features, state_vector = build_state_vector(snapshot)

    assert len(expiry_features) == 2
    assert state_vector.total_variance_term_monotonic is False
