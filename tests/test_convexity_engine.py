from __future__ import annotations

import pytest


pytestmark = pytest.mark.skip(
    reason="deprecated convexity_engine interface retired; coverage lives in package-based convexity tests"
)


def _snapshot() -> OptionChainSnapshot:
    observed_at = datetime(2026, 3, 22, 12, 0, tzinfo=timezone.utc)
    expiry = date(2026, 3, 28)
    return OptionChainSnapshot(
        underlying="SPY",
        observed_at=observed_at,
        source="test",
        spot_price=Decimal("500"),
        forward_price=Decimal("500"),
        realized_vol_20d=Decimal("0.28"),
        quotes=[
            option_quote(underlying="SPY", expiration_date=expiry, strike=Decimal("500"), option_type="call", observed_at=observed_at, source="test", bid="4.8", ask="5.2", open_interest=500),
            option_quote(underlying="SPY", expiration_date=expiry, strike=Decimal("500"), option_type="put", observed_at=observed_at, source="test", bid="4.6", ask="5.0", open_interest=500),
            option_quote(underlying="SPY", expiration_date=expiry, strike=Decimal("505"), option_type="call", observed_at=observed_at, source="test", bid="1.0", ask="1.2", open_interest=500),
            option_quote(underlying="SPY", expiration_date=expiry, strike=Decimal("495"), option_type="put", observed_at=observed_at, source="test", bid="1.0", ask="1.2", open_interest=500),
        ],
    )


def _state() -> OptionStateVector:
    observed_at = datetime(2026, 3, 22, 12, 0, tzinfo=timezone.utc)
    return OptionStateVector(
        underlying="SPY",
        observed_at=observed_at,
        source="test",
        spot_price=Decimal("500"),
        forward_price=Decimal("500"),
        realized_vol_20d=Decimal("0.28"),
        atm_iv_near=Decimal("0.20"),
        atm_iv_next=Decimal("0.22"),
        atm_iv_term_slope=Decimal("0.02"),
        event_premium_proxy=Decimal("0.04"),
        vrp_proxy_near=Decimal("0.08"),
        total_variance_term_monotonic=True,
    )


def test_scanner_finds_event_vol_and_momentum_opportunities() -> None:
    opportunities = scan_convexity_opportunities(
        snapshot=_snapshot(),
        state_vector=_state(),
        catalysts=[
            OpportunityCatalyst(
                setup_type="earnings",
                event_name="earnings",
                days_until_event=5,
                direction="bullish",
                event_move_estimate=Decimal("0.08"),
                implied_move=Decimal("0.03"),
                abnormal_volume_ratio=Decimal("2.0"),
                news_catalyst=True,
            )
        ],
        momentum_signals=[
            MomentumIgnitionSignal(
                direction="bullish",
                breakout_strength=Decimal("0.03"),
                abnormal_volume_ratio=Decimal("2.0"),
                news_catalyst=True,
            )
        ],
    )

    setup_types = {item.setup_type for item in opportunities}
    assert "earnings" in setup_types
    assert "volatility_mispricing" in setup_types
    assert "momentum" in setup_types


def test_builder_rejects_low_payoff_and_accepts_convex_trade() -> None:
    opportunity = scan_convexity_opportunities(
        snapshot=_snapshot(),
        state_vector=_state(),
        catalysts=[
            OpportunityCatalyst(
                setup_type="earnings",
                event_name="earnings",
                days_until_event=5,
                direction="bullish",
                event_move_estimate=Decimal("0.12"),
                implied_move=Decimal("0.03"),
                abnormal_volume_ratio=Decimal("2.0"),
                news_catalyst=True,
            )
        ],
    )[0]

    trade = build_convex_trade(opportunity=opportunity, snapshot=_snapshot(), capital=Decimal("500"))

    assert trade is not None
    assert trade.reward_risk_ratio >= Decimal("5")
    assert trade.capital_at_risk_fraction <= Decimal("0.20")


def test_outcome_tracking_and_learning_state_update() -> None:
    opportunity = scan_convexity_opportunities(
        snapshot=_snapshot(),
        state_vector=_state(),
        catalysts=[
            OpportunityCatalyst(
                setup_type="macro",
                event_name="cpi",
                days_until_event=4,
                direction="neutral",
                event_move_estimate=Decimal("0.10"),
                implied_move=Decimal("0.03"),
                abnormal_volume_ratio=Decimal("2.0"),
            )
        ],
    )[0]
    trade = build_convex_trade(
        opportunity=opportunity,
        snapshot=_snapshot(),
        capital=Decimal("500"),
        config=ConvexityEngineConfig(),
    )
    assert trade is not None
    outcome = record_convexity_outcome(
        trade=trade,
        actual_outcome=Decimal("35"),
        realized_return=Decimal("0.07"),
        realized_vol=Decimal("0.30"),
        realized_iv_change=Decimal("-0.04"),
        max_drawdown_realized=Decimal("8"),
    )
    state = update_learning_state([outcome])

    assert outcome.multiple_return > 0
    assert state.setup_selection_weights[outcome.setup_type] >= Decimal("1")
