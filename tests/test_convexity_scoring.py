from __future__ import annotations

from datetime import date
from decimal import Decimal

from app.strategies.convexity.models import ConvexTradeCandidate, ConvexityOpportunity, OptionContractQuote
from app.strategies.convexity.scorer import convex_score, liquidity_score, scenario_expected_value


def _candidate() -> ConvexTradeCandidate:
    return ConvexTradeCandidate(
        symbol="SPY",
        setup_type="earnings",
        structure_type="long_call",
        expiry=date(2026, 4, 17),
        legs=[{"symbol": "SPYC505", "expiry": date(2026, 4, 17), "strike": Decimal("505"), "option_type": "call", "quantity": 1, "premium": Decimal("1.2")}],
        debit=Decimal("120"),
        max_loss=Decimal("120"),
        max_profit_estimate=Decimal("900"),
        reward_to_risk=Decimal("7.5"),
        expected_value=Decimal("40"),
        expected_value_after_costs=Decimal("25"),
        convex_score=Decimal("0"),
        breakout_sensitivity=Decimal("0.8"),
        liquidity_score=Decimal("0.9"),
        confidence_score=Decimal("0.7"),
        metadata={"bid_ask_pct_of_debit": Decimal("0.05"), "all_in_cost": Decimal("8"), "direction": "bullish"},
    )


def test_ev_after_costs_and_convex_score_positive_for_good_candidate() -> None:
    candidate = _candidate()
    opportunity = ConvexityOpportunity(
        symbol="SPY",
        setup_type="earnings",
        event_time=None,
        spot_price=Decimal("500"),
        expected_move_pct=Decimal("0.10"),
        implied_move_pct=Decimal("0.03"),
        realized_vol_pct=Decimal("0.25"),
        implied_vol_pct=Decimal("0.20"),
        volume_spike_ratio=Decimal("2.0"),
        breakout_score=Decimal("0.08"),
        metadata={"direction": "bullish"},
    )

    expected_value, after_costs = scenario_expected_value(candidate=candidate, opportunity=opportunity, quotes=[])
    score = convex_score(candidate=candidate, opportunity=opportunity)

    assert expected_value > 0
    assert after_costs > 0
    assert score > 0


def test_liquidity_score_rejects_dead_contract() -> None:
    score = liquidity_score(
        [OptionContractQuote("SPYC500", date(2026, 4, 17), Decimal("500"), "call", Decimal("0"), Decimal("5"), Decimal("2.5"), None, None, None, None, None, 0, 0)],
        min_oi=100,
        min_volume=10,
    )

    assert score == 0
