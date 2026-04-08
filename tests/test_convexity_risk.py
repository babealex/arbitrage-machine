from __future__ import annotations

from datetime import date
from decimal import Decimal

from app.strategies.convexity.models import ConvexTradeCandidate, ConvexityAccountState
from app.strategies.convexity.risk import passes_convexity_risk_limits, risk_budget_for_trade


def _candidate(max_loss: str = "10") -> ConvexTradeCandidate:
    return ConvexTradeCandidate(
        symbol="SPY",
        setup_type="momentum",
        structure_type="long_call",
        expiry=date(2026, 4, 17),
        legs=[],
        debit=Decimal(max_loss),
        max_loss=Decimal(max_loss),
        max_profit_estimate=Decimal("80"),
        reward_to_risk=Decimal("8"),
        expected_value=Decimal("5"),
        expected_value_after_costs=Decimal("3"),
        convex_score=Decimal("4"),
        breakout_sensitivity=Decimal("1"),
        liquidity_score=Decimal("1"),
        confidence_score=Decimal("1"),
        metadata={"correlated_event_bucket": "macro"},
    )


def test_aggregate_exposure_cap_enforced() -> None:
    okay, reason = passes_convexity_risk_limits(
        candidate=_candidate("15"),
        account_state=ConvexityAccountState(nav=Decimal("100"), available_cash=Decimal("100"), open_convexity_risk=Decimal("10")),
        max_total_exposure_pct=Decimal("0.20"),
        max_open_trades=5,
        max_symbol_exposure_pct=Decimal("0.10"),
        max_correlated_event_exposure_pct=Decimal("0.15"),
    )

    assert okay is False
    assert reason == "max_total_exposure"


def test_risk_budget_for_trade_is_small_and_capped() -> None:
    budget = risk_budget_for_trade(Decimal("500"), Decimal("0.02"))
    assert budget == Decimal("10")
