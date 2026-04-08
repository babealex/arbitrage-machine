from __future__ import annotations

from datetime import date
from decimal import Decimal

from app.strategies.convexity.execution_adapter import select_convexity_orders
from app.strategies.convexity.models import ConvexTradeCandidate, ConvexityAccountState


def _candidate(symbol: str, score: str, max_loss: str, bucket: str) -> ConvexTradeCandidate:
    return ConvexTradeCandidate(
        symbol=symbol,
        setup_type="momentum",
        structure_type="long_call",
        expiry=date(2026, 4, 17),
        legs=[{"symbol": f"{symbol}C", "expiry": date(2026, 4, 17), "strike": Decimal("100"), "option_type": "call", "quantity": 1, "premium": Decimal("1.0")}],
        debit=Decimal(max_loss),
        max_loss=Decimal(max_loss),
        max_profit_estimate=Decimal("80"),
        reward_to_risk=Decimal("8"),
        expected_value=Decimal("5"),
        expected_value_after_costs=Decimal("3"),
        convex_score=Decimal(score),
        breakout_sensitivity=Decimal("1"),
        liquidity_score=Decimal("1"),
        confidence_score=Decimal("1"),
        metadata={"correlated_event_bucket": bucket, "candidate_id": f"{symbol}:{score}"},
    )


def test_no_trade_scenario_returns_empty_orders() -> None:
    orders = select_convexity_orders(
        [],
        ConvexityAccountState(nav=Decimal("500"), available_cash=Decimal("500")),
        max_risk_per_trade_pct=Decimal("0.02"),
        max_total_exposure_pct=Decimal("0.20"),
        max_open_trades=5,
        max_symbol_exposure_pct=Decimal("0.05"),
        max_correlated_event_exposure_pct=Decimal("0.10"),
    )
    assert orders == []


def test_execution_adapter_selects_top_candidates_with_limits() -> None:
    orders = select_convexity_orders(
        [
            _candidate("SPY", "9", "8", "macro"),
            _candidate("QQQ", "7", "8", "macro"),
        ],
        ConvexityAccountState(nav=Decimal("500"), available_cash=Decimal("500")),
        max_risk_per_trade_pct=Decimal("0.02"),
        max_total_exposure_pct=Decimal("0.20"),
        max_open_trades=5,
        max_symbol_exposure_pct=Decimal("05") / Decimal("100"),
        max_correlated_event_exposure_pct=Decimal("0.20"),
    )

    assert orders
    assert len({order.signal_ref for order in orders}) >= 1
