from __future__ import annotations

from datetime import date, datetime, timezone
from decimal import Decimal

from app.strategies.convexity.models import ConvexityAccountState, ConvexityOpportunity, OptionContractQuote
from app.strategies.convexity.structures import build_convex_trade_candidates


def _chain() -> list[OptionContractQuote]:
    expiry = date(2026, 4, 17)
    return [
        OptionContractQuote("SPYC500", expiry, Decimal("500"), "call", Decimal("4.8"), Decimal("5.2"), Decimal("5.0"), Decimal("0.20"), Decimal("0.32"), None, None, None, 500, 200),
        OptionContractQuote("SPYC505", expiry, Decimal("505"), "call", Decimal("1.0"), Decimal("1.2"), Decimal("1.1"), Decimal("0.19"), Decimal("0.24"), None, None, None, 500, 200),
        OptionContractQuote("SPYP500", expiry, Decimal("500"), "put", Decimal("4.6"), Decimal("5.0"), Decimal("4.8"), Decimal("0.20"), Decimal("-0.31"), None, None, None, 500, 200),
        OptionContractQuote("SPYP495", expiry, Decimal("495"), "put", Decimal("1.0"), Decimal("1.2"), Decimal("1.1"), Decimal("0.19"), Decimal("-0.22"), None, None, None, 500, 200),
    ]


def test_strike_selection_obeys_delta_filters() -> None:
    candidates = build_convex_trade_candidates(
        ConvexityOpportunity(
            symbol="SPY",
            setup_type="momentum",
            event_time=None,
            spot_price=Decimal("500"),
            expected_move_pct=Decimal("0.10"),
            implied_move_pct=Decimal("0.03"),
            realized_vol_pct=Decimal("0.25"),
            implied_vol_pct=Decimal("0.20"),
            volume_spike_ratio=Decimal("2.0"),
            breakout_score=Decimal("0.05"),
            metadata={"direction": "bullish"},
        ),
        _chain(),
        ConvexityAccountState(nav=Decimal("500"), available_cash=Decimal("500")),
    )

    assert candidates
    assert any(candidate.structure_type in {"long_call", "call_spread"} for candidate in candidates)


def test_dead_liquidity_contracts_are_rejected() -> None:
    expiry = date(2026, 4, 17)
    illiquid_chain = [
        OptionContractQuote("SPYC500", expiry, Decimal("500"), "call", Decimal("0"), Decimal("8"), Decimal("4"), Decimal("0.20"), Decimal("0.20"), None, None, None, 0, 0),
    ]
    candidates = build_convex_trade_candidates(
        ConvexityOpportunity(
            symbol="SPY",
            setup_type="momentum",
            event_time=datetime(2026, 4, 1, 12, 0, tzinfo=timezone.utc),
            spot_price=Decimal("500"),
            expected_move_pct=Decimal("0.10"),
            implied_move_pct=Decimal("0.02"),
            realized_vol_pct=Decimal("0.25"),
            implied_vol_pct=Decimal("0.20"),
            volume_spike_ratio=Decimal("2.0"),
            breakout_score=Decimal("0.05"),
            metadata={"direction": "bullish"},
        ),
        illiquid_chain,
        ConvexityAccountState(nav=Decimal("500"), available_cash=Decimal("500")),
    )

    assert candidates == []
