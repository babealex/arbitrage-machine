from __future__ import annotations

from datetime import date
from decimal import Decimal

from app.strategies.convexity.expected_move import (
    implied_move_from_atm_iv,
    implied_move_from_atm_straddle,
    move_edge,
    realized_event_move_estimate,
)
from app.strategies.convexity.models import OptionContractQuote


def _quotes() -> list[OptionContractQuote]:
    expiry = date(2026, 4, 17)
    return [
        OptionContractQuote("SPYC500", expiry, Decimal("500"), "call", Decimal("4.8"), Decimal("5.2"), Decimal("5.0"), Decimal("0.20"), Decimal("0.50"), None, None, None, 500, 200),
        OptionContractQuote("SPYP500", expiry, Decimal("500"), "put", Decimal("4.6"), Decimal("5.0"), Decimal("4.8"), Decimal("0.20"), Decimal("-0.50"), None, None, None, 500, 200),
    ]


def test_implied_move_from_atm_straddle() -> None:
    implied = implied_move_from_atm_straddle(quotes=_quotes(), spot_price=Decimal("500"))
    assert implied == Decimal("9.8") / Decimal("500")


def test_expected_move_gap_logic() -> None:
    expected = realized_event_move_estimate(
        realized_vol_lookahead_estimate=Decimal("0.04"),
        event_history_median_move=Decimal("0.06"),
        breakout_adjusted_move=Decimal("0.05"),
    )
    implied = implied_move_from_atm_iv(atm_iv=Decimal("0.20"), days_to_expiry=25)

    assert expected == Decimal("0.06")
    assert implied is not None
    assert move_edge(expected_move_pct=expected, implied_move_pct=implied) > 0
