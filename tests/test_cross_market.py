from datetime import datetime, timedelta, timezone

from app.models import Market, OrderBookLevel, OrderBookSnapshot
from app.strategies.cross_market import CrossMarketStrategy, fee_from_probability


def make_book(yes_price: float, no_price: float, size: int = 100) -> OrderBookSnapshot:
    return OrderBookSnapshot(
        market_ticker="FED-1",
        yes_asks=[OrderBookLevel(price=yes_price, size=size)],
        no_asks=[OrderBookLevel(price=no_price, size=size)],
    )


def test_cross_market_edge_uses_fee_and_slippage() -> None:
    strategy = CrossMarketStrategy(min_edge=0.02, slippage_buffer=0.01, min_depth=10, min_time_to_expiry_seconds=600, max_position_pct=0.02, directional_allowed=True)
    market = Market(
        ticker="FED-1",
        event_ticker="FED",
        title="Fed target rate above 5.25 after June 2026 meeting",
        subtitle="",
        status="open",
        yes_ask=40,
        no_ask=60,
        expiry=datetime.now(timezone.utc) + timedelta(hours=2),
    )
    signals, evaluations = strategy.evaluate(market, 0.50, make_book(0.40, 0.60), equity=1000)
    assert len(signals) == 1
    expected_edge = (0.50 - 0.40) - fee_from_probability(0.40) - 0.01
    assert round(signals[0].probability_edge, 6) == round(expected_edge, 6)
    assert evaluations[0].generated


def test_cross_market_filters_on_depth_and_time() -> None:
    strategy = CrossMarketStrategy(min_edge=0.02, slippage_buffer=0.01, min_depth=10, min_time_to_expiry_seconds=600, max_position_pct=0.02, directional_allowed=True)
    market = Market(
        ticker="FED-1",
        event_ticker="FED",
        title="Fed target rate above 5.25 after June 2026 meeting",
        subtitle="",
        status="open",
        yes_ask=40,
        no_ask=60,
        expiry=datetime.now(timezone.utc) + timedelta(minutes=5),
    )
    signals, evaluations = strategy.evaluate(market, 0.50, make_book(0.40, 0.60, size=5), equity=1000)
    assert not signals
    assert evaluations[0].reason == "too_close_to_expiry"
