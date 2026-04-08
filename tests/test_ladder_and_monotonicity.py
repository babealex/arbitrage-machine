from app.market_data.ladder_builder import build_ladders
from app.models import Market
from app.strategies.monotonicity import MonotonicityStrategy


def test_build_ladders_sorts_by_strike() -> None:
    markets = [
        Market(ticker="A-300", event_ticker="A", title="Above 300", subtitle="300", status="open"),
        Market(ticker="A-200", event_ticker="A", title="Above 200", subtitle="200", status="open"),
    ]
    ladder = build_ladders(markets)["A"]
    assert [market.ticker for market in ladder] == ["A-200", "A-300"]


def test_monotonicity_detects_invalid_ordering() -> None:
    strategy = MonotonicityStrategy(min_edge_bps=10, slippage_buffer_bps=0, max_position_pct=0.02, min_size_multiple=1.0)
    ladder = [
        Market(
            ticker="A-200",
            event_ticker="A",
            title="Fed rate above 200",
            subtitle="200",
            status="open",
            yes_ask=40,
            yes_bid=39,
            strike=200,
            metadata={"quote_diagnostics": {"best_yes_ask_size": 50, "best_yes_bid_size": 50, "orderbook_snapshot_exists": True}},
        ),
        Market(
            ticker="A-300",
            event_ticker="A",
            title="Fed rate above 300",
            subtitle="300",
            status="open",
            yes_ask=52,
            yes_bid=75,
            strike=300,
            metadata={"quote_diagnostics": {"best_yes_ask_size": 50, "best_yes_bid_size": 50, "orderbook_snapshot_exists": True}},
        ),
    ]
    signals = strategy.generate(ladder, equity=1000)
    assert len(signals) == 1
    assert signals[0].action == "buy_lower_sell_higher"


def test_monotonicity_rejects_insufficient_signal_time_depth() -> None:
    strategy = MonotonicityStrategy(min_edge_bps=10, slippage_buffer_bps=0, max_position_pct=0.02, min_top_level_depth=10)
    ladder = [
        Market(
            ticker="A-200",
            event_ticker="A",
            title="Fed rate above 200",
            subtitle="200",
            status="open",
            yes_ask=40,
            yes_bid=39,
            strike=200,
            metadata={"quote_diagnostics": {"best_yes_ask_size": 5, "best_yes_bid_size": 50, "orderbook_snapshot_exists": True}},
        ),
        Market(
            ticker="A-300",
            event_ticker="A",
            title="Fed rate above 300",
            subtitle="300",
            status="open",
            yes_ask=52,
            yes_bid=55,
            strike=300,
            metadata={"quote_diagnostics": {"best_yes_ask_size": 50, "best_yes_bid_size": 50, "orderbook_snapshot_exists": True}},
        ),
    ]
    signals, evaluations = strategy.evaluate(ladder, equity=1000)
    assert signals == []
    assert evaluations[0].reason == "insufficient_signal_time_depth"


def test_monotonicity_rejects_insufficient_size_multiple() -> None:
    strategy = MonotonicityStrategy(
        min_edge_bps=10,
        slippage_buffer_bps=0,
        max_position_pct=0.02,
        min_top_level_depth=10,
        min_size_multiple=2.0,
    )
    ladder = [
        Market(
            ticker="A-200",
            event_ticker="A",
            title="Fed rate above 200",
            subtitle="200",
            status="open",
            yes_ask=40,
            yes_bid=39,
            strike=200,
            metadata={"quote_diagnostics": {"best_yes_ask_size": 50, "best_yes_bid_size": 50, "orderbook_snapshot_exists": True}},
        ),
        Market(
            ticker="A-300",
            event_ticker="A",
            title="Fed rate above 300",
            subtitle="300",
            status="open",
            yes_ask=52,
            yes_bid=55,
            strike=300,
            metadata={"quote_diagnostics": {"best_yes_ask_size": 50, "best_yes_bid_size": 50, "orderbook_snapshot_exists": True}},
        ),
    ]
    signals, evaluations = strategy.evaluate(ladder, equity=1000)
    assert signals == []
    assert evaluations[0].reason == "insufficient_size_multiple"
