from app.models import Market
from app.strategies.partition import PartitionStrategy


def test_partition_rejects_dead_markets_in_event_group() -> None:
    strategy = PartitionStrategy(min_edge_bps=10, slippage_buffer_bps=0, max_position_pct=0.02, min_depth=1, min_coverage=0.98)
    all_event_markets = [
        Market(ticker="A", event_ticker="E", title="CPI range A", subtitle="", status="open", yes_ask=40),
        Market(ticker="B", event_ticker="E", title="CPI range B", subtitle="", status="open", yes_ask=30),
        Market(ticker="C", event_ticker="E", title="CPI range C", subtitle="", status="open", yes_ask=None),
    ]
    tradeable = [all_event_markets[0], all_event_markets[1]]

    _, evaluations = strategy.evaluate(tradeable, equity=1000, all_event_markets=all_event_markets)

    assert evaluations[0].reason == "invalid_partition_due_to_dead_markets"
    assert evaluations[0].metadata["dead_markets"] == ["C"]


def test_partition_rejects_incomplete_probability_coverage() -> None:
    strategy = PartitionStrategy(min_edge_bps=10, slippage_buffer_bps=0, max_position_pct=0.02, min_depth=1, min_coverage=0.98)
    all_event_markets = [
        Market(ticker="A", event_ticker="E", title="CPI range A", subtitle="", status="open", yes_ask=20),
        Market(ticker="B", event_ticker="E", title="CPI range B", subtitle="", status="open", yes_ask=30),
        Market(ticker="C", event_ticker="E", title="CPI range C", subtitle="", status="open", yes_ask=25),
    ]

    _, evaluations = strategy.evaluate(all_event_markets, equity=1000, all_event_markets=all_event_markets)

    assert evaluations[0].reason == "incomplete_partition_coverage"
    assert evaluations[0].metadata["missing_probability_mass"] > 0
