from app.models import Market
from app.strategies.partition import PartitionStrategy


def test_partition_rejects_singleton_group() -> None:
    strategy = PartitionStrategy(min_edge_bps=10, slippage_buffer_bps=0, max_position_pct=0.02, min_depth=1)
    _, evaluations = strategy.evaluate([Market(ticker="A", event_ticker="E", title="Fed A", subtitle="", status="open", yes_ask=20)], equity=1000)
    assert evaluations[0].reason == "singleton_group"


def test_partition_rejects_non_exhaustive_candidates() -> None:
    strategy = PartitionStrategy(min_edge_bps=10, slippage_buffer_bps=0, max_position_pct=0.02, min_depth=1)
    markets = [
        Market(ticker="A", event_ticker="E", title="Random outcome A", subtitle="", status="open", yes_ask=20),
        Market(ticker="B", event_ticker="E", title="Random outcome B", subtitle="", status="open", yes_ask=30),
    ]
    _, evaluations = strategy.evaluate(markets, equity=1000)
    assert evaluations[0].reason == "not_exhaustive_candidate"
