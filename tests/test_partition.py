from app.models import Market
from app.strategies.partition import PartitionStrategy


def test_partition_detects_sum_below_one() -> None:
    strategy = PartitionStrategy(min_edge_bps=10, slippage_buffer_bps=0, max_position_pct=0.02, min_coverage=0.80)
    markets = [
        Market(ticker="A", event_ticker="E", title="Fed rate range A", subtitle="", status="open", yes_ask=20),
        Market(ticker="B", event_ticker="E", title="Fed rate range B", subtitle="", status="open", yes_ask=30),
        Market(ticker="C", event_ticker="E", title="Fed rate range C", subtitle="", status="open", yes_ask=35),
    ]
    signals = strategy.generate(markets, equity=1000)
    assert len(signals) == 1
    assert signals[0].action == "buy_partition"
