from app.config import Settings
from app.kalshi.rest_client import KalshiRestClient
from app.market_data.service import MarketDataService
from app.models import Market


class QuoteClient(KalshiRestClient):
    def __init__(self, orderbooks):
        self.orderbooks = orderbooks

    def get_orderbook(self, ticker: str, depth: int = 10):
        return self.orderbooks[ticker]


def test_hydrate_structural_quotes_prefers_orderbook_values() -> None:
    service = MarketDataService(QuoteClient({"MKT": {"yes_bids": [[41, 5]], "yes_asks": [[43, 7]]}}), Settings())
    markets = [Market(ticker="MKT", event_ticker="EVT", title="Fed ladder", subtitle="", status="open", yes_bid=39, yes_ask=None)]

    hydrated = service.hydrate_structural_quotes(markets)

    assert len(hydrated) == 1
    assert hydrated[0].yes_bid == 41
    assert hydrated[0].yes_ask == 43
    assert hydrated[0].no_bid == 57
    assert hydrated[0].no_ask == 59
    assert hydrated[0].metadata["quote_diagnostics"]["orderbook_snapshot_exists"] is True


def test_hydrate_structural_quotes_skips_dead_markets() -> None:
    service = MarketDataService(QuoteClient({"DEAD": {"yes_bids": [], "yes_asks": [], "no_bids": [], "no_asks": []}}), Settings())
    markets = [Market(ticker="DEAD", event_ticker="EVT", title="Fed ladder", subtitle="", status="open")]

    hydrated = service.hydrate_structural_quotes(markets)

    assert hydrated == []
    assert service.last_quote_stats.dead_markets_skipped == 1
    assert service.last_quote_stats.quote_completeness_stats["dead_after_enrichment"] == 1
