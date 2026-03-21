from app.config import Settings
from app.kalshi.rest_client import KalshiRestClient
from app.market_data.service import MarketDataService


class DummyClient(KalshiRestClient):
    def __init__(self):
        pass

    def list_series(self):
        return [
            {"ticker": "KXFEDEND", "title": "Will Trump end the Federal Reserve before Jan 20, 2029?", "category": "Politics", "tags": ["Fed"]},
            {"ticker": "KXFEDDECISION", "title": "Fed meeting", "category": "Economics", "tags": ["Fed"]},
            {"ticker": "KXU3MAX", "title": "How high will unemployment get before 2027?", "category": "Economics", "tags": ["unemployment"]},
            {"ticker": "KXSPORT", "title": "Sports market", "category": "Sports", "tags": []},
        ]

    def list_events_page(self, cursor=None, series_ticker=None):
        return {"events": [{"event_ticker": "FED-1"}]} if series_ticker == "KXFEDDECISION" else {"events": []}

    def _request(self, method, path, params=None, payload=None, auth=False):
        if path == "/markets" and params == {"status": "open", "series_ticker": "KXFEDDECISION"}:
            return {"markets": [{"ticker": "KXFEDDECISION-1", "event_ticker": "FED-1", "title": "Will Fed hike?", "subtitle": "", "status": "open"}]}
        if path == "/markets" and params == {"status": "open", "series_ticker": "KXU3MAX"}:
            return {"markets": [{"ticker": "KXU3MAX-27-9", "event_ticker": "U3MAX-27", "title": "How high will unemployment get before 2027?", "subtitle": "", "status": "open"}]}
        if path == "/markets" and params == {"status": "open", "series_ticker": "KXFEDEND"}:
            return {"markets": [{"ticker": "KXFEDEND-29-JAN20", "event_ticker": "FEDEND-29", "title": "Will Trump end the Federal Reserve before Jan 20, 2029?", "subtitle": "", "status": "open"}]}
        return {"markets": []}


def test_safe_mode_uses_macro_series_discovery() -> None:
    service = MarketDataService(DummyClient(), Settings())
    markets = service.refresh_markets()
    assert len(markets) == 3
    assert service.last_discovery_stats.discovery_mode_used == "macro_series_first"
    assert service.last_discovery_stats.candidate_series_found == 3
    assert markets[0].ticker == "KXU3MAX-27-9"
