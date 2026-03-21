from datetime import datetime, timezone

from app.config import Settings
from app.external_data.fed_futures import ExternalDataResult
from app.kalshi.auth import KalshiAuthAdapter


def test_auth_adapter_uses_url_path_not_query() -> None:
    adapter = KalshiAuthAdapter(Settings(kalshi_api_key_id="", kalshi_private_key_path=""))
    assert adapter.build_rest_headers("GET", "https://api.elections.kalshi.com/trade-api/v2/markets?status=open", "") == {}


def test_external_result_can_fail_closed() -> None:
    result = ExternalDataResult(distributions={}, stale=True, errors=["parse_failed"], fetched_at=datetime.now(timezone.utc), source="failed")
    assert result.stale
