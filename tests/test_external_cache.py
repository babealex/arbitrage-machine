from datetime import datetime, timezone
from pathlib import Path

from app.config import Settings
from app.external_data.fed_futures import FedFuturesProvider
from app.models import ExternalDistribution


class FailingSession:
    def get(self, url, timeout=20):
        raise RuntimeError("network down")


def test_external_provider_uses_fresh_cache(tmp_path: Path) -> None:
    settings = Settings(external_cache_path=tmp_path / "cache.json", external_cache_freshness_seconds=3600)
    provider = FedFuturesProvider(settings, session=FailingSession())
    cached = {
        "fetched_at": datetime.now(timezone.utc).isoformat(),
        "source": "live",
        "distributions": {
            "fed": {
                "source": "fed_futures",
                "key": "fed",
                "values": {"5.25": 0.4, "5.5": 0.6},
                "as_of": datetime.now(timezone.utc).isoformat(),
                "metadata": {},
            }
        },
    }
    settings.external_cache_path.write_text(__import__("json").dumps(cached), encoding="utf-8")
    result = provider.fetch()
    assert result.source == "cache"
    assert not result.stale
    assert "fed" in result.distributions
