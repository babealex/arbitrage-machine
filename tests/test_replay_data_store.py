from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from app.replay.data_models import NormalizedBar, ResearchChunkMetadata
from app.replay.data_store import HistoricalResearchDataStore
from app.replay.providers import ProviderFetchResult


class DummyProvider:
    provider_name = "dummy"
    provider_version = "dummy-v1"

    def __init__(self, provider_interval_returned: str = "1m") -> None:
        self.provider_interval_returned = provider_interval_returned
        self.calls: list[tuple[str, datetime, datetime, str]] = []

    def fetch(self, symbol: str, interval: str, start: datetime, end: datetime) -> ProviderFetchResult:
        self.calls.append((symbol, start, end, interval))
        bars: list[NormalizedBar] = []
        cursor = start.replace(second=0, microsecond=0)
        step = timedelta(minutes=1)
        while cursor <= end:
            minute = int((cursor - start).total_seconds() // 60)
            bars.append(
                NormalizedBar(
                    symbol=symbol,
                    timestamp_utc=cursor,
                    open=100.0 + minute,
                    high=100.5 + minute,
                    low=99.5 + minute,
                    close=100.0 + minute,
                    volume=1000.0 + minute,
                    requested_interval=interval,
                    provider_interval_returned=self.provider_interval_returned,
                    provider=self.provider_name,
                    provider_version=self.provider_version,
                    ingest_timestamp_utc=datetime(2026, 3, 19, tzinfo=timezone.utc),
                )
            )
            cursor += step
        warnings = []
        if self.provider_interval_returned != interval:
            warnings.append(f"provider_granularity_downgrade:{interval}->{self.provider_interval_returned}")
        return ProviderFetchResult(bars=bars, provider_interval_returned=self.provider_interval_returned, warnings=warnings)


def test_chunk_creation_and_full_store_hit(tmp_path: Path) -> None:
    provider = DummyProvider()
    store = HistoricalResearchDataStore(tmp_path / "research", provider)
    start = datetime(2026, 3, 17, 13, 0, tzinfo=timezone.utc)
    end = start + timedelta(minutes=5)
    first = store.get_bars("SPY", "1m", start, end)
    second = store.get_bars("SPY", "1m", start, end)
    assert len(first) == len(second)
    assert len(provider.calls) == 1
    manifest = (tmp_path / "research" / "dummy" / "SPY" / "1m" / "manifest.json").read_text(encoding="utf-8")
    assert "2026-03" in manifest
    assert store.stats["full_store_hits"] == 1


def test_overlapping_request_reuses_stored_chunk(tmp_path: Path) -> None:
    provider = DummyProvider()
    store = HistoricalResearchDataStore(tmp_path / "research", provider)
    march_start = datetime(2026, 3, 1, tzinfo=timezone.utc)
    march_end = datetime(2026, 3, 31, 23, 59, tzinfo=timezone.utc)
    store.get_bars("QQQ", "1m", march_start, march_start + timedelta(minutes=10))
    store.get_bars("QQQ", "1m", march_start + timedelta(days=1), march_start + timedelta(days=1, minutes=10))
    assert len(provider.calls) == 1


def test_partial_coverage_fetches_missing_month_only(tmp_path: Path) -> None:
    provider = DummyProvider()
    store = HistoricalResearchDataStore(tmp_path / "research", provider)
    march_start = datetime(2026, 3, 31, 23, 50, tzinfo=timezone.utc)
    april_end = datetime(2026, 4, 1, 0, 10, tzinfo=timezone.utc)
    store.get_bars("TLT", "1m", march_start, march_start + timedelta(minutes=5))
    store.get_bars("TLT", "1m", march_start, april_end)
    assert len(provider.calls) == 2
    assert provider.calls[0][1].month == 3
    assert provider.calls[1][1].month == 4
    assert store.stats["partial_fetches"] == 1


def test_refresh_data_refetches_existing_chunk(tmp_path: Path) -> None:
    provider = DummyProvider()
    store = HistoricalResearchDataStore(tmp_path / "research", provider)
    start = datetime(2026, 3, 17, 13, 0, tzinfo=timezone.utc)
    end = start + timedelta(minutes=2)
    store.get_bars("UUP", "1m", start, end)
    store.get_bars("UUP", "1m", start, end, refresh=True)
    assert len(provider.calls) == 2


def test_cache_only_requires_existing_chunk(tmp_path: Path) -> None:
    provider = DummyProvider()
    store = HistoricalResearchDataStore(tmp_path / "research", provider)
    start = datetime(2026, 3, 17, 13, 0, tzinfo=timezone.utc)
    end = start + timedelta(minutes=2)
    with pytest.raises(RuntimeError):
        store.get_bars("BTC-USD", "1m", start, end, cache_only=True)


def test_duplicate_timestamp_deduplication_prefers_fresh_bar(tmp_path: Path) -> None:
    class DuplicateProvider(DummyProvider):
        def fetch(self, symbol: str, interval: str, start: datetime, end: datetime) -> ProviderFetchResult:
            result = super().fetch(symbol, interval, start, end)
            duplicate = result.bars[0]
            result.bars.append(
                NormalizedBar(
                    symbol=duplicate.symbol,
                    timestamp_utc=start + timedelta(minutes=1),
                    open=999.0,
                    high=999.0,
                    low=999.0,
                    close=999.0,
                    volume=999.0,
                    requested_interval=duplicate.requested_interval,
                    provider_interval_returned=duplicate.provider_interval_returned,
                    provider=duplicate.provider,
                    provider_version=duplicate.provider_version,
                    ingest_timestamp_utc=duplicate.ingest_timestamp_utc,
                )
            )
            return result

    provider = DuplicateProvider()
    store = HistoricalResearchDataStore(tmp_path / "research", provider)
    start = datetime(2026, 3, 1, 0, 0, tzinfo=timezone.utc)
    bars = store.get_bars("XLE", "1m", start, start + timedelta(minutes=1))
    conflicted = [bar for bar in bars if bar.timestamp_utc == start + timedelta(minutes=1)]
    assert conflicted and conflicted[0].close == 999.0


def test_provider_granularity_metadata_retained(tmp_path: Path) -> None:
    provider = DummyProvider(provider_interval_returned="5m")
    store = HistoricalResearchDataStore(tmp_path / "research", provider)
    start = datetime(2026, 3, 17, 13, 0, tzinfo=timezone.utc)
    bars = store.get_bars("SPY", "1m", start, start + timedelta(minutes=1))
    assert bars[0].provider_interval_returned == "5m"
    assert store.stats["granularity_downgrade_count"] == 1
