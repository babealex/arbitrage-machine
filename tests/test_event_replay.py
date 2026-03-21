from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path

from app.classification.service import EventClassifier
from app.mapping.engine import EventTradeMapper
from app.replay.data_store import HistoricalResearchDataStore
from app.replay.edge_analysis import ReplayEdgeAnalyzer
from app.replay.engine import ReplayEngine
from app.replay.features import ReplayFeatureExtractor
from app.replay.market_data import HistoricalMarketData
from app.replay.news_loader import HistoricalNewsLoader
from app.replay.outcomes import ReplayOutcomeDatasetBuilder
from app.replay.providers import ProviderFetchResult
from app.replay.report import ReplayReportWriter
from app.replay.simulator import ReplayTradeSimulator
from app.replay.data_models import NormalizedBar


class StubProvider:
    provider_name = "stub"
    provider_version = "stub-v1"

    def __init__(self) -> None:
        self.calls: list[tuple[str, str]] = []

    def fetch(self, symbol: str, interval: str, start: datetime, end: datetime) -> ProviderFetchResult:
        self.calls.append((symbol, interval))
        base = 100.0
        if symbol in {"UUP", "XLE", "CL=F", "^VIX"}:
            base = 105.0
        bars = []
        cursor = start.replace(second=0, microsecond=0)
        step = timedelta(minutes=1 if interval == "1m" else 5 if interval == "5m" else 1440)
        while cursor <= end:
            minute = int((cursor - start).total_seconds() // 60)
            close = base + (minute / 5.0)
            bars.append(
                NormalizedBar(
                    symbol=symbol,
                    timestamp_utc=cursor,
                    open=close - 0.1,
                    high=close + 0.2,
                    low=close - 0.2,
                    close=close,
                    volume=1000.0 + minute,
                    requested_interval=interval,
                    provider_interval_returned=interval,
                    provider="stub",
                    provider_version="stub-v1",
                    ingest_timestamp_utc=datetime(2026, 3, 19, tzinfo=timezone.utc),
                )
            )
            cursor += step
        return ProviderFetchResult(bars=bars, provider_interval_returned=interval, warnings=[])


def test_news_loader_json_fixture() -> None:
    fixture = Path("C:/Users/alexa/arbitrage-machine/tests/fixtures/historical_news_sample.json")
    items = HistoricalNewsLoader().load(json_path=str(fixture))
    assert len(items) == 2
    assert items[0].headline.startswith("US CPI")


def test_replay_engine_and_report_with_store(tmp_path: Path) -> None:
    fixture = Path("C:/Users/alexa/arbitrage-machine/tests/fixtures/historical_news_sample.json")
    items = HistoricalNewsLoader().load(json_path=str(fixture))
    provider = StubProvider()
    store = HistoricalResearchDataStore(tmp_path / "research", provider)
    market_data = HistoricalMarketData(store=store)
    engine = ReplayEngine(
        classifier=EventClassifier(enable_openai=False),
        mapper=EventTradeMapper(),
        market_data=market_data,
        simulator=ReplayTradeSimulator(capital=100000, risk_fraction=0.01),
        confirmation_needed=1,
        scale_steps=1,
        scale_interval_seconds=0,
        min_severity=0.4,
    )
    results = engine.run(items)
    report = ReplayReportWriter(tmp_path / "replay_report.json").write(
        results,
        {"research_data_stats": market_data.research_data_stats, "cache_stats": market_data.research_data_stats, "cache_enabled": True},
    )
    assert report["summary"]["total_trades"] > 0
    first_fetches = len(provider.calls)
    results_second = engine.run(items)
    assert results_second
    assert len(provider.calls) == first_fetches
    outcomes, summary = ReplayOutcomeDatasetBuilder(ReplayFeatureExtractor(market_data)).build(results_second)
    analysis = ReplayEdgeAnalyzer(min_samples=1).analyze(outcomes)
    report_second = ReplayReportWriter(tmp_path / "replay_report_second.json").write(
        results_second,
        {
            "research_data_stats": market_data.research_data_stats,
            "cache_stats": market_data.research_data_stats,
            "cache_enabled": True,
            "outcome_dataset": summary,
            "edge_analysis": {"candidate_rule_count": analysis["candidate_rule_count"]},
        },
    )
    assert report_second["research_data_stats"]["full_store_hits"] > 0
    assert report_second["outcome_dataset"]["row_count"] > 0
