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
from app.replay.simulator import ReplayTradeSimulator
from tests.test_event_replay import StubProvider
from app.replay.providers import ProviderFetchResult
from app.replay.data_models import NormalizedBar


def _build_rows(tmp_path: Path) -> list[dict]:
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
    rows, _ = ReplayOutcomeDatasetBuilder(ReplayFeatureExtractor(market_data)).build(results)
    return rows


def test_feature_extraction_is_deterministic_and_no_lookahead(tmp_path: Path) -> None:
    rows = _build_rows(tmp_path)
    first = rows[0]
    second_rows = _build_rows(tmp_path / "rerun")
    second = second_rows[0]
    assert first["event_id"] == second["event_id"]
    assert first["pre_15m_return"] == second["pre_15m_return"]
    assert first["entry_bar_timestamp_utc"] >= first["event_timestamp_utc"]
    assert first["feature_window_bar_count"] > 0


def test_market_snapshot_uses_first_bar_at_or_after_event_and_fixed_interval(tmp_path: Path) -> None:
    provider = StubProvider()
    store = HistoricalResearchDataStore(tmp_path / "research", provider)
    market_data = HistoricalMarketData(store=store)
    event_ts = datetime(2026, 3, 19, 14, 0, 30, tzinfo=timezone.utc)
    snapshot = market_data.snapshot_for_event("SPY", event_ts)
    assert snapshot.metadata["requested_interval"] == "1m"
    assert snapshot.event_time_price.timestamp_utc == datetime(2026, 3, 19, 14, 1, 0, tzinfo=timezone.utc)
    assert snapshot.pre_event_price.timestamp_utc <= event_ts - timedelta(minutes=15)
    assert snapshot.metadata["entry_anchor_mode"] == "first_tradable_bar_at_or_after_event_within_same_session"
    assert snapshot.metadata["event_to_entry_delay_seconds"] == 30.0


class SessionEdgeProvider:
    provider_name = "session-edge"
    provider_version = "session-edge-v1"

    def fetch(self, symbol: str, interval: str, start: datetime, end: datetime) -> ProviderFetchResult:
        bars = [
            NormalizedBar(
                symbol=symbol,
                timestamp_utc=datetime(2026, 3, 20, 19, 58, 0, tzinfo=timezone.utc),
                open=100.0,
                high=100.1,
                low=99.9,
                close=100.0,
                volume=1000.0,
                requested_interval=interval,
                provider_interval_returned=interval,
                provider="session-edge",
                provider_version="session-edge-v1",
                ingest_timestamp_utc=datetime(2026, 3, 20, tzinfo=timezone.utc),
            ),
            NormalizedBar(
                symbol=symbol,
                timestamp_utc=datetime(2026, 3, 20, 19, 59, 0, tzinfo=timezone.utc),
                open=100.0,
                high=100.2,
                low=99.9,
                close=100.1,
                volume=1000.0,
                requested_interval=interval,
                provider_interval_returned=interval,
                provider="session-edge",
                provider_version="session-edge-v1",
                ingest_timestamp_utc=datetime(2026, 3, 20, tzinfo=timezone.utc),
            ),
            NormalizedBar(
                symbol=symbol,
                timestamp_utc=datetime(2026, 3, 20, 20, 0, 0, tzinfo=timezone.utc),
                open=100.1,
                high=100.3,
                low=100.0,
                close=100.2,
                volume=1000.0,
                requested_interval=interval,
                provider_interval_returned=interval,
                provider="session-edge",
                provider_version="session-edge-v1",
                ingest_timestamp_utc=datetime(2026, 3, 20, tzinfo=timezone.utc),
            ),
            NormalizedBar(
                symbol=symbol,
                timestamp_utc=datetime(2026, 3, 21, 13, 30, 0, tzinfo=timezone.utc),
                open=101.0,
                high=101.2,
                low=100.9,
                close=101.1,
                volume=1000.0,
                requested_interval=interval,
                provider_interval_returned=interval,
                provider="session-edge",
                provider_version="session-edge-v1",
                ingest_timestamp_utc=datetime(2026, 3, 21, tzinfo=timezone.utc),
            ),
        ]
        return ProviderFetchResult(bars=bars, provider_interval_returned=interval, warnings=[])


def test_replay_snapshot_blocks_after_hours_entry(tmp_path: Path) -> None:
    provider = SessionEdgeProvider()
    store = HistoricalResearchDataStore(tmp_path / "research_afterhours", provider)
    market_data = HistoricalMarketData(store=store)
    event_ts = datetime(2026, 3, 20, 20, 30, 0, tzinfo=timezone.utc)
    snapshot = market_data.snapshot_for_event("SPY", event_ts)
    assert snapshot.event_time_price.price is None
    assert snapshot.plus_5m.price is None
    assert snapshot.metadata["event_within_tradable_session"] is False
    assert snapshot.metadata["entry_blocked_reason"] == "outside_tradable_session"


def test_replay_snapshot_does_not_use_next_session_for_intraday_horizons(tmp_path: Path) -> None:
    provider = SessionEdgeProvider()
    store = HistoricalResearchDataStore(tmp_path / "research", provider)
    market_data = HistoricalMarketData(store=store)
    event_ts = datetime(2026, 3, 20, 19, 58, 30, tzinfo=timezone.utc)
    snapshot = market_data.snapshot_for_event("SPY", event_ts)
    assert snapshot.event_time_price.price is None
    assert snapshot.plus_5m.price is None
    assert snapshot.plus_60m.price is None
    assert snapshot.metadata["entry_blocked_reason"] == "boundary_forward_bar_disallowed"


def test_outcome_rows_cover_all_horizons_and_warning_flags(tmp_path: Path) -> None:
    rows = _build_rows(tmp_path)
    horizons = {row["horizon"] for row in rows}
    assert horizons == {"5m", "15m", "60m", "eod"}
    assert all("mapped_symbol" in row for row in rows)
    assert all("realized_return" in row for row in rows)
    assert all("granularity_downgrade_flag" in row for row in rows)


def test_edge_analysis_filters_small_samples_and_is_stable(tmp_path: Path) -> None:
    rows = _build_rows(tmp_path)
    analysis_one = ReplayEdgeAnalyzer(min_samples=2).analyze(rows)
    analysis_two = ReplayEdgeAnalyzer(min_samples=2).analyze(rows)
    assert analysis_one == analysis_two
    assert analysis_one["config"]["min_samples"] == 2
    assert "by_horizon" in analysis_one
    assert analysis_one["candidate_rule_count"] >= 0
