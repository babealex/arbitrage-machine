from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

from app.config import Settings
from app.db import Database
from app.persistence.contracts import ClassifiedEventRecord, TradeCandidateRecord
from app.persistence.event_repo import EventRepository
from app.portfolio.runtime import ConvexityCatalystProvider


def test_convexity_catalyst_provider_uses_configured_events_and_recent_event_bias(tmp_path: Path) -> None:
    db = Database(tmp_path / "convexity_runtime_inputs.db")
    repo = EventRepository(db)
    news_event_id = repo.persist_news_event(
        type(
            "NewsEvent",
            (),
            {
                "external_id": "news-1",
                "timestamp_utc": datetime(2026, 3, 23, 12, 0, tzinfo=timezone.utc).isoformat(),
                "source": "test",
                "headline": "AAPL earnings preview",
                "body": None,
                "url": None,
                "metadata_json": {},
            },
        )()
    )
    classified_id = repo.persist_classified_event(
        ClassifiedEventRecord(
            news_event_id=news_event_id,
            event_type="earnings",
            region="US",
            severity=0.8,
            directional_bias="bullish",
            assets_affected=["AAPL"],
            metadata_json={},
            created_at=datetime.now(timezone.utc).isoformat(),
        )
    )
    repo.persist_trade_candidate(
        TradeCandidateRecord(
            classified_event_id=classified_id,
            symbol="AAPL",
            side="buy",
            event_type="earnings",
            status="confirmed",
            confidence=0.9,
            confirmation_score=0.8,
            metadata_json={},
            created_at=datetime.now(timezone.utc).isoformat(),
        )
    )
    settings = Settings()
    settings.convexity_macro_events = "SPY|macro|2026-03-25T08:30:00-04:00|neutral|0.02"
    settings.convexity_earnings_events = "AAPL|earnings|2026-03-28T16:30:00-04:00|bullish|0.05"
    provider = ConvexityCatalystProvider(settings, repo)

    calendars, news_state, input_summary = provider.build_inputs(["SPY", "AAPL"])

    assert len(calendars["SPY"]) == 1
    assert calendars["SPY"][0]["setup_type"] == "macro"
    assert len(calendars["AAPL"]) == 1
    assert calendars["AAPL"][0]["setup_type"] == "earnings"
    assert news_state["AAPL"]["direction"] == "bullish"
    assert Decimal(news_state["AAPL"]["confidence"]) == Decimal("0.9")
    assert input_summary["configured_macro_events"] == 1
