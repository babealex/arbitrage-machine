from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path

from app.config import Settings
from app.classification.rule_engine import classify_with_rules
from app.db import Database
from app.events.models import NewsItem, TradeIdea
from app.execution.broker import PaperBroker
from app.execution.event_engine import EventExecutionEngine
from app.execution.router import ExecutionRouter
from app.mapping.engine import EventTradeMapper
from app.persistence.contracts import EventEntryQueueRecord, TradeCandidateRecord
from app.persistence.event_repo import EventRepository
from app.persistence.kalshi_repo import KalshiRepository
from app.persistence.market_state_repo import MarketStateRepository
from app.persistence.reporting_repo import ReportingRepository
from app.reporting import PaperTradingReporter, build_event_run_manifest
from app.portfolio.state import PortfolioState
from app.risk.manager import RiskManager


IN_SESSION_TS = datetime(2026, 3, 20, 18, 0, 0, tzinfo=timezone.utc)


class StubBroker:
    def __init__(self) -> None:
        self.execution_enabled = True
        self.cash = 100000.0

    def place_order(self, symbol: str, side: str, quantity: int, **kwargs):
        from app.events.models import BrokerFill
        from app.execution.broker import PaperExecutionResult

        timestamp = kwargs.get("signal_ts") or datetime.now(timezone.utc)
        fill = BrokerFill(symbol=symbol, side=side, quantity=quantity, price=100.0, timestamp_utc=timestamp)
        return PaperExecutionResult(
            fill=fill,
            requested_quantity=quantity,
            filled_quantity=quantity,
            remaining_quantity=0,
            status="filled",
            submitted_at=fill.timestamp_utc,
            order_style="marketable",
            book=None,
            audit_events=[{"timestamp_utc": fill.timestamp_utc.isoformat(), "state": "filled", "decision": "stub_fill"}],
        )

    def get_price(self, symbol: str, *, liquidation_side: str | None = None) -> float | None:
        return 100.0

    def planned_exit_at(self, max_hold_seconds: int):
        return datetime.now(timezone.utc) + timedelta(seconds=max_hold_seconds)

    def snapshot_order_book(self, symbol: str, signal_ts=None):
        from app.execution.broker import PaperOrderBook, PaperBookLevel

        ts = signal_ts or datetime.now(timezone.utc)
        return PaperOrderBook(
            symbol=symbol,
            mid_price=100.0,
            bid_price=100.0,
            ask_price=100.0,
            bid_levels=[PaperBookLevel(price=100.0, size=100)],
            ask_levels=[PaperBookLevel(price=100.0, size=100)],
            ts=ts,
            source="stub_book",
        )


def _event_engine(db: Database, repo: EventRepository, broker) -> EventExecutionEngine:
    signal_repo = KalshiRepository(db)
    return EventExecutionEngine(
        repo,
        signal_repo,
        broker,
        ExecutionRouter(
            None,
            signal_repo,
            PortfolioState(None),
            RiskManager(1.0, 10.0, 10.0, 10),
            __import__("logging").getLogger("event-test"),
            False,
            market_state_repo=MarketStateRepository(db),
        ),
    )


def test_rule_classifier_detects_inflation_hot() -> None:
    item = NewsItem(
        external_id="x",
        timestamp_utc=datetime.now(timezone.utc),
        source="test",
        headline="US CPI hotter than expected as core prices accelerate",
        body="The inflation report came in hotter than expected.",
    )
    classified = classify_with_rules(item)
    assert classified is not None
    assert classified.event_type == "macro"
    assert "inflation_hot" in classified.tags
    assert classified.directional_bias == "bearish"


def test_mapper_creates_trade_ideas() -> None:
    item = NewsItem(
        external_id="y",
        timestamp_utc=datetime.now(timezone.utc),
        source="test",
        headline="Israel conflict pushes oil higher",
        body="Middle East conflict raises supply concerns.",
    )
    classified = classify_with_rules(item)
    assert classified is not None
    ideas = EventTradeMapper().map_event(classified, scale_steps=2, scale_interval_seconds=60, confirmation_needed=2)
    assert any(idea.symbol == "XOM" and idea.side == "buy" for idea in ideas)
    assert any(idea.symbol == "DAL" and idea.side == "sell" for idea in ideas)


def test_event_execution_and_reporting(tmp_path: Path) -> None:
    db = Database(tmp_path / "event_v1.db")
    event_repo = EventRepository(db)
    reporting_repo = ReportingRepository(db)
    broker = StubBroker()
    engine = _event_engine(db, event_repo, broker)
    base_ts = datetime(2026, 3, 20, 18, 0, 0, tzinfo=timezone.utc)
    candidate_id = event_repo.persist_trade_candidate(
        TradeCandidateRecord(
            classified_event_id=1,
            symbol="SPY",
            side="sell",
            event_type="macro",
            status="confirmed",
            confidence=0.8,
            confirmation_score=1.0,
            metadata_json={"news_event_id": 1},
            created_at=base_ts.isoformat(),
        )
    )
    for tranche_index in range(2):
        event_repo.persist_entry_queue_item(
            EventEntryQueueRecord(
                trade_candidate_id=candidate_id,
                due_at=(base_ts + timedelta(seconds=tranche_index)).isoformat(),
                symbol="SPY",
                side="sell",
                quantity=5,
                tranche_index=tranche_index + 1,
                total_tranches=2,
                status="pending",
                metadata_json={"news_event_id": 1},
                created_at=base_ts.isoformat(),
            )
        )
    filled = engine.process_due_entries(stop_loss_pct=0.01, max_hold_seconds=0, logger=_NoopLogger(), now=base_ts + timedelta(seconds=5))
    assert filled == 2
    closed = engine.close_due_positions(_NoopLogger(), now=base_ts + timedelta(seconds=10))
    assert closed == 2
    report = PaperTradingReporter(reporting_repo, tmp_path / "report.json").write({"event_driven_loop_stats": {"news_items_seen": 1}})
    assert report["event_driven_summary"]["closed_positions"] == 2
    assert report["event_driven_summary"]["trade_candidates"] == 1
    assert report["execution_report"]["by_strategy"]["event_driven"]["orders"] >= 4


def test_paper_broker_buy_fills_at_ask_not_mid() -> None:
    broker = PaperBroker(
        100000.0,
        True,
        submission_latency_seconds=0.0,
        half_spread_bps=10.0,
        top_level_size=10,
        depth_levels=1,
        price_fetcher=lambda symbol: 100.0,
    )
    result = broker.place_order("SPY", "buy", 5, signal_ts=IN_SESSION_TS)
    assert result.fill is not None
    assert result.fill.price == 100.1
    assert result.fill.price > 100.0


def test_paper_broker_rejects_outside_regular_session() -> None:
    broker = PaperBroker(
        100000.0,
        True,
        submission_latency_seconds=0.0,
        half_spread_bps=10.0,
        top_level_size=10,
        depth_levels=1,
        price_fetcher=lambda symbol: 100.0,
    )
    after_close = datetime(2026, 3, 20, 20, 30, 0, tzinfo=timezone.utc)
    result = broker.place_order("SPY", "buy", 5, signal_ts=after_close)
    assert result.fill is None
    assert result.status == "rejected"
    assert any(event.get("reason") == "market_closed" for event in result.audit_events)


def test_paper_broker_sells_fill_at_bid_not_mid() -> None:
    broker = PaperBroker(
        100000.0,
        True,
        submission_latency_seconds=0.0,
        half_spread_bps=10.0,
        top_level_size=10,
        depth_levels=1,
        price_fetcher=lambda symbol: 100.0,
    )
    result = broker.place_order("SPY", "sell", 5, signal_ts=IN_SESSION_TS)
    assert result.fill is not None
    assert result.fill.price == 99.9
    assert result.fill.price < 100.0


def test_paper_broker_respects_displayed_size_and_partially_fills() -> None:
    broker = PaperBroker(
        100000.0,
        True,
        submission_latency_seconds=0.0,
        half_spread_bps=10.0,
        top_level_size=3,
        depth_levels=1,
        price_fetcher=lambda symbol: 100.0,
    )
    result = broker.place_order("SPY", "buy", 5, signal_ts=IN_SESSION_TS)
    assert result.status == "partial_fill"
    assert result.filled_quantity == 3
    assert result.remaining_quantity == 2
    assert result.fill is not None
    assert result.fill.quantity == 3


def test_paper_broker_thins_liquidity_near_session_boundary() -> None:
    broker = PaperBroker(
        100000.0,
        True,
        submission_latency_seconds=0.0,
        half_spread_bps=10.0,
        top_level_size=8,
        depth_levels=1,
        session_boundary_buffer_seconds=300,
        session_boundary_liquidity_factor=0.25,
        session_boundary_spread_multiplier=2.0,
        price_fetcher=lambda symbol: 100.0,
    )
    near_close = datetime(2026, 3, 20, 19, 58, 0, tzinfo=timezone.utc)
    result = broker.place_order("SPY", "buy", 3, signal_ts=near_close)
    assert result.status == "partial_fill"
    assert result.filled_quantity == 2
    assert result.remaining_quantity == 1
    assert result.fill is not None
    assert result.fill.price == 100.2


def test_passive_paper_order_does_not_fill_without_later_cross() -> None:
    prices = iter([100.0, 100.0])
    broker = PaperBroker(
        100000.0,
        True,
        submission_latency_seconds=0.0,
        half_spread_bps=10.0,
        top_level_size=10,
        depth_levels=1,
        passive_recheck_seconds=1.0,
        price_fetcher=lambda symbol: next(prices),
    )
    result = broker.place_order("SPY", "buy", 5, signal_ts=IN_SESSION_TS, order_style="passive", limit_price=99.95)
    assert result.fill is None
    assert result.status == "unfilled"
    assert any(event.get("reason") == "passive_not_crossed" for event in result.audit_events)


def test_event_engine_routes_execution_through_shared_outcomes_for_partial_fill(tmp_path: Path) -> None:
    db = Database(tmp_path / "audit.db")
    event_repo = EventRepository(db)
    reporting_repo = ReportingRepository(db)
    broker = PaperBroker(
        100000.0,
        True,
        submission_latency_seconds=0.0,
        half_spread_bps=10.0,
        top_level_size=2,
        depth_levels=1,
        price_fetcher=lambda symbol: 100.0,
    )
    engine = _event_engine(db, event_repo, broker)
    base_ts = datetime(2026, 3, 20, 18, 0, 0, tzinfo=timezone.utc)
    candidate_id = event_repo.persist_trade_candidate(
        TradeCandidateRecord(
            classified_event_id=1,
            symbol="SPY",
            side="buy",
            event_type="macro",
            status="confirmed",
            confidence=0.8,
            confirmation_score=1.0,
            metadata_json={"news_event_id": 1},
            created_at=base_ts.isoformat(),
        )
    )
    event_repo.persist_entry_queue_item(
        EventEntryQueueRecord(
            trade_candidate_id=candidate_id,
            due_at=base_ts.isoformat(),
            symbol="SPY",
            side="buy",
            quantity=5,
            tranche_index=1,
            total_tranches=1,
            status="pending",
            metadata_json={"news_event_id": 1},
            created_at=base_ts.isoformat(),
        )
    )
    filled = engine.process_due_entries(stop_loss_pct=0.01, max_hold_seconds=60, logger=_NoopLogger(), now=base_ts + timedelta(seconds=1))
    assert filled == 1
    event_bundle = reporting_repo.load_event_reporting_bundle()
    queue_rows = event_bundle.entry_queue
    assert queue_rows[0]["status"] == "partially_filled"
    positions = event_bundle.positions
    assert positions[0]["quantity"] == 2
    signal_repo = KalshiRepository(db)
    assert any(outcome.filled_quantity == 2 for outcome in signal_repo.load_execution_outcomes())
    assert MarketStateRepository(db).load_market_state_inputs()


def test_event_driven_report_includes_execution_manifest_and_shared_execution_summary(tmp_path: Path) -> None:
    db = Database(tmp_path / "manifest.db")
    event_repo = EventRepository(db)
    reporting_repo = ReportingRepository(db)
    broker = PaperBroker(
        100000.0,
        True,
        submission_latency_seconds=3.0,
        half_spread_bps=12.0,
        top_level_size=2,
        depth_levels=2,
        passive_recheck_seconds=1.0,
        price_fetcher=lambda symbol: 100.0,
    )
    engine = _event_engine(db, event_repo, broker)
    base_ts = datetime(2026, 3, 20, 18, 0, 0, tzinfo=timezone.utc)
    candidate_id = event_repo.persist_trade_candidate(
        TradeCandidateRecord(
            classified_event_id=1,
            symbol="SPY",
            side="buy",
            event_type="macro",
            status="confirmed",
            confidence=0.8,
            confirmation_score=1.0,
            metadata_json={"news_event_id": 1},
            created_at=base_ts.isoformat(),
        )
    )
    event_repo.persist_entry_queue_item(
        EventEntryQueueRecord(
            trade_candidate_id=candidate_id,
            due_at=base_ts.isoformat(),
            symbol="SPY",
            side="buy",
            quantity=5,
            tranche_index=1,
            total_tranches=1,
            status="pending",
            metadata_json={"news_event_id": 1},
            created_at=base_ts.isoformat(),
        )
    )
    event_repo.persist_entry_queue_item(
        EventEntryQueueRecord(
            trade_candidate_id=candidate_id,
            due_at=base_ts.isoformat(),
            symbol="SPY",
            side="buy",
            quantity=2,
            tranche_index=1,
            total_tranches=1,
            status="pending",
            metadata_json={
                "news_event_id": 1,
                "paper_order_style": "passive",
                "paper_limit_price": 99.95,
            },
            created_at=base_ts.isoformat(),
        )
    )
    processed = engine.process_due_entries(stop_loss_pct=0.01, max_hold_seconds=60, logger=_NoopLogger(), now=base_ts + timedelta(seconds=1))
    assert processed == 1
    settings = Settings(
        enable_event_driven_mode=True,
        event_trading_only_mode=True,
        event_execution_enabled=True,
        event_paper_submission_latency_seconds=3.0,
        event_paper_half_spread_bps=12.0,
        event_paper_top_level_size=2,
        event_paper_depth_levels=2,
        event_paper_passive_recheck_seconds=1.0,
    )
    report = PaperTradingReporter(
        reporting_repo,
        tmp_path / "report.json",
        run_manifest=build_event_run_manifest(settings),
    ).write({"event_driven_loop_stats": {"news_items_seen": 1}})
    assert report["run_manifest"]["manifest_schema_version"] == "paper-event-run-manifest-v1"
    assert report["run_manifest"]["execution_model"]["submission_latency_seconds"] == 3.0
    assert report["execution_report"]["by_strategy"]["event_driven"]["orders"] >= 2
    assert report["event_driven_summary"]["legacy_paper_execution_audit_rows"] == 0


class _NoopLogger:
    def info(self, *args, **kwargs) -> None:
        return None
