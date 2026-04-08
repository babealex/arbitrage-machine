from __future__ import annotations

import json
import time
from datetime import datetime, timezone

from app.common.runtime_health import assert_runtime_headroom, run_startup_checks, write_runtime_status
from app.classification.service import EventClassifier
from app.config import Settings
from app.db import Database
from app.events.bus import InProcessEventBus
from app.events.models import ClassifiedEvent, ConfirmationResult, NewsItem, TradeIdea
from app.execution.broker import MarketProxyBook, PaperBroker, PriceSnapshot
from app.execution.event_engine import EventExecutionEngine
from app.execution.router import ExecutionRouter
from app.ingestion.newsapi import NewsApiSource
from app.ingestion.rss import RSSNewsSource
from app.ingestion.service import NewsIngestionService
from app.logging.event_logger import classified_event_payload, news_event_payload, trade_idea_payload
from app.logging_setup import setup_logging
from app.mapping.engine import EventTradeMapper
from app.persistence.contracts import ClassifiedEventRecord, NewsEventRecord, TradeCandidateRecord
from app.persistence.event_repo import EventRepository
from app.persistence.kalshi_repo import KalshiRepository
from app.persistence.market_state_repo import MarketStateRepository
from app.persistence.reporting_repo import ReportingRepository
from app.reporting import PaperTradingReporter, build_event_run_manifest
from app.risk.event_risk import EventRiskManager
from app.risk.manager import RiskManager
from app.portfolio.state import PortfolioState


CONFIRMATION_SYMBOLS = ["SPY", "QQQ", "TLT", "BTC-USD", "^VIX", "CL=F", "XLE", "UUP"]


def run_event_driven(settings: Settings) -> None:
    logger = setup_logging(settings)
    startup_check = run_startup_checks(settings, "event_driven")
    for warning in startup_check.warnings:
        logger.warning("startup_validation_warning", extra={"event": {"warning": warning}})
    if startup_check.errors:
        write_runtime_status(
            settings,
            "event_driven",
            "startup_failed",
            message=",".join(startup_check.errors),
            details=startup_check.details,
            startup_check=startup_check,
        )
        raise RuntimeError(f"event_driven_startup_failed:{','.join(startup_check.errors)}")
    write_runtime_status(
        settings,
        "event_driven",
        "starting",
        details=startup_check.details,
        startup_check=startup_check,
    )
    db = Database(settings.db_path)
    repo = EventRepository(db)
    signal_repo = KalshiRepository(db)
    reporting_repo = ReportingRepository(db)
    market_state_repo = MarketStateRepository(db)
    reporter = PaperTradingReporter(reporting_repo, settings.report_path, run_manifest=build_event_run_manifest(settings))
    bus = InProcessEventBus()
    ingestion = NewsIngestionService(
        [
            RSSNewsSource(settings.event_rss_urls),
            NewsApiSource(settings.event_newsapi_key, settings.event_newsapi_url),
        ]
    )
    classifier = EventClassifier(
        enable_openai=settings.enable_openai_classification,
        openai_api_key=settings.openai_api_key,
        openai_model=settings.openai_model,
    )
    mapper = EventTradeMapper()
    proxy_book = MarketProxyBook()
    broker = PaperBroker(
        settings.event_paper_starting_capital,
        settings.event_execution_enabled,
        submission_latency_seconds=settings.event_paper_submission_latency_seconds,
        half_spread_bps=settings.event_paper_half_spread_bps,
        top_level_size=settings.event_paper_top_level_size,
        depth_levels=settings.event_paper_depth_levels,
        passive_recheck_seconds=settings.event_paper_passive_recheck_seconds,
        session_boundary_buffer_seconds=settings.event_paper_session_boundary_buffer_seconds,
        session_boundary_liquidity_factor=settings.event_paper_session_boundary_liquidity_factor,
        session_boundary_spread_multiplier=settings.event_paper_session_boundary_spread_multiplier,
    )
    risk = EventRiskManager(settings.event_paper_starting_capital, settings.event_max_risk_per_trade_pct, settings.event_stop_loss_pct)
    execution_router = ExecutionRouter(
        None,
        signal_repo,
        PortfolioState(None),
        RiskManager(1.0, 10.0, 10.0, 10, logger=logger),
        logger,
        False,
        market_state_repo=market_state_repo,
    )
    execution = EventExecutionEngine(repo, signal_repo, broker, execution_router)
    cycle_stats = {
        "news_items_seen": 0,
        "new_events_persisted": 0,
        "classified_events": 0,
        "trade_candidates": 0,
        "confirmed_trade_candidates": 0,
        "queued_entries": 0,
        "filled_entries": 0,
        "closed_positions": 0,
        "confirmation_failures": 0,
    }

    def handle_news_item(item: NewsItem) -> None:
        if repo.news_event_exists(item.external_id):
            return
        cycle_stats["new_events_persisted"] += 1
        news_event_id = repo.persist_news_event(
            NewsEventRecord(
                external_id=item.external_id,
                timestamp_utc=item.timestamp_utc.isoformat(),
                source=item.source,
                headline=item.headline,
                body=item.body,
                url=item.url,
                metadata_json=item.metadata,
            )
        )
        logger.info("event_news_ingested", extra={"event": news_event_payload(item)})
        classified = classifier.classify(item)
        if classified is None or classified.severity < settings.event_min_severity:
            return
        cycle_stats["classified_events"] += 1
        classified_id = repo.persist_classified_event(
            ClassifiedEventRecord(
                news_event_id=news_event_id,
                event_type=classified.event_type,
                region=classified.region,
                severity=classified.severity,
                directional_bias=classified.directional_bias,
                assets_affected=classified.assets_affected,
                metadata_json={
                    "tags": classified.tags,
                    "rationale": classified.rationale,
                    **classified.metadata,
                },
                created_at=datetime.now(timezone.utc).isoformat(),
            )
        )
        logger.info("event_classified", extra={"event": classified_event_payload(classified)})
        proxy_snapshots = proxy_book.snapshot(CONFIRMATION_SYMBOLS + classified.assets_affected)
        ideas = mapper.map_event(
            classified,
            scale_steps=settings.event_scale_steps,
            scale_interval_seconds=settings.event_scale_interval_seconds,
            confirmation_needed=settings.event_confirmation_min_agreeing_signals,
        )
        for idea in ideas:
            cycle_stats["trade_candidates"] += 1
            confirmation = confirm_trade(classified, idea, proxy_snapshots)
            status = "confirmed" if confirmation.passed else "filtered"
            candidate_id = repo.persist_trade_candidate(
                TradeCandidateRecord(
                    classified_event_id=classified_id,
                    symbol=idea.symbol,
                    side=idea.side,
                    event_type=idea.event_type,
                    status=status,
                    confidence=idea.confidence,
                    confirmation_score=confirmation.agreeing_signals / max(1, confirmation.checked_signals),
                    metadata_json={
                        "trade_idea": trade_idea_payload(idea),
                        "confirmation": {
                            "passed": confirmation.passed,
                            "agreeing_signals": confirmation.agreeing_signals,
                            "checked_signals": confirmation.checked_signals,
                            "details": confirmation.details,
                        },
                        "news_event_id": news_event_id,
                    },
                    created_at=datetime.now(timezone.utc).isoformat(),
                )
            )
            if not confirmation.passed:
                cycle_stats["confirmation_failures"] += 1
                logger.info("trade_candidate_rejected", extra={"event": {"symbol": idea.symbol, "side": idea.side, "confirmation": confirmation.details}})
                continue
            cycle_stats["confirmed_trade_candidates"] += 1
            symbol_quote = proxy_book.quote(idea.symbol)
            quantity = risk.size_for_price(symbol_quote.price)
            if quantity <= 0:
                logger.info("trade_candidate_unpriced", extra={"event": {"symbol": idea.symbol, "side": idea.side}})
                continue
            execution.queue_trade(candidate_id, news_event_id, idea, quantity)
            cycle_stats["queued_entries"] += max(1, idea.scale_steps)
            logger.info("trade_candidate_queued", extra={"event": {"symbol": idea.symbol, "side": idea.side, "quantity": quantity, "scale_steps": idea.scale_steps}})

    bus.subscribe("news.item", handle_news_item)

    logger.info(
        "event_driven_startup_status",
        extra={
            "event": {
                "execution_enabled": settings.event_execution_enabled,
                "paper_capital": settings.event_paper_starting_capital,
                "rss_urls": settings.event_rss_urls,
                "newsapi_enabled": bool(settings.event_newsapi_key),
                "openai_enabled": settings.enable_openai_classification and bool(settings.openai_api_key),
            }
        },
    )

    while True:
        try:
            disk_headroom = assert_runtime_headroom(settings)
            for key in list(cycle_stats):
                cycle_stats[key] = 0
            items = ingestion.poll()
            cycle_stats["news_items_seen"] = len(items)
            for item in items:
                bus.publish("news.item", item)
            cycle_stats["filled_entries"] = execution.process_due_entries(
                stop_loss_pct=settings.event_stop_loss_pct,
                max_hold_seconds=settings.event_max_hold_seconds,
                logger=logger,
            )
            cycle_stats["closed_positions"] = execution.close_due_positions(logger)
            reporter.write({"event_driven_loop_stats": cycle_stats})
            write_runtime_status(
                settings,
                "event_driven",
                "running",
                details={"disk_headroom": disk_headroom, "cycle_stats": cycle_stats},
                update_heartbeat=True,
            )
            logger.info("event_driven_cycle_status", extra={"event": cycle_stats})
        except Exception as exc:
            logger.error("event_driven_cycle_error", extra={"event": {"error": str(exc)}})
            write_runtime_status(
                settings,
                "event_driven",
                "error",
                message=str(exc),
                details={"runtime": "event_driven"},
            )
        time.sleep(settings.event_poll_interval_seconds)


def confirm_trade(event: ClassifiedEvent, idea: TradeIdea, proxy_snapshots: dict[str, PriceSnapshot]) -> ConfirmationResult:
    checks: list[dict] = []
    agreeing = 0
    symbol_set = set(proxy_snapshots)

    def add_check(name: str, condition: bool | None) -> None:
        nonlocal agreeing
        if condition is None:
            return
        if condition:
            agreeing += 1
        checks.append({"name": name, "passed": condition})

    spy = proxy_snapshots.get("SPY")
    qqq = proxy_snapshots.get("QQQ")
    tlt = proxy_snapshots.get("TLT")
    vix = proxy_snapshots.get("^VIX")
    oil = proxy_snapshots.get("CL=F")
    xle = proxy_snapshots.get("XLE")
    uup = proxy_snapshots.get("UUP")

    if event.event_type == "geopolitical" and event.region == "Middle East":
        add_check("oil_up", _pct_positive(oil))
        add_check("energy_up", _pct_positive(xle))
        add_check("vix_up", _pct_positive(vix))
    elif "inflation_hot" in event.tags or "hawkish_rates" in event.tags:
        add_check("spy_down", _pct_negative(spy))
        add_check("qqq_down", _pct_negative(qqq))
        add_check("tlt_down", _pct_negative(tlt))
        add_check("uup_up", _pct_positive(uup))
    elif "inflation_cool" in event.tags or "dovish_rates" in event.tags:
        add_check("spy_up", _pct_positive(spy))
        add_check("qqq_up", _pct_positive(qqq))
        add_check("tlt_up", _pct_positive(tlt))
        add_check("vix_down", _pct_negative(vix))
    else:
        if idea.side == "buy":
            add_check("symbol_up", _pct_positive(proxy_snapshots.get(idea.symbol)))
            add_check("spy_or_qqq_up", _either_positive(spy, qqq))
        else:
            add_check("symbol_down", _pct_negative(proxy_snapshots.get(idea.symbol)))
            add_check("spy_or_qqq_down", _either_negative(spy, qqq))

    passed = agreeing >= idea.confirmation_needed
    return ConfirmationResult(
        symbol=idea.symbol,
        passed=passed,
        agreeing_signals=agreeing,
        checked_signals=len(checks),
        details=checks,
        metadata={"available_symbols": sorted(symbol_set)},
    )


def _pct_positive(snapshot: PriceSnapshot | None) -> bool | None:
    if snapshot is None or snapshot.pct_change is None:
        return None
    return snapshot.pct_change > 0


def _pct_negative(snapshot: PriceSnapshot | None) -> bool | None:
    if snapshot is None or snapshot.pct_change is None:
        return None
    return snapshot.pct_change < 0


def _either_positive(left: PriceSnapshot | None, right: PriceSnapshot | None) -> bool | None:
    values = [item.pct_change for item in (left, right) if item and item.pct_change is not None]
    if not values:
        return None
    return any(value > 0 for value in values)


def _either_negative(left: PriceSnapshot | None, right: PriceSnapshot | None) -> bool | None:
    values = [item.pct_change for item in (left, right) if item and item.pct_change is not None]
    if not values:
        return None
    return any(value < 0 for value in values)
