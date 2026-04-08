from __future__ import annotations

import time
from datetime import datetime, timezone
from decimal import Decimal

from app.allocation.adapters import candidate_from_kalshi_signal
from app.allocation.coordinator import AllocationCoordinator, PendingAllocationAction
from app.allocation.engine import AllocationEngine
from app.allocation.feedback import AllocationFeedbackEngine
from app.allocation.models import PortfolioRiskState
from app.alerts.notifier import Notifier
from app.common.runtime_health import assert_runtime_headroom, run_startup_checks, write_runtime_status
from app.config import Settings
from app.db import Database
from app.execution.router import ExecutionRouter
from app.external_data.fed_futures import ExternalDataResult, FedFuturesProvider
from app.external_data.probability_mapper import map_market_probability
from app.kalshi.rest_client import KalshiRestClient
from app.logging_setup import setup_logging
from app.market_data.macro_fetcher import fetch_macro_prices
from app.market_data.snapshots import market_state_inputs_from_kalshi_market
from app.market_data.service import MarketDataService
from app.models import StrategyName
from app.monotonicity_observation import (
    build_monotonicity_observation,
    followup_monotonicity_signal,
    quote_rejection_events,
    recheck_monotonicity_signal,
    update_monotonicity_observations,
)
from app.portfolio.state import PortfolioState
from app.persistence.kalshi_repo import KalshiRepository
from app.persistence.allocation_repo import AllocationRepository
from app.persistence.market_state_repo import MarketStateRepository
from app.persistence.options_repo import OptionsRepository
from app.persistence.contracts import CrossMarketSnapshotRecord, MonotonicityObservationRecord
from app.persistence.reporting_repo import ReportingRepository
from app.persistence.risk_repo import RiskRepository
from app.reporting import PaperTradingReporter
from app.risk.manager import RiskManager
from app.runtime.health import assess_layer2_trading_readiness
from app.strategies.cross_market import CrossMarketStrategy
from app.strategies.cross_market_snapshot import CrossMarketSnapshotEngine
from app.strategies.monotonicity import MonotonicityStrategy
from app.strategies.partition import PartitionStrategy


def run_layer2_kalshi(settings: Settings) -> None:
    if settings.enable_portfolio_orchestrator:
        from app.portfolio.runtime import run_portfolio_orchestrator

        run_portfolio_orchestrator(settings)
        return
    logger = setup_logging(settings)
    startup_check = run_startup_checks(settings, "layer2_kalshi")
    for warning in startup_check.warnings:
        logger.warning("startup_validation_warning", extra={"event": {"warning": warning}})
    if startup_check.errors:
        write_runtime_status(
            settings,
            "layer2_kalshi",
            "startup_failed",
            message=",".join(startup_check.errors),
            details=startup_check.details,
            startup_check=startup_check,
        )
        raise RuntimeError(f"layer2_kalshi_startup_failed:{','.join(startup_check.errors)}")
    write_runtime_status(
        settings,
        "layer2_kalshi",
        "starting",
        details=startup_check.details,
        startup_check=startup_check,
    )
    db = Database(settings.db_path)
    repo = KalshiRepository(db)
    allocation_repo = AllocationRepository(db)
    reporting_repo = ReportingRepository(db)
    risk_repo = RiskRepository(db)
    market_state_repo = MarketStateRepository(db)
    options_repo = OptionsRepository(db)
    notifier = Notifier(settings)
    client = KalshiRestClient(settings)
    market_data = MarketDataService(client, settings)
    portfolio = PortfolioState(client)
    risk = RiskManager(
        max_daily_loss_pct=settings.max_daily_loss_pct,
        max_total_exposure_pct=settings.max_total_exposure_pct,
        max_position_per_trade_pct=settings.max_position_per_trade_pct,
        max_failed_trades_per_day=settings.max_failed_trades_per_day,
        logger=logger,
        event_recorder=risk_repo.persist_transition,
    )
    router = ExecutionRouter(client, repo, portfolio, risk, logger, settings.live_trading, market_state_repo=market_state_repo)
    allocation_coordinator = AllocationCoordinator(AllocationEngine(), allocation_repo)
    allocation_feedback = AllocationFeedbackEngine(
        allocation_repo=allocation_repo,
        kalshi_repo=repo,
        market_state_repo=market_state_repo,
        options_repo=options_repo,
    )
    monotonicity = MonotonicityStrategy(
        settings.min_edge_bps,
        settings.slippage_buffer_bps,
        settings.max_position_per_trade_pct,
        settings.monotonicity_min_top_level_depth,
        settings.monotonicity_min_size_multiple,
        settings.monotonicity_ladder_cooldown_seconds,
    )
    partition = PartitionStrategy(
        settings.min_edge_bps,
        settings.slippage_buffer_bps,
        settings.max_position_per_trade_pct,
        settings.min_depth,
        settings.partition_min_coverage,
    )
    cross_market = CrossMarketStrategy(
        min_edge=settings.min_edge,
        slippage_buffer=settings.slippage_buffer_bps / 10_000.0,
        min_depth=settings.min_depth,
        min_time_to_expiry_seconds=settings.min_time_to_expiry_seconds,
        max_position_pct=settings.max_position_per_trade_pct,
        directional_allowed=settings.app_mode == "aggressive",
    )
    external_provider = FedFuturesProvider(settings)
    reporter = PaperTradingReporter(reporting_repo, settings.report_path)
    cross_market_snapshotter = CrossMarketSnapshotEngine(settings.cross_market_family_terms)
    last_cross_market_capture_at = 0.0
    run_start_eval_id = reporting_repo.latest_strategy_evaluation_id()
    run_start_signal_id = reporting_repo.latest_signal_id()
    pending_monotonicity_observations: dict[str, MonotonicityObservationRecord] = {}
    logger.info(
        "startup_status",
        extra={
            "event": {
                "live_trading": settings.live_trading,
                "app_mode": settings.app_mode,
                "kalshi_mode": settings.kalshi_mode,
                "external_data": "configured" if settings.fedwatch_url else "missing",
                "timing_experiment_mode": settings.timing_experiment_mode,
                "timing_confirmation_delay_seconds": settings.timing_confirmation_delay_seconds,
                "timing_followup_delay_seconds": settings.timing_followup_delay_seconds,
                "cross_market_mode": settings.enable_cross_market_mode,
            }
        },
    )

    while True:
        try:
            disk_headroom = assert_runtime_headroom(settings)
            reconciliation = portfolio.reconcile(repo)
            repo.persist_snapshot(portfolio.snapshot)
            raw_markets = market_data.refresh_markets()
            filter_result = market_data.filter_markets(raw_markets)
            markets = market_data.hydrate_structural_quotes(filter_result.included)
            for market in markets:
                market_state_repo.persist_many(
                    market_state_inputs_from_kalshi_market(market, market_data.last_update)
                )
            market_lookup = {market.ticker: market for market in filter_result.included}
            update_monotonicity_observations(
                pending_monotonicity_observations,
                market_lookup,
                settings.min_edge_bps,
                settings.slippage_buffer_bps,
                settings.monotonicity_min_top_level_depth,
                repo,
                logger,
            )
            ladders = market_data.build_ladders(markets)
            if settings.app_mode == "safe":
                external = ExternalDataResult(
                    distributions={},
                    stale=True,
                    errors=[],
                    fetched_at=datetime.now(timezone.utc),
                    source="skipped_safe_mode",
                )
            else:
                external = external_provider.fetch()
            ladder_sizes = [len(ladder) for ladder in ladders.values()]
            exclusion_counts: dict[str, int] = {}
            for _, reason, _, _ in filter_result.excluded:
                exclusion_counts[reason] = exclusion_counts.get(reason, 0) + 1
            readiness = assess_layer2_trading_readiness(
                market_data_last_update=market_data.last_update,
                max_market_data_staleness_seconds=settings.max_market_data_staleness_seconds,
                external_stale=external.stale,
                external_errors=external.errors,
                app_mode=settings.app_mode,
                reconciliation_success=reconciliation.success,
                reconciliation_mismatches=reconciliation.mismatches,
                now=datetime.now(timezone.utc),
            )
            logger.info(
                "cycle_status",
                extra={
                    "event": {
                        "discovery_mode_used": market_data.last_discovery_stats.discovery_mode_used,
                        "candidate_series_found": market_data.last_discovery_stats.candidate_series_found,
                        "candidate_events_found": market_data.last_discovery_stats.candidate_events_found,
                        "candidate_markets_found": market_data.last_discovery_stats.candidate_markets_found,
                        "fallback_used": market_data.last_discovery_stats.fallback_used,
                        "markets_loaded": len(markets),
                        "markets_seen": len(raw_markets),
                        "markets_filtered_out": len(filter_result.excluded),
                        "dead_markets_skipped": market_data.last_quote_stats.dead_markets_skipped,
                        "market_filter_reasons": exclusion_counts,
                        "ladders_built": len(ladders),
                        "ladder_size_distribution": ladder_sizes,
                        "external_data_status": "stale" if external.stale else "active",
                        "external_distribution_count": len(external.distributions),
                        "external_data_source": external.source,
                        "quote_completeness_stats": market_data.last_quote_stats.quote_completeness_stats,
                        "kalshi_mode": settings.kalshi_mode,
                        "trading_readiness": readiness.can_trade,
                        "trading_halt_reasons": readiness.halt_reasons,
                    }
                },
            )
            for diagnostic in filter_result.diagnostics[: settings.diagnostic_market_sample_size]:
                logger.info("market_filter_diagnostic", extra={"event": diagnostic})
            loop_stats = {
                "discovery_mode_used": market_data.last_discovery_stats.discovery_mode_used,
                "macro_candidate_series": market_data.last_discovery_stats.candidate_series_found,
                "macro_candidate_events": market_data.last_discovery_stats.candidate_events_found,
                "macro_candidate_markets": market_data.last_discovery_stats.candidate_markets_found,
                "fallback_used": market_data.last_discovery_stats.fallback_used,
                "markets_seen": len(raw_markets),
                "markets_filtered_out": len(filter_result.excluded),
                "markets_loaded": len(markets),
                "dead_markets_skipped": market_data.last_quote_stats.dead_markets_skipped,
                "ladders_evaluated": 0,
                "monotonicity_depth_verified_signals": 0,
                "monotonicity_depth_rejections": 0,
                "monotonicity_signal_time_depths": [],
                "partition_groups_evaluated": 0,
                "invalid_partition_due_to_missing_legs": 0,
                "invalid_partition_due_to_dead_markets": 0,
                "partition_completeness_values": [],
                "external_data_source_used": external.source,
                "quote_completeness_stats": market_data.last_quote_stats.quote_completeness_stats,
                "filter_diagnostics": filter_result.diagnostics[: settings.diagnostic_market_sample_size],
                "run_start_eval_id": run_start_eval_id,
                "run_start_signal_id": run_start_signal_id,
                "timing_experiment_mode": settings.timing_experiment_mode,
                "cross_market_snapshots_collected": 0,
                "cross_market_avg_disagreement_score": 0.0,
                "cross_market_highest_disagreement_events": [],
                "trading_readiness": readiness.can_trade,
                "trading_halt_reasons": readiness.halt_reasons,
            }
            if settings.live_trading:
                for reason in readiness.halt_reasons:
                    risk.kill(reason, readiness.details)
            if settings.enable_cross_market_mode and (time.time() - last_cross_market_capture_at) >= settings.cross_market_poll_interval_seconds:
                try:
                    macro_prices = fetch_macro_prices()
                    cross_market_snapshots, cross_market_stats = cross_market_snapshotter.build_snapshots(markets, macro_prices)
                    for snapshot in cross_market_snapshots:
                        repo.persist_cross_market_snapshot(
                            CrossMarketSnapshotRecord(
                                timestamp=snapshot["timestamp"],
                                event_ticker=snapshot["event_ticker"],
                                contract_pair=snapshot.get("contract_pair"),
                                kalshi_probability=snapshot["kalshi_probability"],
                                kalshi_bid=snapshot["kalshi_bid"],
                                kalshi_ask=snapshot["kalshi_ask"],
                                kalshi_yes_bid_size=snapshot["kalshi_yes_bid_size"],
                                kalshi_yes_ask_size=snapshot["kalshi_yes_ask_size"],
                                spy_price=snapshot.get("spy_price"),
                                qqq_price=snapshot.get("qqq_price"),
                                tlt_price=snapshot.get("tlt_price"),
                                btc_price=snapshot.get("btc_price"),
                                derived_risk_score=snapshot["derived_risk_score"],
                                notes=snapshot.get("notes", ""),
                            )
                        )
                    last_cross_market_capture_at = time.time()
                    loop_stats["cross_market_snapshots_collected"] = cross_market_stats.snapshots_collected
                    loop_stats["cross_market_avg_disagreement_score"] = cross_market_stats.avg_disagreement_score
                    loop_stats["cross_market_highest_disagreement_events"] = cross_market_stats.highest_disagreement_events
                    logger.info(
                        "cross_market_cycle_status",
                        extra={
                            "event": {
                                "snapshots_collected": cross_market_stats.snapshots_collected,
                                "macro_prices": cross_market_stats.macro_prices,
                                "avg_disagreement_score": cross_market_stats.avg_disagreement_score,
                                "highest_disagreement_events": cross_market_stats.highest_disagreement_events,
                            }
                        },
                    )
                except Exception as exc:
                    logger.error("cross_market_snapshot_failed", extra={"event": {"error": str(exc)}})
            if risk.state.killed:
                notifier.send("Arbitrage Machine halted", ",".join(risk.state.reasons or ["unknown"]))
            else:
                pending_allocations: list[PendingAllocationAction] = []
                for ladder in ladders.values():
                    loop_stats["ladders_evaluated"] += 1
                    signals, evaluations = monotonicity.evaluate(ladder, portfolio.snapshot.equity)
                    for evaluation in evaluations:
                        if evaluation.metadata.get("depth_verified"):
                            loop_stats["monotonicity_depth_verified_signals"] += 1
                        if evaluation.reason == "insufficient_signal_time_depth":
                            loop_stats["monotonicity_depth_rejections"] += 1
                        average_depth = evaluation.metadata.get("average_signal_time_depth")
                        if average_depth is not None:
                            loop_stats["monotonicity_signal_time_depths"].append(average_depth)
                        repo.persist_evaluation(evaluation)
                        logger.info("signal_evaluation", extra={"event": {"strategy": evaluation.strategy.value, "ticker": evaluation.ticker, "generated": evaluation.generated, "reason": evaluation.reason, "edge_before_fees": evaluation.edge_before_fees, "edge_after_fees": evaluation.edge_after_fees, "fee_estimate": evaluation.fee_estimate, "metadata": evaluation.metadata}})
                        for event in quote_rejection_events(evaluation, {market.ticker: market for market in ladder}):
                            logger.info("quote_rejection_diagnostic", extra={"event": event})
                    for signal in signals:
                        logger.info("signal_accepted", extra={"event": {"strategy": signal.strategy.value, "ticker": signal.ticker, "edge_after_fees": signal.probability_edge}})
                        if signal.strategy == StrategyName.MONOTONICITY:
                            observation = build_monotonicity_observation(signal)
                            if settings.timing_experiment_mode:
                                immediate_recheck = recheck_monotonicity_signal(
                                    signal,
                                    market_data,
                                    settings.min_edge_bps,
                                    settings.slippage_buffer_bps,
                                    settings.monotonicity_min_top_level_depth,
                                    settings.monotonicity_min_size_multiple,
                                    settings.timing_confirmation_delay_seconds,
                                )
                                if immediate_recheck.get("immediate_recheck_survived"):
                                    followup = followup_monotonicity_signal(
                                        signal,
                                        market_data,
                                        settings.min_edge_bps,
                                        settings.slippage_buffer_bps,
                                        settings.monotonicity_min_top_level_depth,
                                        settings.monotonicity_min_size_multiple,
                                        settings.timing_followup_delay_seconds,
                                    )
                                    immediate_recheck.update(followup)
                                    logger.info("monotonicity_signal_followup_recheck", extra={"event": {"signal_key": observation.signal_key, **followup}})
                                observation.metadata_json.update(immediate_recheck)
                                logger.info("monotonicity_signal_immediate_recheck", extra={"event": {"signal_key": observation.signal_key, **immediate_recheck}})
                            pending_monotonicity_observations[observation.signal_key] = observation
                            repo.persist_monotonicity_observation(observation)
                            logger.info("monotonicity_signal_observed", extra={"event": {
                                "signal_key": observation.signal_key,
                                "ticker": observation.ticker,
                                "lower_ticker": observation.lower_ticker,
                                "higher_ticker": observation.higher_ticker,
                                "product_family": observation.product_family,
                                "accepted_at": observation.accepted_at,
                                "edge_before_fees": observation.edge_before_fees,
                                "edge_after_fees": observation.edge_after_fees,
                                "lower_yes_ask_price": observation.lower_yes_ask_price,
                                "lower_yes_ask_size": observation.lower_yes_ask_size,
                                "higher_yes_bid_price": observation.higher_yes_bid_price,
                                "higher_yes_bid_size": observation.higher_yes_bid_size,
                                "latest_status": observation.latest_status,
                                "latest_observed_at": observation.latest_observed_at,
                                "metadata": observation.metadata_json,
                            }})
                        if not settings.timing_experiment_mode:
                            candidate = candidate_from_kalshi_signal(
                                signal,
                                cvar_95_loss=Decimal(max(50, signal.quantity * 10)),
                                uncertainty_penalty=Decimal("0.25"),
                                liquidity_penalty=Decimal("0.05"),
                                horizon_days=14,
                                correlation_bucket=signal.ticker,
                            )
                            pending_allocations.append(
                                PendingAllocationAction(
                                    candidate=candidate,
                                    execute=lambda decision, signal=signal: router.execute_signal(
                                        signal,
                                        extra_source_metadata={
                                            "allocation_run_id": decision.run_id,
                                            "allocation_candidate_id": decision.candidate_id,
                                        },
                                    ),
                                )
                            )
                by_event_all: dict[str, list] = {}
                for market in filter_result.included:
                    by_event_all.setdefault(market.event_ticker, []).append(market)
                by_event_tradeable: dict[str, list] = {}
                for market in markets:
                    by_event_tradeable.setdefault(market.event_ticker, []).append(market)
                if settings.enable_partition_strategy:
                    for event_ticker, all_group in by_event_all.items():
                        tradeable_group = by_event_tradeable.get(event_ticker, [])
                        if len(all_group) < 2:
                            continue
                        loop_stats["partition_groups_evaluated"] += 1
                        signals, evaluations = partition.evaluate(tradeable_group, portfolio.snapshot.equity, all_group)
                        for evaluation in evaluations:
                            completeness = evaluation.metadata.get("summed_probability_all_legs")
                            if completeness is not None:
                                loop_stats["partition_completeness_values"].append(completeness)
                            if evaluation.reason == "incomplete_partition_coverage":
                                loop_stats["invalid_partition_due_to_missing_legs"] += 1
                            if evaluation.reason == "invalid_partition_due_to_dead_markets":
                                loop_stats["invalid_partition_due_to_dead_markets"] += 1
                            repo.persist_evaluation(evaluation)
                            logger.info("signal_evaluation", extra={"event": {"strategy": evaluation.strategy.value, "ticker": evaluation.ticker, "generated": evaluation.generated, "reason": evaluation.reason, "edge_before_fees": evaluation.edge_before_fees, "edge_after_fees": evaluation.edge_after_fees, "fee_estimate": evaluation.fee_estimate, "metadata": evaluation.metadata}})
                            for event in quote_rejection_events(evaluation, {market.ticker: market for market in all_group}):
                                logger.info("quote_rejection_diagnostic", extra={"event": event})
                        for signal in signals:
                            logger.info("signal_accepted", extra={"event": {"strategy": signal.strategy.value, "ticker": signal.ticker, "edge_after_fees": signal.probability_edge}})
                            if not settings.timing_experiment_mode:
                                candidate = candidate_from_kalshi_signal(
                                    signal,
                                    cvar_95_loss=Decimal(max(50, signal.quantity * 10)),
                                    uncertainty_penalty=Decimal("0.20"),
                                    liquidity_penalty=Decimal("0.04"),
                                    horizon_days=7,
                                    correlation_bucket=signal.ticker,
                                )
                                pending_allocations.append(
                                    PendingAllocationAction(
                                        candidate=candidate,
                                        execute=lambda decision, signal=signal: router.execute_signal(
                                            signal,
                                            extra_source_metadata={
                                                "allocation_run_id": decision.run_id,
                                                "allocation_candidate_id": decision.candidate_id,
                                            },
                                        ),
                                    )
                                )
                if settings.app_mode in {"balanced", "aggressive"} and not external.stale:
                    for market in markets:
                        mapped_probability, mapping_metadata = map_market_probability(market, external.distributions)
                        try:
                            orderbook = market_data.refresh_orderbook(market.ticker)
                        except Exception as exc:
                            logger.error("orderbook_refresh_failed", extra={"event": {"ticker": market.ticker, "error": str(exc)}})
                            continue
                        signals, evaluations = cross_market.evaluate(market, mapped_probability, orderbook, portfolio.snapshot.equity)
                        for evaluation in evaluations:
                            evaluation.metadata.update(mapping_metadata)
                            repo.persist_evaluation(evaluation)
                            logger.info("signal_evaluation", extra={"event": {"strategy": evaluation.strategy.value, "ticker": evaluation.ticker, "generated": evaluation.generated, "reason": evaluation.reason, "edge_before_fees": evaluation.edge_before_fees, "edge_after_fees": evaluation.edge_after_fees, "fee_estimate": evaluation.fee_estimate, "metadata": evaluation.metadata}})
                        for signal in signals:
                            logger.info("signal_accepted", extra={"event": {"strategy": signal.strategy.value, "ticker": signal.ticker, "edge_after_fees": signal.probability_edge}})
                            if not settings.timing_experiment_mode:
                                candidate = candidate_from_kalshi_signal(
                                    signal,
                                    cvar_95_loss=Decimal(max(40, signal.quantity * 8)),
                                    uncertainty_penalty=Decimal("0.35"),
                                    liquidity_penalty=Decimal("0.06"),
                                    horizon_days=21,
                                    correlation_bucket=market.event_ticker,
                                )
                                pending_allocations.append(
                                    PendingAllocationAction(
                                        candidate=candidate,
                                        execute=lambda decision, signal=signal: router.execute_signal(
                                            signal,
                                            extra_source_metadata={
                                                "allocation_run_id": decision.run_id,
                                                "allocation_candidate_id": decision.candidate_id,
                                            },
                                        ),
                                    )
                                )
                if pending_allocations:
                    allocation_coordinator.run(
                        pending=pending_allocations,
                        risk_state=PortfolioRiskState(
                            observed_at=datetime.now(timezone.utc),
                            portfolio_equity=Decimal(str(portfolio.snapshot.equity)),
                            daily_realized_pnl=Decimal(str(portfolio.snapshot.daily_pnl)),
                            current_gross_notional=Decimal(str(sum(position.exposure * 100 for position in portfolio.snapshot.positions.values()))),
                            gross_notional_cap=Decimal(str(portfolio.snapshot.equity)) * Decimal(str(settings.max_total_exposure_pct)),
                            allocated_notional_by_sleeve={},
                            allocated_notional_by_bucket={},
                        ),
                        adjustments=allocation_repo.load_latest_adjustments(),
                    )
                    allocation_feedback.refresh_latest_run()
            reporter.write(loop_stats)
            write_runtime_status(
                settings,
                "layer2_kalshi",
                "running",
                details={
                    "disk_headroom": disk_headroom,
                    "loop_stats": {
                        "markets_seen": loop_stats.get("markets_seen", 0),
                        "markets_loaded": loop_stats.get("markets_loaded", 0),
                        "ladders_evaluated": loop_stats.get("ladders_evaluated", 0),
                    },
                },
                update_heartbeat=True,
            )
        except Exception as exc:
            logger.error("main_loop_error", extra={"event": {"error": str(exc)}})
            write_runtime_status(
                settings,
                "layer2_kalshi",
                "error",
                message=str(exc),
                details={"runtime": "layer2_kalshi"},
            )
            try:
                notifier.send("Arbitrage Machine crash", str(exc))
            except Exception:
                logger.error("notifier_error", extra={"event": {"error": "failed_to_send_crash_notification"}})
        time.sleep(settings.poll_interval_seconds)
