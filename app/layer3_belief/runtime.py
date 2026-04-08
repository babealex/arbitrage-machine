from __future__ import annotations

import time
from datetime import datetime, timezone

from app.common.runtime_health import assert_runtime_headroom, run_startup_checks, write_runtime_status
from app.config import Settings
from app.db import Database
from app.external_data.fed_futures import FedFuturesProvider
from app.kalshi.rest_client import KalshiRestClient
from app.logging_setup import setup_logging
from app.market_data.service import MarketDataService
from app.persistence.contracts import BeliefSnapshotRecord, BeliefSourceRecord, CrossMarketDisagreementRecord, DisagreementSnapshotRecord, EventDefinitionRecord, OutcomeRealizationRecord, OutcomeTrackingRecord, OutcomeTrackingUpdate
from app.persistence.belief_repo import BeliefRepository
from app.persistence.options_repo import OptionsRepository
from app.persistence.reporting_repo import ReportingRepository
from app.reporting import PaperTradingReporter
from app.research.belief_objects import build_kalshi_belief_snapshots
from app.research.cross_market_ingest import ExternalProxyIngestor
from app.research.disagreement_engine import DisagreementEngine
from app.research.normalization import fed_futures_beliefs, options_state_belief, proxy_belief
from app.research.outcome_tracker import OutcomeTracker


def run_layer3_belief(settings: Settings) -> None:
    logger = setup_logging(settings)
    startup_check = run_startup_checks(settings, "belief_layer")
    for warning in startup_check.warnings:
        logger.warning("startup_validation_warning", extra={"event": {"warning": warning}})
    if startup_check.errors:
        write_runtime_status(
            settings,
            "belief_layer",
            "startup_failed",
            message=",".join(startup_check.errors),
            details=startup_check.details,
            startup_check=startup_check,
        )
        raise RuntimeError(f"belief_layer_startup_failed:{','.join(startup_check.errors)}")
    write_runtime_status(
        settings,
        "belief_layer",
        "starting",
        details=startup_check.details,
        startup_check=startup_check,
    )
    db = Database(settings.db_path)
    repo = BeliefRepository(db)
    options_repo = OptionsRepository(db)
    reporting_repo = ReportingRepository(db)
    client = KalshiRestClient(settings)
    market_data = MarketDataService(client, settings)
    reporter = PaperTradingReporter(reporting_repo, settings.report_path)
    proxy_ingestor = ExternalProxyIngestor()
    fed_provider = FedFuturesProvider(settings)
    disagreement_engine = DisagreementEngine(settings.disagreement_threshold)
    outcome_tracker = OutcomeTracker()
    for source in (
        BeliefSourceRecord(source_key="kalshi_event_market", source_type="kalshi", venue="kalshi", instrument_class="event_contract", description="Prediction-market implied probability from tradeable event markets."),
        BeliefSourceRecord(source_key="fed_futures", source_type="fed_futures", venue="cme", instrument_class="rates", description="Futures-implied rate or policy path summaries."),
        BeliefSourceRecord(source_key="options_surface", source_type="options", venue="options", instrument_class="options", description="Options-implied state summaries from persisted surface features."),
        BeliefSourceRecord(source_key="equity_proxy", source_type="equity_proxy", venue="proxy", instrument_class="cross_asset_proxy", description="Structured proxy state used only for belief context and disagreement research."),
        BeliefSourceRecord(source_key="crypto_proxy", source_type="crypto_proxy", venue="proxy", instrument_class="cross_asset_proxy", description="Structured proxy state used only for belief context and disagreement research."),
        BeliefSourceRecord(source_key="fedwatch_proxy", source_type="fedwatch_proxy", venue="proxy", instrument_class="rates", description="Placeholder-only fallback proxy; never treated as trade oracle."),
        BeliefSourceRecord(source_key="tips_proxy", source_type="tips_proxy", venue="proxy", instrument_class="rates", description="Placeholder-only inflation proxy; never treated as trade oracle."),
    ):
        repo.persist_belief_source(source)
    logger.info(
        "belief_layer_startup_status",
        extra={
            "event": {
                "belief_layer_only_mode": settings.belief_layer_only_mode,
                "belief_families": settings.belief_family_terms,
                "belief_poll_interval_seconds": settings.belief_poll_interval_seconds,
                "enable_external_proxy_fetch": settings.enable_external_proxy_fetch,
                "kalshi_mode": settings.kalshi_mode,
            }
        },
    )
    while True:
        try:
            disk_headroom = assert_runtime_headroom(settings)
            due_rows = [dict(row) for row in repo.due_outcomes(datetime.now(timezone.utc).isoformat())]
            due_updates = outcome_tracker.process_due_rows(
                due_rows,
                lambda ticker: market_data.refresh_market_observation(ticker),
            )
            due_success_counts = {30: 0, 60: 0, 300: 0, 900: 0}
            due_error_counts = {30: 0, 60: 0, 300: 0, 900: 0}
            for update in due_updates:
                repo.update_outcome_tracking(update.row_id, OutcomeTrackingUpdate(**update.outcome))
                metadata = update.outcome.get("metadata_json", {})
                event_key = str(metadata.get("event_key") or metadata.get("event_group") or update.outcome.get("event_ticker"))
                normalized_underlying_key = str(metadata.get("normalized_underlying_key") or update.outcome.get("family", "unknown"))
                for horizon in update.due_horizons:
                    realized_value = update.outcome.get(f"value_after_{horizon}s")
                    if realized_value is None:
                        continue
                    repo.persist_outcome_realization(
                        OutcomeRealizationRecord(
                            timestamp_utc=datetime.now(timezone.utc).isoformat(),
                            event_key=event_key,
                            normalized_underlying_key=normalized_underlying_key,
                            outcome_type=f"belief_markout_{horizon}s",
                            realized_value=float(realized_value),
                            metadata_json={
                                "initial_value": update.outcome.get("initial_value"),
                                "disagreement_score": update.outcome.get("disagreement_score"),
                                "resolved_direction": update.outcome.get("resolved_direction"),
                            },
                        )
                    )
                scheduler = update.outcome.get("metadata_json", {}).get("scheduler", {})
                for horizon in update.due_horizons:
                    horizon_tracker = scheduler.get(str(horizon), {})
                    if update.outcome.get(f"value_after_{horizon}s") is not None:
                        due_success_counts[horizon] += 1
                    elif horizon_tracker.get("last_error"):
                        due_error_counts[horizon] += 1
            raw_markets = market_data.refresh_markets()
            filter_result = market_data.filter_markets(raw_markets)
            markets = market_data.hydrate_structural_quotes(filter_result.included)
            belief_rows = build_kalshi_belief_snapshots(markets, settings.belief_family_terms)
            external_snapshot = proxy_ingestor.fetch(enabled=settings.enable_external_proxy_fetch)
            fed_result = fed_provider.fetch()
            fed_rows = [item.to_snapshot_row() for item in fed_futures_beliefs(fed_result.distributions)]
            proxy_rows = [proxy_belief(row).to_snapshot_row() for row in external_snapshot.rows]
            options_rows = []
            for underlying in options_repo.load_latest_chain_underlyings():
                snapshot = options_repo.load_latest_chain_snapshot(underlying=underlying)
                if snapshot is None:
                    continue
                state_vector = options_repo.load_state_vector(underlying=underlying, observed_at=snapshot.observed_at)
                if state_vector is None:
                    continue
                options_rows.append(options_state_belief(state_vector).to_snapshot_row())
            external_rows = [*fed_rows, *proxy_rows, *options_rows]
            for row in belief_rows:
                repo.persist_belief_snapshot(BeliefSnapshotRecord(**row))
                repo.persist_event_definition(
                    EventDefinitionRecord(
                        event_key=str(row.get("event_key") or row["event_ticker"]),
                        family=str(row["family"]),
                        normalized_underlying_key=str(row.get("normalized_underlying_key") or row["family"]),
                        title=str(row["market_ticker"]),
                        metadata_json={"source_key": row.get("source_key")},
                    )
                )
            for row in external_rows:
                repo.persist_belief_snapshot(BeliefSnapshotRecord(**row))
                repo.persist_event_definition(
                    EventDefinitionRecord(
                        event_key=str(row.get("event_key") or row["event_ticker"]),
                        family=str(row["family"]),
                        normalized_underlying_key=str(row.get("normalized_underlying_key") or row["family"]),
                        title=str(row["market_ticker"]),
                        metadata_json={"source_key": row.get("source_key")},
                    )
                )
            disagreement_result = disagreement_engine.build(belief_rows, external_rows)
            for row in disagreement_result.rows:
                repo.persist_disagreement_snapshot(DisagreementSnapshotRecord(**row))
                repo.persist_cross_market_disagreement(
                    CrossMarketDisagreementRecord(
                        timestamp_utc=row["timestamp_utc"],
                        normalized_underlying_key=str(row["normalized_underlying_key"]),
                        event_key=str(row["metadata_json"].get("event_key") or row["event_group"]),
                        comparison_metric=str(row["metadata_json"].get("comparison_metric", "unknown")),
                        left_source_key=str(row["left_source_key"]),
                        right_source_key=str(row["right_source_key"]),
                        left_value=float(row["metadata_json"].get("left_value", 0.0)),
                        right_value=float(row["metadata_json"].get("right_value", 0.0)),
                        gross_disagreement=float(row["gross_disagreement"]),
                        fees_bps=float(row["metadata_json"].get("fees_bps", 0.0)),
                        slippage_bps=float(row["metadata_json"].get("slippage_bps", 0.0)),
                        execution_uncertainty_bps=float(row["metadata_json"].get("execution_uncertainty_bps", 0.0)),
                        net_disagreement=float(row["net_disagreement"]),
                        tradeable=bool(row["tradeable"]),
                        metadata_json=dict(row["metadata_json"]),
                    )
                )
            for outcome in outcome_tracker.build_outcomes(disagreement_result.rows):
                repo.persist_outcome_tracking(OutcomeTrackingRecord(**outcome))
            outstanding_rows = repo.due_outcomes(datetime.now(timezone.utc).isoformat())
            logger.info(
                "belief_layer_cycle_status",
                extra={
                    "event": {
                        "markets_seen": len(raw_markets),
                        "markets_loaded": len(markets),
                        "belief_rows_collected": len(belief_rows),
                        "external_rows_collected": len(external_rows),
                        "fed_rows_collected": len(fed_rows),
                        "options_rows_collected": len(options_rows),
                        "disagreement_rows_collected": len(disagreement_result.rows),
                        "outcomes_pending": len(outstanding_rows),
                        "outcome_due_success_counts": due_success_counts,
                        "outcome_due_error_counts": due_error_counts,
                        "external_fetch_errors": external_snapshot.fetch_errors + fed_result.errors,
                    }
                },
            )
            reporter.write(
                {
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
                    "partition_groups_evaluated": 0,
                    "external_data_source_used": "belief_layer",
                    "quote_completeness_stats": market_data.last_quote_stats.quote_completeness_stats,
                    "filter_diagnostics": filter_result.diagnostics[: settings.diagnostic_market_sample_size],
                    "run_start_eval_id": reporting_repo.latest_strategy_evaluation_id(),
                    "run_start_signal_id": reporting_repo.latest_signal_id(),
                }
            )
            write_runtime_status(
                settings,
                "belief_layer",
                "running",
                details={
                    "disk_headroom": disk_headroom,
                    "cycle": {
                        "markets_seen": len(raw_markets),
                        "markets_loaded": len(markets),
                        "belief_rows_collected": len(belief_rows),
                        "disagreement_rows_collected": len(disagreement_result.rows),
                    },
                },
                update_heartbeat=True,
            )
        except Exception as exc:
            logger.error("belief_layer_cycle_error", extra={"event": {"error": str(exc)}})
            write_runtime_status(
                settings,
                "belief_layer",
                "error",
                message=str(exc),
                details={"runtime": "belief_layer"},
            )
        time.sleep(settings.belief_poll_interval_seconds)
