from __future__ import annotations

import json
from collections import defaultdict

from app.db import Database
from app.allocation.reporting import build_allocation_report
from app.persistence.allocation_repo import AllocationRepository
from app.persistence.reporting_repo import ReportingRepository
from app.persistence.kalshi_repo import KalshiRepository
from app.persistence.market_state_repo import MarketStateRepository
from app.persistence.portfolio_cycle_repo import PortfolioCycleRepository
from app.reporting_belief import belief_layer_summary, outcome_tracking_summary
from app.persistence.convexity_repo import ConvexityRepository
from app.strategies.convexity.analysis import compare_expected_vs_realized, summarize_convexity_outcomes
from app.reporting_execution import build_execution_report
from app.persistence.trend_broker_repo import TrendBrokerRepository
from app.reporting_event_driven import build_event_run_manifest, event_driven_summary, merge_run_manifest
from app.reporting_kalshi import (
    avg,
    cross_market_summary,
    monotonicity_confirmed_signal_summary,
    monotonicity_filter_summary,
    monotonicity_immediate_recheck_summary,
    monotonicity_observation_summary,
    rejection_examples,
)


class PaperTradingReporter:
    def __init__(self, repo_or_db: ReportingRepository | Database, output_path, run_manifest: dict | None = None) -> None:
        if isinstance(repo_or_db, ReportingRepository):
            self.repo = repo_or_db
        else:
            self.repo = ReportingRepository(repo_or_db)
        self.output_path = output_path
        self.run_manifest = run_manifest or {}

    def write(self, loop_stats: dict | None = None) -> dict:
        kalshi_bundle = self.repo.load_kalshi_reporting_bundle()
        belief_bundle = self.repo.load_belief_reporting_bundle()
        event_bundle = self.repo.load_event_reporting_bundle()
        kalshi_repo = KalshiRepository(self.repo.db)
        allocation_repo = AllocationRepository(self.repo.db)
        market_state_repo = MarketStateRepository(self.repo.db)
        portfolio_cycle_repo = PortfolioCycleRepository(self.repo.db)
        trend_broker_repo = TrendBrokerRepository(self.repo.db)
        convexity_repo = ConvexityRepository(self.repo.db)
        latest_portfolio_cycle = portfolio_cycle_repo.load_latest_snapshot()
        latest_allocation_run = allocation_repo.load_latest_run()
        settings = getattr(self.repo, "settings", None)
        allocation_decisions = [] if latest_allocation_run is None else allocation_repo.load_decisions(latest_allocation_run.run_id)
        allocation_outcomes = [] if latest_allocation_run is None else allocation_repo.load_outcomes_for_run(latest_allocation_run.run_id)
        allocation_adjustments = allocation_repo.load_latest_adjustments()
        convexity_lookback_days = 90 if settings is None else settings.convexity_lookback_days_summary
        convexity_candidates = convexity_repo.load_recent_candidate_history(lookback_days=convexity_lookback_days)
        convexity_outcomes = convexity_repo.load_recent_outcomes(lookback_days=max(180, convexity_lookback_days))
        convexity_latest_summary = convexity_repo.load_latest_run_summary()
        convexity_historical = summarize_convexity_outcomes(convexity_outcomes)
        convexity_calibration = compare_expected_vs_realized(convexity_candidates, convexity_outcomes)
        evaluations = kalshi_bundle.evaluations
        signals = kalshi_bundle.signals
        orders = kalshi_bundle.orders
        run_start_id = int((loop_stats or {}).get("run_start_eval_id", 0))
        run_evaluations = [row for row in evaluations if row["id"] > run_start_id]
        run_signals = [row for row in signals if row["id"] > int((loop_stats or {}).get("run_start_signal_id", 0))]
        accepted = sum(1 for row in evaluations if row["generated"])
        rejected = sum(1 for row in evaluations if not row["generated"])
        run_accepted = sum(1 for row in run_evaluations if row["generated"])
        run_rejected = sum(1 for row in run_evaluations if not row["generated"])
        simulated_pnl = sum(float(row["expected_edge_bps"]) / 10_000.0 * int(row["quantity"]) for row in signals)
        run_simulated_pnl = sum(float(row["expected_edge_bps"]) / 10_000.0 * int(row["quantity"]) for row in run_signals)
        execution_report = build_execution_report(
            signals=kalshi_repo.load_signals(),
            order_intents=kalshi_repo.load_order_intents(),
            execution_outcomes=kalshi_repo.load_execution_outcomes(),
            market_state_inputs=market_state_repo.load_market_state_inputs(),
        )
        rejection_counts = defaultdict(int)
        run_rejection_counts = defaultdict(int)
        for row in evaluations:
            if not row["generated"]:
                rejection_counts[row["reason"]] += 1
        for row in run_evaluations:
            if not row["generated"]:
                run_rejection_counts[row["reason"]] += 1
        report = {
            "run_manifest": merge_run_manifest(self.run_manifest),
            "this_run": {
                "total_signals": len(run_evaluations),
                "accepted_signals": run_accepted,
                "rejected_signals": run_rejected,
                "avg_edge_before_fees": avg(row["edge_before_fees"] for row in run_evaluations),
                "avg_edge_after_fees": avg(row["edge_after_fees"] for row in run_evaluations),
                "simulated_pnl": run_simulated_pnl,
                "rejection_counts_by_reason": dict(run_rejection_counts),
                "rejection_examples_by_reason": rejection_examples(run_evaluations),
            },
            "cumulative": {
                "total_signals": len(evaluations),
                "accepted_signals": accepted,
                "rejected_signals": rejected,
                "avg_edge_before_fees": avg(row["edge_before_fees"] for row in evaluations),
                "avg_edge_after_fees": avg(row["edge_after_fees"] for row in evaluations),
                "simulated_pnl": simulated_pnl,
                "rejection_counts_by_reason": dict(rejection_counts),
                "rejection_examples_by_reason": rejection_examples(evaluations),
            },
            "discovery_mode_used": (loop_stats or {}).get("discovery_mode_used", "unknown"),
            "macro_candidate_series": (loop_stats or {}).get("macro_candidate_series", 0),
            "macro_candidate_events": (loop_stats or {}).get("macro_candidate_events", 0),
            "macro_candidate_markets": (loop_stats or {}).get("macro_candidate_markets", 0),
            "included_markets_after_filter": (loop_stats or {}).get("markets_loaded", 0),
            "fallback_used": (loop_stats or {}).get("fallback_used", False),
            "markets_seen": (loop_stats or {}).get("markets_seen", 0),
            "markets_filtered_out": (loop_stats or {}).get("markets_filtered_out", 0),
            "markets_loaded": (loop_stats or {}).get("markets_loaded", 0),
            "dead_markets_skipped": (loop_stats or {}).get("dead_markets_skipped", 0),
            "ladders_evaluated": (loop_stats or {}).get("ladders_evaluated", 0),
            "monotonicity_depth_verified_signals": (loop_stats or {}).get("monotonicity_depth_verified_signals", 0),
            "monotonicity_depth_rejections": (loop_stats or {}).get("monotonicity_depth_rejections", 0),
            "average_signal_time_depth": avg((loop_stats or {}).get("monotonicity_signal_time_depths", [])),
            "partition_groups_evaluated": (loop_stats or {}).get("partition_groups_evaluated", 0),
            "invalid_partition_due_to_missing_legs": (loop_stats or {}).get("invalid_partition_due_to_missing_legs", 0),
            "invalid_partition_due_to_dead_markets": (loop_stats or {}).get("invalid_partition_due_to_dead_markets", 0),
            "average_partition_completeness": avg((loop_stats or {}).get("partition_completeness_values", [])),
            "external_data_source_used": (loop_stats or {}).get("external_data_source_used", "unknown"),
            "quote_completeness_stats": (loop_stats or {}).get("quote_completeness_stats", {}),
            "filter_diagnostics": (loop_stats or {}).get("filter_diagnostics", []),
            "strategy_estimated_edge": self._strategy_estimated_edge(signals),
            "execution_report": _execution_report_payload(execution_report),
            "execution_truth": execution_report.summary,
            "allocation_summary": build_allocation_report(
                run=latest_allocation_run,
                decisions=allocation_decisions,
                outcomes=allocation_outcomes,
                adjustments=allocation_adjustments,
            ),
            "portfolio_runtime_readiness": (loop_stats or {}).get("portfolio_runtime_readiness"),
            "convexity_runtime_diagnostics": (loop_stats or {}).get("convexity_runtime_diagnostics"),
            "deployment_readiness": (loop_stats or {}).get("deployment_readiness"),
            "trend_broker_live_status": (loop_stats or {}).get("trend_broker_live_status", {
                "broker": "ibkr",
                "configured": False,
                "readiness": False,
                "reasons": [] if trend_broker_repo.load_latest_readiness_check() is None else trend_broker_repo.load_latest_readiness_check().reasons,
                "last_auth_ok_at": None if trend_broker_repo.load_latest_readiness_check() is None or trend_broker_repo.load_latest_readiness_check().last_successful_auth_at is None else trend_broker_repo.load_latest_readiness_check().last_successful_auth_at.isoformat(),
                "last_reconciliation_ok_at": None if trend_broker_repo.load_latest_readiness_check() is None or trend_broker_repo.load_latest_readiness_check().last_reconciliation_check_at is None else trend_broker_repo.load_latest_readiness_check().last_reconciliation_check_at.isoformat(),
                "last_order_reject_at": trend_broker_repo.latest_reject_timestamp(),
                "reject_count": trend_broker_repo.count_rejects(),
                "reconnect_count": trend_broker_repo.count_reconnect_events(),
                "reconciliation_mismatch_count": trend_broker_repo.count_reconciliation_mismatches(),
            }),
            "portfolio_cycle_summary": None if latest_portfolio_cycle is None else {
                "run_id": latest_portfolio_cycle.run_id,
                "candidate_count": latest_portfolio_cycle.candidate_count,
                "approved_count": latest_portfolio_cycle.approved_count,
                "rejected_count": latest_portfolio_cycle.rejected_count,
                "allocated_capital": latest_portfolio_cycle.allocated_capital_decimal,
                "expected_portfolio_ev": latest_portfolio_cycle.expected_portfolio_ev_decimal,
                "strategy_counts": latest_portfolio_cycle.strategy_counts_json,
                "rejection_counts": latest_portfolio_cycle.rejection_counts_json,
                "readiness": latest_portfolio_cycle.readiness_json,
                "convexity_runtime_diagnostics": latest_portfolio_cycle.readiness_json.get("convexity_runtime_diagnostics", {}),
            },
            "convexity_engine_summary": _convexity_report_payload(
                latest_summary=convexity_latest_summary,
                historical=convexity_historical,
                calibration=convexity_calibration,
                enabled=True if settings is None else settings.convexity_enabled and settings.convexity_reporting_enabled,
                live_execution_enabled=False,
            ),
            "monotonicity_observation_summary": monotonicity_observation_summary(kalshi_bundle.observations),
            "monotonicity_filter_summary": monotonicity_filter_summary(evaluations),
            "monotonicity_immediate_recheck_summary": monotonicity_immediate_recheck_summary(kalshi_bundle.observations),
            "monotonicity_confirmed_signal_summary": monotonicity_confirmed_signal_summary(kalshi_bundle.observations),
            "cross_market_summary": cross_market_summary(kalshi_bundle.cross_market_snapshots),
            "belief_layer_summary": belief_layer_summary(
                belief_bundle.belief_snapshots,
                belief_bundle.disagreement_snapshots,
                belief_bundle.belief_sources,
                belief_bundle.event_definitions,
                belief_bundle.cross_market_disagreements,
            ),
            "outcome_tracking_summary": outcome_tracking_summary(
                belief_bundle.outcome_tracking,
                belief_bundle.outcome_realizations,
            ),
            "event_driven_summary": event_driven_summary(
                event_bundle.news_events,
                event_bundle.classified_events,
                event_bundle.trade_candidates,
                event_bundle.entry_queue,
                event_bundle.positions,
                event_bundle.trade_outcomes,
                event_bundle.paper_execution_audit,
                (loop_stats or {}).get("event_driven_loop_stats", {}),
            ),
            "fill_assumptions": {
                "paper_mode": "event-driven paper execution uses conservative top-of-book mechanics with bid/ask fills, latency, depth limits, partial fills, and passive non-fill rules; Kalshi paper summaries remain strategy-level simulations",
                "live_mode": "uses exchange reconciliation when available",
            },
        }
        self.output_path.write_text(json.dumps(report, indent=2), encoding="utf-8")
        return report

    def _strategy_estimated_edge(self, signals) -> dict:
        pnl = defaultdict(lambda: {"estimated_pnl": 0.0, "signal_count": 0})
        for row in signals:
            strategy = row["strategy"]
            estimated = float(row["expected_edge_bps"]) / 10_000.0 * int(row["quantity"])
            pnl[strategy]["estimated_pnl"] += estimated
            pnl[strategy]["signal_count"] += 1
        return dict(pnl)


def _execution_report_payload(report) -> dict:
    return {
        "summary": report.summary,
        "by_strategy": report.by_strategy,
        "by_venue": report.by_venue,
    }


def _convexity_report_payload(*, latest_summary, historical: dict, calibration: dict, enabled: bool, live_execution_enabled: bool) -> dict:
    if latest_summary is None:
        return {
            "enabled": enabled,
            "live_execution_enabled": live_execution_enabled,
            "scanned_opportunities": 0,
            "candidate_trades": 0,
            "accepted_trades": 0,
            "rejected_for_liquidity": 0,
            "rejected_for_negative_ev": 0,
            "rejected_for_risk_budget": 0,
            "open_convexity_risk": "0",
            "avg_reward_to_risk": "0",
            "avg_expected_value_after_costs": "0",
            "selection_yield": "0",
            "runtime_inputs": {},
            "historical": historical,
            "calibration": calibration,
        }
    metadata = latest_summary.get("metadata_json")
    if isinstance(metadata, str):
        try:
            metadata = json.loads(metadata)
        except json.JSONDecodeError:
            metadata = {}
    metadata = metadata or {}
    scanned_opportunities = int(latest_summary["scanned_opportunities"])
    candidate_trades = int(latest_summary["candidate_trades"])
    accepted_trades = int(latest_summary["accepted_trades"])
    return {
        "enabled": enabled,
        "live_execution_enabled": live_execution_enabled,
        "scanned_opportunities": scanned_opportunities,
        "candidate_trades": candidate_trades,
        "accepted_trades": accepted_trades,
        "rejected_for_liquidity": int(latest_summary["rejected_for_liquidity"]),
        "rejected_for_negative_ev": int(latest_summary["rejected_for_negative_ev"]),
        "rejected_for_risk_budget": int(latest_summary["rejected_for_risk_budget"]),
        "rejected_other": int(latest_summary["rejected_other"]),
        "open_convexity_risk": str(latest_summary["open_convexity_risk_decimal"]),
        "avg_reward_to_risk": str(latest_summary["avg_reward_to_risk_decimal"]),
        "avg_expected_value_after_costs": str(latest_summary["avg_expected_value_after_costs_decimal"]),
        "selection_yield": "0" if candidate_trades <= 0 else format(accepted_trades / candidate_trades, ".6f"),
        "runtime_inputs": {
            "input_underlyings": int(metadata.get("input_underlyings", 0)),
            "configured_macro_events": int(metadata.get("configured_macro_events", 0)),
            "configured_earnings_events": int(metadata.get("configured_earnings_events", 0)),
            "recent_event_bias_rows": int(metadata.get("recent_event_bias_rows", 0)),
            "calendar_symbols_with_events": int(metadata.get("calendar_symbols_with_events", 0)),
            "news_symbols_with_bias": int(metadata.get("news_symbols_with_bias", 0)),
        },
        "historical": historical,
        "calibration": calibration,
    }
