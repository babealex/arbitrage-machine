from __future__ import annotations

import json
from collections import defaultdict

from app.db import Database
from app.persistence.reporting_repo import ReportingRepository
from app.reporting_belief import belief_layer_summary, outcome_tracking_summary
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
            "estimated_vs_actual_pnl": self._strategy_pnl(signals, orders),
            "monotonicity_observation_summary": monotonicity_observation_summary(kalshi_bundle.observations),
            "monotonicity_filter_summary": monotonicity_filter_summary(evaluations),
            "monotonicity_immediate_recheck_summary": monotonicity_immediate_recheck_summary(kalshi_bundle.observations),
            "monotonicity_confirmed_signal_summary": monotonicity_confirmed_signal_summary(kalshi_bundle.observations),
            "cross_market_summary": cross_market_summary(kalshi_bundle.cross_market_snapshots),
            "belief_layer_summary": belief_layer_summary(belief_bundle.belief_snapshots, belief_bundle.disagreement_snapshots),
            "outcome_tracking_summary": outcome_tracking_summary(belief_bundle.outcome_tracking),
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

    def _strategy_pnl(self, signals, orders) -> dict:
        pnl = defaultdict(lambda: {"estimated_pnl": 0.0, "actual_pnl": 0.0})
        accepted_orders = {row["client_order_id"]: row for row in orders if "simulated" in row["status"] or "accepted" in row["status"]}
        for row in signals:
            strategy = row["strategy"]
            estimated = float(row["expected_edge_bps"]) / 10_000.0 * int(row["quantity"])
            pnl[strategy]["estimated_pnl"] += estimated
        for _row in accepted_orders.values():
            pnl["execution"]["actual_pnl"] += 0.0
        return dict(pnl)
