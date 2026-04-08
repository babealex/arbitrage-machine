from __future__ import annotations

import json
from collections import defaultdict
from datetime import datetime, timezone


def belief_layer_summary(belief_rows, disagreement_rows, belief_sources=None, event_definitions=None, cross_market_rows=None) -> dict:
    cross_market_rows = cross_market_rows or []
    belief_sources = belief_sources or []
    event_definitions = event_definitions or []
    if not belief_rows and not disagreement_rows and not cross_market_rows:
        return {
            "belief_sources_registered": 0,
            "event_definitions_registered": 0,
            "belief_snapshots_collected": 0,
            "disagreement_snapshots_collected": 0,
            "cross_market_disagreements_collected": 0,
            "top_event_groups_by_disagreement_frequency": [],
            "top_families_by_disagreement_magnitude": [],
            "tradeable_disagreement_rate": 0.0,
            "recent_examples": [],
        }
    disagreement_by_group: dict[str, int] = defaultdict(int)
    disagreement_by_family: dict[str, list[float]] = defaultdict(list)
    for row in disagreement_rows:
        disagreement_by_group[row["event_group"]] += 1
        metadata = load_metadata(row["metadata_json"])
        family = str(metadata.get("kalshi_metadata", {}).get("family", row["event_group"].split(":", 1)[0]))
        disagreement_by_family[family].append(float(row["disagreement_score"]))
    top_groups = sorted(disagreement_by_group.items(), key=lambda item: item[1], reverse=True)[:10]
    top_families = sorted(
        (
            {
                "family": family,
                "avg_disagreement_magnitude": avg(scores),
                "count": len(scores),
            }
            for family, scores in disagreement_by_family.items()
        ),
        key=lambda item: item["avg_disagreement_magnitude"],
        reverse=True,
    )[:10]
    all_disagreements = list(disagreement_rows) + list(cross_market_rows)
    tradeable_count = 0
    for row in all_disagreements:
        if row.get("tradeable") or load_metadata(row["metadata_json"]).get("net_disagreement", 0) > 0:
            tradeable_count += 1
    recent_examples = [
        {
            "timestamp_utc": row["timestamp_utc"],
            "event_group": row["event_group"],
            "kalshi_reference": row["kalshi_reference"],
            "external_reference": row["external_reference"],
            "disagreement_score": row["disagreement_score"],
            "zscore": row["zscore"],
            "metadata_json": load_metadata(row["metadata_json"]),
        }
        for row in all_disagreements[-10:]
    ]
    return {
        "belief_sources_registered": len(belief_sources),
        "event_definitions_registered": len(event_definitions),
        "belief_snapshots_collected": len(belief_rows),
        "disagreement_snapshots_collected": len(disagreement_rows),
        "cross_market_disagreements_collected": len(cross_market_rows),
        "tradeable_disagreement_rate": (tradeable_count / len(all_disagreements)) if all_disagreements else 0.0,
        "top_event_groups_by_disagreement_frequency": [
            {"event_group": event_group, "count": count} for event_group, count in top_groups
        ],
        "top_families_by_disagreement_magnitude": top_families,
        "recent_examples": recent_examples,
    }


def outcome_tracking_summary(rows, outcome_realizations=None) -> dict:
    outcome_realizations = outcome_realizations or []
    if not rows:
        return {
            "tracked_outcomes": 0,
            "realized_outcomes_logged": len(outcome_realizations),
            "due_vs_populated_coverage": {},
            "scheduler_state_counts": {},
            "observation_source_counts_by_horizon": {},
            "observability_status_counts_by_horizon": {},
            "avg_return_by_disagreement_bucket": [],
            "win_rate_by_disagreement_bucket": [],
            "distribution_of_moves": {},
            "retry_error_summary": {},
            "error_breakdown_by_horizon": {},
            "lateness_distribution_by_horizon": {},
            "two_sided_quote_coverage_by_horizon": {},
            "spread_distribution_by_horizon": {},
            "last_trade_fallback_usage_by_horizon": {},
            "populated_source_share_by_horizon": {},
            "research_observability_summary": {},
            "recent_examples": [],
        }
    bucket_stats: dict[str, dict[str, list[float] | int]] = defaultdict(lambda: {"returns": [], "wins": 0, "count": 0})
    family_counts: dict[str, int] = defaultdict(int)
    move_distribution: dict[str, dict[str, float]] = {}
    due_vs_populated_coverage: dict[str, dict[str, float]] = {}
    scheduler_state_counts: dict[str, dict[str, int]] = {}
    observation_source_counts_by_horizon: dict[str, dict[str, int]] = {}
    observability_status_counts_by_horizon: dict[str, dict[str, int]] = {}
    retry_error_summary: dict[str, dict[str, float]] = {}
    error_breakdown_by_horizon: dict[str, dict[str, int]] = {}
    lateness_distribution_by_horizon: dict[str, dict[str, float]] = {}
    two_sided_quote_coverage_by_horizon: dict[str, dict[str, float]] = {}
    spread_distribution_by_horizon: dict[str, dict[str, float]] = {}
    last_trade_fallback_usage_by_horizon: dict[str, int] = {}
    populated_source_share_by_horizon: dict[str, dict[str, float]] = {}
    now = datetime.now(timezone.utc)
    for row in rows:
        disagreement_score = float(row["disagreement_score"])
        bucket = disagreement_bucket(disagreement_score)
        realized_return = best_available_return(row)
        if realized_return is not None:
            bucket_stats[bucket]["returns"].append(realized_return)
            bucket_stats[bucket]["wins"] += 1 if realized_return > 0 else 0
            bucket_stats[bucket]["count"] += 1
        family_counts[row["family"]] += 1
    for horizon in (30, 60, 300, 900):
        due_count = 0
        populated_count = 0
        attempts: list[int] = []
        errors = 0
        state_counts: dict[str, int] = defaultdict(int)
        error_counts: dict[str, int] = defaultdict(int)
        lateness_values: list[float] = []
        source_counts: dict[str, int] = defaultdict(int)
        observability_counts: dict[str, int] = defaultdict(int)
        two_sided_total = 0
        two_sided_populated = 0
        spreads: list[float] = []
        last_trade_usage = 0
        populated_sources: dict[str, int] = defaultdict(int)
        for row in rows:
            due_at = row[f"due_at_{horizon}s"]
            metadata = load_metadata(row["metadata_json"])
            scheduler = metadata.get("scheduler", {})
            horizon_tracker = scheduler.get(str(horizon), {})
            status = horizon_status(row, horizon, now, horizon_tracker)
            state_counts[status] += 1
            source = str(horizon_tracker.get("observation_source") or "unavailable")
            source_counts[source] += 1
            observability = str(horizon_tracker.get("observability_status") or ("observable" if row[f"value_after_{horizon}s"] is not None else "unknown"))
            observability_counts[observability] += 1
            if due_at and parse_iso_datetime(str(due_at)) <= now:
                due_count += 1
                if row[f"value_after_{horizon}s"] is not None:
                    populated_count += 1
            attempts.append(int(horizon_tracker.get("attempts", 0) or 0))
            errors += int(horizon_tracker.get("errors", 0) or 0)
            terminal_reason = horizon_tracker.get("terminal_reason")
            if terminal_reason:
                error_counts[str(terminal_reason)] += 1
            lateness = horizon_tracker.get("lateness_seconds")
            if lateness is not None:
                lateness_values.append(float(lateness))
            if horizon_tracker.get("observed_has_two_sided_quote"):
                two_sided_total += 1
                if row[f"value_after_{horizon}s"] is not None:
                    two_sided_populated += 1
            spread = horizon_tracker.get("observed_spread")
            if spread is not None:
                spreads.append(float(spread))
            if source == "last_trade":
                last_trade_usage += 1
            if row[f"value_after_{horizon}s"] is not None:
                populated_sources[source] += 1
        due_vs_populated_coverage[f"{horizon}s"] = {
            "due_count": due_count,
            "populated_count": populated_count,
            "coverage_pct": (populated_count / due_count) if due_count else 0.0,
        }
        scheduler_state_counts[f"{horizon}s"] = dict(state_counts)
        observation_source_counts_by_horizon[f"{horizon}s"] = dict(source_counts)
        observability_status_counts_by_horizon[f"{horizon}s"] = dict(observability_counts)
        retry_error_summary[f"{horizon}s"] = {
            "total_attempts": sum(attempts),
            "average_attempts_per_row": avg(attempts),
            "total_errors": errors,
        }
        error_breakdown_by_horizon[f"{horizon}s"] = dict(error_counts)
        lateness_distribution_by_horizon[f"{horizon}s"] = {
            "count": len(lateness_values),
            "average": avg(lateness_values),
            "median": median(lateness_values),
            "min": min(lateness_values) if lateness_values else 0.0,
            "max": max(lateness_values) if lateness_values else 0.0,
        }
        two_sided_quote_coverage_by_horizon[f"{horizon}s"] = {
            "two_sided_quote_rows": two_sided_total,
            "two_sided_quote_populated_rows": two_sided_populated,
            "two_sided_quote_populated_pct": (two_sided_populated / two_sided_total) if two_sided_total else 0.0,
        }
        spread_distribution_by_horizon[f"{horizon}s"] = {
            "count": len(spreads),
            "average": avg(spreads),
            "median": median(spreads),
            "min": min(spreads) if spreads else 0.0,
            "max": max(spreads) if spreads else 0.0,
        }
        last_trade_fallback_usage_by_horizon[f"{horizon}s"] = last_trade_usage
        populated_total = sum(populated_sources.values())
        populated_source_share_by_horizon[f"{horizon}s"] = {
            source: (count / populated_total) if populated_total else 0.0
            for source, count in sorted(populated_sources.items())
        }
    for horizon in (30, 60, 300, 900):
        deltas = []
        for row in rows:
            value = row[f"value_after_{horizon}s"]
            if value is None:
                continue
            deltas.append(float(value) - float(row["initial_value"]))
        move_distribution[f"{horizon}s"] = {
            "count": len(deltas),
            "average": avg(deltas),
            "median": median(deltas),
            "min": min(deltas) if deltas else 0.0,
            "max": max(deltas) if deltas else 0.0,
        }
    avg_return_by_bucket = []
    win_rate_by_bucket = []
    for bucket, stats in sorted(bucket_stats.items()):
        returns = list(stats["returns"])
        count = int(stats["count"])
        avg_return_by_bucket.append(
            {
                "bucket": bucket,
                "count": count,
                "average_return": avg(returns),
                "median_return": median(returns),
            }
        )
        win_rate_by_bucket.append(
            {
                "bucket": bucket,
                "count": count,
                "win_rate": (int(stats["wins"]) / count) if count else 0.0,
            }
        )
    recent_examples = [
        {
            "timestamp_detected": row["timestamp_detected"],
            "event_ticker": row["event_ticker"],
            "family": row["family"],
            "initial_value": row["initial_value"],
            "disagreement_score": row["disagreement_score"],
            "value_after_30s": row["value_after_30s"],
            "value_after_60s": row["value_after_60s"],
            "value_after_300s": row["value_after_300s"],
            "value_after_900s": row["value_after_900s"],
            "max_favorable_move": row["max_favorable_move"],
            "max_adverse_move": row["max_adverse_move"],
            "resolved_direction": row["resolved_direction"],
            "metadata_json": load_metadata(row["metadata_json"]),
        }
        for row in rows[-10:]
    ]
    return {
        "tracked_outcomes": len(rows),
        "realized_outcomes_logged": len(outcome_realizations),
        "due_vs_populated_coverage": due_vs_populated_coverage,
        "scheduler_state_counts": scheduler_state_counts,
        "observation_source_counts_by_horizon": observation_source_counts_by_horizon,
        "observability_status_counts_by_horizon": observability_status_counts_by_horizon,
        "avg_return_by_disagreement_bucket": avg_return_by_bucket,
        "win_rate_by_disagreement_bucket": win_rate_by_bucket,
        "distribution_of_moves": move_distribution,
        "retry_error_summary": retry_error_summary,
        "error_breakdown_by_horizon": error_breakdown_by_horizon,
        "lateness_distribution_by_horizon": lateness_distribution_by_horizon,
        "two_sided_quote_coverage_by_horizon": two_sided_quote_coverage_by_horizon,
        "spread_distribution_by_horizon": spread_distribution_by_horizon,
        "last_trade_fallback_usage_by_horizon": last_trade_fallback_usage_by_horizon,
        "populated_source_share_by_horizon": populated_source_share_by_horizon,
        "family_counts": dict(sorted(family_counts.items())),
        "research_observability_summary": research_observability_summary(rows, now),
        "tradability_segment_analysis": tradability_segment_analysis(rows, now),
        "recent_examples": recent_examples,
    }


def best_available_return(row) -> float | None:
    initial_value = float(row["initial_value"])
    for field_name in ("value_after_900s", "value_after_300s", "value_after_60s", "value_after_30s"):
        value = row[field_name]
        if value is not None:
            return float(value) - initial_value
    return None


def disagreement_bucket(score: float) -> str:
    if score < 0.30:
        return "<0.30"
    if score < 0.50:
        return "0.30-0.50"
    if score < 0.70:
        return "0.50-0.70"
    return ">=0.70"


def parse_iso_datetime(value: str) -> datetime:
    parsed = datetime.fromisoformat(value)
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def horizon_status(row, horizon: int, now: datetime, horizon_tracker: dict) -> str:
    if row[f"value_after_{horizon}s"] is not None:
        return "populated"
    due_at = row[f"due_at_{horizon}s"]
    if not due_at or parse_iso_datetime(str(due_at)) > now:
        return "not_due"
    status = str(horizon_tracker.get("terminal_status") or "")
    if status == "terminal_failed":
        return "terminal_failed"
    if status == "retrying":
        return "retrying"
    return "due"


def research_observability_summary(rows, now: datetime) -> dict:
    liquidity_failed = 0
    executable_without_midpoint = 0
    flat_by_source: dict[str, int] = defaultdict(int)
    family_missing_two_sided: dict[str, int] = defaultdict(int)
    for row in rows:
        metadata = load_metadata(row["metadata_json"])
        scheduler = metadata.get("scheduler", {})
        row_sources: set[str] = set()
        row_flat = row["resolved_direction"] == "flat"
        for horizon in (30, 60, 300, 900):
            horizon_tracker = scheduler.get(str(horizon), {})
            status = horizon_status(row, horizon, now, horizon_tracker)
            source = str(horizon_tracker.get("observation_source") or "unavailable")
            row_sources.add(source)
            reason = str(horizon_tracker.get("terminal_reason") or "")
            if status in {"retrying", "terminal_failed", "due"} and reason in {"liquidity_unobservable", "no_two_sided_quote", "no_orderbook_depth", "no_recent_trade"}:
                liquidity_failed += 1
            if source == "executable_quote":
                executable_without_midpoint += 1
            if not horizon_tracker.get("observed_has_two_sided_quote") and status != "not_due":
                family_missing_two_sided[row["family"]] += 1
        if row_flat:
            for source in row_sources:
                flat_by_source[source] += 1
    return {
        "genuinely_unobservable_due_to_thin_liquidity": liquidity_failed,
        "usable_executable_quotes_without_midpoint": executable_without_midpoint,
        "flat_outcomes_by_source": dict(flat_by_source),
        "families_lacking_two_sided_quotes": dict(sorted(family_missing_two_sided.items(), key=lambda item: item[1], reverse=True)[:10]),
    }


def tradability_segment_analysis(rows, now: datetime) -> dict:
    by_horizon: dict[str, dict] = {}
    research_answers: dict[str, dict | str] = {}
    for horizon in (30, 60, 300):
        populated = []
        for row in rows:
            value = row[f"value_after_{horizon}s"]
            if value is None:
                continue
            metadata = load_metadata(row["metadata_json"])
            tracker = metadata.get("scheduler", {}).get(str(horizon), {})
            initial_value = float(row["initial_value"])
            realized_return = float(value) - initial_value
            populated.append(
                {
                    "family": row["family"],
                    "return": realized_return,
                    "resolved_direction": row["resolved_direction"],
                    "source": str(tracker.get("observation_source") or "unavailable"),
                    "two_sided": bool(tracker.get("observed_has_two_sided_quote")),
                    "spread": tracker.get("observed_spread"),
                    "max_favorable_move": float(row["max_favorable_move"] or 0.0),
                    "max_adverse_move": float(row["max_adverse_move"] or 0.0),
                    "status": str(tracker.get("observability_status") or "unknown"),
                    "reason": str(tracker.get("observability_reason") or tracker.get("terminal_reason") or ""),
                }
            )
        by_horizon[f"{horizon}s"] = {
            "segments_by_tradability": segment_stats(populated, tradability_bucket),
            "segments_by_spread_bucket": segment_stats(populated, spread_bucket),
            "segments_by_family": segment_stats(populated, lambda item: item["family"]),
        }
    reference_horizon = "60s" if by_horizon["60s"]["segments_by_tradability"] else "30s"
    tradability_segments = by_horizon[reference_horizon]["segments_by_tradability"]
    spread_segments = by_horizon[reference_horizon]["segments_by_spread_bucket"]
    research_answers["expected_direction_concentration"] = {
        "reference_horizon": reference_horizon,
        "two_sided_tight_markets_win_rate": segment_value(spread_segments, "<=3", "win_rate"),
        "wide_markets_win_rate": segment_value(spread_segments, ">7", "win_rate"),
        "midpoint_two_sided_win_rate": segment_value(tradability_segments, "midpoint+two_sided_quote", "win_rate"),
        "midpoint_not_two_sided_win_rate": segment_value(tradability_segments, "midpoint+not_two_sided", "win_rate"),
    }
    research_answers["flat_outcome_concentration"] = {
        "reference_horizon": reference_horizon,
        "wide_markets_flat_rate": segment_value(spread_segments, ">7", "flat_rate"),
        "tight_markets_flat_rate": segment_value(spread_segments, "<=3", "flat_rate"),
        "liquidity_unobservable_flat_rate": segment_value(tradability_segments, "liquidity_unobservable", "flat_rate"),
    }
    research_answers["family_observability_vs_tradability"] = family_tradability_summary(rows, now, reference_horizon)
    research_answers["horizon_comparison"] = {
        horizon: {
            "tradable_count": segment_value(by_horizon[horizon]["segments_by_tradability"], "midpoint+two_sided_quote", "count"),
            "tradable_win_rate": segment_value(by_horizon[horizon]["segments_by_tradability"], "midpoint+two_sided_quote", "win_rate"),
            "tradable_avg_return": segment_value(by_horizon[horizon]["segments_by_tradability"], "midpoint+two_sided_quote", "average_return"),
            "unobservable_count": segment_value(by_horizon[horizon]["segments_by_tradability"], "liquidity_unobservable", "count"),
        }
        for horizon in ("30s", "60s", "300s")
    }
    return {
        "by_horizon": by_horizon,
        "research_answers": research_answers,
    }


def segment_stats(items: list[dict], key_fn) -> dict[str, dict]:
    buckets: dict[str, list[dict]] = defaultdict(list)
    for item in items:
        buckets[str(key_fn(item))].append(item)
    return {
        key: {
            "count": len(bucket),
            "average_return": avg(item["return"] for item in bucket),
            "median_return": median(item["return"] for item in bucket),
            "win_rate": (sum(1 for item in bucket if item["return"] > 0) / len(bucket)) if bucket else 0.0,
            "flat_rate": (sum(1 for item in bucket if item["return"] == 0) / len(bucket)) if bucket else 0.0,
            "max_favorable_move_average": avg(item["max_favorable_move"] for item in bucket),
            "max_favorable_move_median": median(item["max_favorable_move"] for item in bucket),
            "max_adverse_move_average": avg(item["max_adverse_move"] for item in bucket),
            "max_adverse_move_median": median(item["max_adverse_move"] for item in bucket),
        }
        for key, bucket in sorted(buckets.items())
    }


def tradability_bucket(item: dict) -> str:
    if item["status"] == "unobservable":
        return "liquidity_unobservable"
    if item["source"] == "midpoint" and item["two_sided"]:
        return "midpoint+two_sided_quote"
    if item["source"] == "midpoint":
        return "midpoint+not_two_sided"
    if item["source"] == "executable_quote":
        return "executable_quote"
    if item["source"] == "last_trade":
        return "last_trade"
    return "observable_but_likely_untradable"


def spread_bucket(item: dict) -> str:
    spread = item["spread"]
    if spread is None:
        return "unavailable"
    spread = float(spread)
    if spread <= 3:
        return "<=3"
    if spread <= 7:
        return "4-7"
    return ">7"


def segment_value(segments: dict[str, dict], key: str, field: str):
    return segments.get(key, {}).get(field, 0.0 if field != "count" else 0)


def family_tradability_summary(rows, now: datetime, horizon_label: str) -> list[dict]:
    horizon = int(horizon_label.rstrip("s"))
    family_totals: dict[str, dict[str, int]] = defaultdict(lambda: {"observable": 0, "tradable": 0, "unobservable": 0})
    for row in rows:
        metadata = load_metadata(row["metadata_json"])
        tracker = metadata.get("scheduler", {}).get(str(horizon), {})
        status = horizon_status(row, horizon, now, tracker)
        if status == "not_due":
            continue
        family_totals[row["family"]]["observable"] += 1 if row[f"value_after_{horizon}s"] is not None else 0
        if tracker.get("observed_has_two_sided_quote"):
            family_totals[row["family"]]["tradable"] += 1
        if status in {"retrying", "terminal_failed", "due"} and str(tracker.get("observability_reason") or tracker.get("terminal_reason") or "") in {"liquidity_unobservable", "no_two_sided_quote", "no_orderbook_depth", "no_recent_trade"}:
            family_totals[row["family"]]["unobservable"] += 1
    summary = []
    for family, counts in family_totals.items():
        observable = counts["observable"]
        tradable = counts["tradable"]
        summary.append(
            {
                "family": family,
                "observable_count": observable,
                "tradable_count": tradable,
                "unobservable_count": counts["unobservable"],
                "tradable_share_of_observable": (tradable / observable) if observable else 0.0,
            }
        )
    summary.sort(key=lambda item: item["tradable_share_of_observable"], reverse=True)
    return summary


def avg(values) -> float:
    values = list(values)
    return sum(values) / len(values) if values else 0.0


def median(values) -> float:
    values = sorted(float(value) for value in values)
    if not values:
        return 0.0
    midpoint = len(values) // 2
    if len(values) % 2 == 1:
        return values[midpoint]
    return (values[midpoint - 1] + values[midpoint]) / 2.0


def load_metadata(raw: str) -> dict:
    try:
        return json.loads(raw) if raw else {}
    except json.JSONDecodeError:
        return {}
