from __future__ import annotations

import json
from collections import defaultdict


def rejection_examples(rows) -> dict[str, list[dict]]:
    examples: dict[str, list[dict]] = defaultdict(list)
    for row in rows:
        if row["generated"]:
            continue
        reason = row["reason"]
        if len(examples[reason]) >= 3:
            continue
        metadata = load_metadata(row["metadata"])
        examples[reason].append(
            {
                "ticker": row["ticker"],
                "strategy": row["strategy"],
                "metadata": metadata,
            }
        )
    return dict(examples)


def monotonicity_observation_summary(rows) -> dict:
    if not rows:
        return {
            "total_accepted_signals": 0,
            "product_family_breakdown": {},
            "avg_edge_after_fees": 0.0,
            "simulated_pnl": 0.0,
            "quote_persistence_stats": {
                "still_present_pct": 0.0,
                "partially_present_pct": 0.0,
                "gone_immediately_pct": 0.0,
                "avg_persistence_time_seconds": 0.0,
            },
            "top_signals": [],
        }
    next_status_counts = defaultdict(int)
    product_families = defaultdict(int)
    persistence_times: list[float] = []
    top_signals: list[dict] = []
    simulated_pnl = 0.0
    for row in rows:
        product_families[row["product_family"]] += 1
        if row["next_refresh_status"]:
            next_status_counts[row["next_refresh_status"]] += 1
        if row["time_to_disappear_seconds"] is not None:
            persistence_times.append(float(row["time_to_disappear_seconds"]))
        metadata = load_metadata(row["metadata"])
        simulated_pnl += float(row["edge_after_fees"]) * int(metadata.get("quantity", 0))
        top_signals.append(
            {
                "ticker": row["ticker"],
                "lower_ticker": row["lower_ticker"],
                "higher_ticker": row["higher_ticker"],
                "edge_after_fees": row["edge_after_fees"],
                "lower_yes_ask_price": row["lower_yes_ask_price"],
                "lower_yes_ask_size": row["lower_yes_ask_size"],
                "higher_yes_bid_price": row["higher_yes_bid_price"],
                "higher_yes_bid_size": row["higher_yes_bid_size"],
                "next_refresh_status": row["next_refresh_status"],
                "time_to_disappear_seconds": row["time_to_disappear_seconds"],
                "metadata": metadata,
            }
        )
    total = len(rows)
    top_signals.sort(key=lambda item: item["edge_after_fees"], reverse=True)
    return {
        "total_accepted_signals": total,
        "product_family_breakdown": dict(product_families),
        "avg_edge_after_fees": avg(row["edge_after_fees"] for row in rows),
        "simulated_pnl": simulated_pnl,
        "quote_persistence_stats": {
            "still_present_pct": (next_status_counts["still_present"] / total) if total else 0.0,
            "partially_present_pct": (next_status_counts["partially_present"] / total) if total else 0.0,
            "gone_immediately_pct": (next_status_counts["gone"] / total) if total else 0.0,
            "avg_persistence_time_seconds": avg(persistence_times),
        },
        "top_signals": top_signals[:20],
    }


def monotonicity_filter_summary(rows) -> dict:
    monotonicity_rows = [row for row in rows if row["strategy"] == "monotonicity"]
    if not monotonicity_rows:
        return {
            "size_multiple_pass_rate": 0.0,
            "size_multiple_ratios": {},
            "acceptance_rate_after_all_filters": 0.0,
            "counts_by_ladder_pair": {},
        }
    size_multiple_eligible = 0
    size_multiple_pass = 0
    lower_ratios: list[float] = []
    higher_ratios: list[float] = []
    ladder_pair_counts: dict[str, int] = defaultdict(int)
    for row in monotonicity_rows:
        metadata = load_metadata(row["metadata"])
        lower = metadata.get("lower")
        higher = metadata.get("higher")
        if lower and higher and row["generated"]:
            ladder_pair_counts[f"{lower} -> {higher}"] += 1
        lower_ratio = metadata.get("size_multiple_lower")
        higher_ratio = metadata.get("size_multiple_higher")
        if lower_ratio is None or higher_ratio is None:
            continue
        lower_ratio = float(lower_ratio)
        higher_ratio = float(higher_ratio)
        lower_ratios.append(lower_ratio)
        higher_ratios.append(higher_ratio)
        size_multiple_eligible += 1
        if lower_ratio >= float(metadata.get("min_size_multiple", 0.0)) and higher_ratio >= float(metadata.get("min_size_multiple", 0.0)):
            size_multiple_pass += 1
    return {
        "size_multiple_pass_rate": (size_multiple_pass / size_multiple_eligible) if size_multiple_eligible else 0.0,
        "size_multiple_ratios": {
            "lower_average": avg(lower_ratios),
            "lower_median": median(lower_ratios),
            "higher_average": avg(higher_ratios),
            "higher_median": median(higher_ratios),
        },
        "acceptance_rate_after_all_filters": sum(1 for row in monotonicity_rows if row["generated"]) / len(monotonicity_rows),
        "counts_by_ladder_pair": dict(sorted(ladder_pair_counts.items(), key=lambda item: item[1], reverse=True)[:20]),
    }


def monotonicity_immediate_recheck_summary(rows) -> dict:
    observed_rows = []
    confirmation_delays: list[float] = []
    survived = 0
    reasons: dict[str, int] = defaultdict(int)
    for row in rows:
        metadata = load_metadata(row["metadata"])
        if "immediate_recheck_survived" not in metadata:
            continue
        observed_rows.append(row)
        if metadata.get("immediate_recheck_survived"):
            survived += 1
        reasons[str(metadata.get("immediate_recheck_reason", "unknown"))] += 1
        delay = metadata.get("confirmation_delay_seconds")
        if delay is not None:
            confirmation_delays.append(float(delay))
    total = len(observed_rows)
    return {
        "signals_rechecked": total,
        "signals_survived": survived,
        "survival_pct": (survived / total) if total else 0.0,
        "average_confirmation_delay_seconds": avg(confirmation_delays),
        "median_confirmation_delay_seconds": median(confirmation_delays),
        "counts_by_reason": dict(reasons),
    }


def monotonicity_confirmed_signal_summary(rows) -> dict:
    total_detected = 0
    confirmed_rows: list[dict] = []
    product_families: dict[str, int] = defaultdict(int)
    ladder_pairs: dict[str, int] = defaultdict(int)
    confirmation_delays: list[float] = []
    edge_before_decay: list[float] = []
    edge_after_decay: list[float] = []
    lower_size_decay: list[float] = []
    higher_size_decay: list[float] = []
    followup_survived = 0
    followup_total = 0
    for row in rows:
        metadata = load_metadata(row["metadata"])
        if "immediate_recheck_survived" not in metadata:
            continue
        total_detected += 1
        if not metadata.get("immediate_recheck_survived"):
            continue
        confirmed_rows.append({"row": row, "metadata": metadata})
        product_families[row["product_family"]] += 1
        ladder_pairs[f"{row['lower_ticker']} -> {row['higher_ticker']}"] += 1
        if metadata.get("confirmation_delay_seconds") is not None:
            confirmation_delays.append(float(metadata["confirmation_delay_seconds"]))
        if metadata.get("confirmation_edge_before_fees") is not None:
            edge_before_decay.append(float(metadata["confirmation_edge_before_fees"]) - float(row["edge_before_fees"]))
        if metadata.get("confirmation_edge_after_fees") is not None:
            edge_after_decay.append(float(metadata["confirmation_edge_after_fees"]) - float(row["edge_after_fees"]))
        if metadata.get("confirmation_lower_yes_ask_size") is not None:
            lower_size_decay.append(float(metadata["confirmation_lower_yes_ask_size"]) - float(row["lower_yes_ask_size"]))
        if metadata.get("confirmation_higher_yes_bid_size") is not None:
            higher_size_decay.append(float(metadata["confirmation_higher_yes_bid_size"]) - float(row["higher_yes_bid_size"]))
        if "followup_survived" in metadata:
            followup_total += 1
            if metadata.get("followup_survived"):
                followup_survived += 1
    executable_after_confirmation = 0
    top_confirmed: list[dict] = []
    for item in confirmed_rows:
        row = item["row"]
        metadata = item["metadata"]
        if metadata.get("confirmation_edge_after_fees", 0.0) >= 0.05:
            executable_after_confirmation += 1
        top_confirmed.append(
            {
                "ticker": row["ticker"],
                "lower_ticker": row["lower_ticker"],
                "higher_ticker": row["higher_ticker"],
                "product_family": row["product_family"],
                "detection_edge_before_fees": row["edge_before_fees"],
                "detection_edge_after_fees": row["edge_after_fees"],
                "confirmation_edge_before_fees": metadata.get("confirmation_edge_before_fees"),
                "confirmation_edge_after_fees": metadata.get("confirmation_edge_after_fees"),
                "detection_lower_yes_ask_size": row["lower_yes_ask_size"],
                "detection_higher_yes_bid_size": row["higher_yes_bid_size"],
                "confirmation_lower_yes_ask_size": metadata.get("confirmation_lower_yes_ask_size"),
                "confirmation_higher_yes_bid_size": metadata.get("confirmation_higher_yes_bid_size"),
                "confirmation_delay_seconds": metadata.get("confirmation_delay_seconds"),
                "followup_survived": metadata.get("followup_survived"),
                "followup_edge_after_fees": metadata.get("followup_edge_after_fees"),
            }
        )
    top_confirmed.sort(key=lambda item: item["confirmation_edge_after_fees"] or 0.0, reverse=True)
    return {
        "total_detected": total_detected,
        "total_confirmed": len(confirmed_rows),
        "confirmation_rate": (len(confirmed_rows) / total_detected) if total_detected else 0.0,
        "total_surviving_second_followup": followup_survived,
        "followup_survival_rate_conditional_on_confirmation": (followup_survived / followup_total) if followup_total else 0.0,
        "average_confirmation_delay_seconds": avg(confirmation_delays),
        "median_confirmation_delay_seconds": median(confirmation_delays),
        "edge_decay_from_detection_to_confirmation": {
            "before_fees_average": avg(edge_before_decay),
            "before_fees_median": median(edge_before_decay),
            "after_fees_average": avg(edge_after_decay),
            "after_fees_median": median(edge_after_decay),
        },
        "size_decay_from_detection_to_confirmation": {
            "lower_yes_ask_size_average": avg(lower_size_decay),
            "lower_yes_ask_size_median": median(lower_size_decay),
            "higher_yes_bid_size_average": avg(higher_size_decay),
            "higher_yes_bid_size_median": median(higher_size_decay),
        },
        "product_families": dict(product_families),
        "ladder_pairs": dict(sorted(ladder_pairs.items(), key=lambda item: item[1], reverse=True)),
        "confirmed_signals_still_executable_after_confirmation": executable_after_confirmation,
        "top_confirmed_signals": top_confirmed[:20],
    }


def cross_market_summary(rows) -> dict:
    if not rows:
        return {
            "total_snapshots_collected": 0,
            "avg_disagreement_score": 0.0,
            "top_highest_disagreement_events": [],
            "breakdown_by_event_ticker": {},
            "recent_disagreement_trends": [],
        }
    by_event: dict[str, list[float]] = defaultdict(list)
    for row in rows:
        by_event[row["event_ticker"]].append(float(row["derived_risk_score"]))
    top_events = sorted(rows, key=lambda row: float(row["derived_risk_score"]), reverse=True)[:10]
    recent = rows[-10:]
    return {
        "total_snapshots_collected": len(rows),
        "avg_disagreement_score": avg(float(row["derived_risk_score"]) for row in rows),
        "top_highest_disagreement_events": [
            {
                "timestamp": row["timestamp"],
                "event_ticker": row["event_ticker"],
                "contract_pair": row["contract_pair"],
                "kalshi_probability": row["kalshi_probability"],
                "derived_risk_score": row["derived_risk_score"],
                "notes": row["notes"],
            }
            for row in top_events
        ],
        "breakdown_by_event_ticker": {
            ticker: {
                "count": len(scores),
                "avg_disagreement": avg(scores),
                "max_disagreement": max(scores),
            }
            for ticker, scores in sorted(by_event.items())
        },
        "recent_disagreement_trends": [
            {
                "timestamp": row["timestamp"],
                "event_ticker": row["event_ticker"],
                "derived_risk_score": row["derived_risk_score"],
            }
            for row in recent
        ],
    }


def avg(values) -> float:
    values = list(values)
    return sum(values) / len(values) if values else 0.0


def median(values) -> float:
    values = sorted(values)
    if not values:
        return 0.0
    midpoint = len(values) // 2
    if len(values) % 2:
        return values[midpoint]
    return (values[midpoint - 1] + values[midpoint]) / 2.0


def load_metadata(raw: str) -> dict:
    try:
        return json.loads(raw) if raw else {}
    except json.JSONDecodeError:
        return {}
