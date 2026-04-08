from __future__ import annotations

import hashlib
import json
import sys
from collections import defaultdict
from dataclasses import asdict, is_dataclass
from datetime import datetime, timezone
from pathlib import Path


def build_event_run_manifest(settings) -> dict:
    config_snapshot = _settings_snapshot(settings)
    config_identity = hashlib.sha256(
        json.dumps(config_snapshot, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()
    code_files = [
        Path(__file__).resolve().parent / "events" / "runtime.py",
        Path(__file__).resolve().parent / "execution" / "broker.py",
        Path(__file__).resolve().parent / "execution" / "event_engine.py",
        Path(__file__).resolve().parent / "risk" / "event_risk.py",
        Path(__file__).resolve().parent / "classification" / "service.py",
        Path(__file__).resolve().parent / "mapping" / "engine.py",
        Path(__file__).resolve(),
    ]
    return {
        "mode": "event_driven_paper",
        "config_snapshot": config_snapshot,
        "config_identity": config_identity,
        "code_identity": _code_identity(code_files),
        "execution_model": {
            "fill_conservatism": "top_of_book_taker_only_for_marketable_orders",
            "marketable_buy_fill": "ask_or_worse",
            "marketable_sell_fill": "bid_or_worse",
            "partial_fills_enabled": True,
            "displayed_size_enforced": True,
            "passive_orders_assumed_instant": False,
            "numeric_model_status": "internal analytics remain float-oriented in several persistence/reporting paths; exchange-facing fixed-point migration is only partially implemented",
            "submission_latency_seconds": settings.event_paper_submission_latency_seconds,
            "half_spread_bps": settings.event_paper_half_spread_bps,
            "top_level_size": settings.event_paper_top_level_size,
            "depth_levels": settings.event_paper_depth_levels,
            "passive_recheck_seconds": settings.event_paper_passive_recheck_seconds,
            "session_boundary_buffer_seconds": settings.event_paper_session_boundary_buffer_seconds,
            "session_boundary_liquidity_factor": settings.event_paper_session_boundary_liquidity_factor,
            "session_boundary_spread_multiplier": settings.event_paper_session_boundary_spread_multiplier,
        },
    }


def merge_run_manifest(manifest: dict) -> dict:
    merged = {
        "manifest_schema_version": "paper-event-run-manifest-v1",
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "python_version": sys.version.split()[0],
    }
    merged.update(manifest)
    return merged


def event_driven_summary(news_rows, classified_rows, candidate_rows, queue_rows, position_rows, outcome_rows, audit_rows, loop_stats: dict) -> dict:
    candidates_by_status: dict[str, int] = defaultdict(int)
    candidates_by_symbol: dict[str, int] = defaultdict(int)
    classified_by_type: dict[str, int] = defaultdict(int)
    outcomes_by_symbol: dict[str, list[float]] = defaultdict(list)
    open_positions = 0
    for row in classified_rows:
        classified_by_type[row["event_type"]] += 1
    for row in candidate_rows:
        candidates_by_status[row["status"]] += 1
        candidates_by_symbol[row["symbol"]] += 1
    for row in position_rows:
        if row["status"] == "open":
            open_positions += 1
    for row in outcome_rows:
        outcomes_by_symbol[row["symbol"]].append(float(row["return_pct"]))
    recent_news = [
        {
            "timestamp_utc": row["timestamp_utc"],
            "source": row["source"],
            "headline": row["headline"],
            "url": row["url"],
        }
        for row in news_rows[-10:]
    ]
    top_symbol_outcomes = [
        {
            "symbol": symbol,
            "count": len(returns),
            "average_return_pct": _avg(returns),
            "median_return_pct": _median(returns),
        }
        for symbol, returns in sorted(outcomes_by_symbol.items(), key=lambda item: len(item[1]), reverse=True)[:10]
    ]
    return {
        "loop_stats": loop_stats,
        "news_events_ingested": len(news_rows),
        "classified_events": len(classified_rows),
        "trade_candidates": len(candidate_rows),
        "candidate_status_breakdown": dict(sorted(candidates_by_status.items())),
        "classified_event_type_breakdown": dict(sorted(classified_by_type.items())),
        "queue_status_breakdown": dict(sorted(count_field(queue_rows, "status").items())),
        "open_positions": open_positions,
        "closed_positions": len(outcome_rows),
        "average_return_pct": _avg(float(row["return_pct"]) for row in outcome_rows),
        "median_return_pct": _median(float(row["return_pct"]) for row in outcome_rows),
        "win_rate": (sum(1 for row in outcome_rows if float(row["return_pct"]) > 0) / len(outcome_rows)) if outcome_rows else 0.0,
        "top_symbols_by_candidates": dict(sorted(candidates_by_symbol.items(), key=lambda item: item[1], reverse=True)[:10]),
        "top_symbols_by_outcome": top_symbol_outcomes,
        "legacy_paper_execution_audit_rows": len(audit_rows),
        "execution_realism_caveats": [
            "paper metrics do not prove live execution viability",
            "legging loss, disconnect handling, stale-book halts, and exchange-side state divergence remain material risks",
            "fee and slippage realism should be interpreted conservatively, not as broker-grade reconciliation",
        ],
        "recent_news": recent_news,
        "recent_closed_trades": [
            {
                "symbol": row["symbol"],
                "entry_price": row["entry_price"],
                "exit_price": row["exit_price"],
                "return_pct": row["return_pct"],
                "holding_seconds": row["holding_seconds"],
                "exit_reason": row["exit_reason"],
                "closed_at": row["closed_at"],
            }
            for row in outcome_rows[-10:]
        ],
    }


def count_field(rows, field: str) -> dict[str, int]:
    counts: dict[str, int] = defaultdict(int)
    for row in rows:
        counts[str(row[field])] += 1
    return counts


def _settings_snapshot(settings) -> dict:
    if not is_dataclass(settings):
        return {}
    snapshot = {}
    for key, value in asdict(settings).items():
        if isinstance(value, Path):
            snapshot[key] = str(value)
        else:
            snapshot[key] = value
    return snapshot


def _code_identity(paths: list[Path]) -> dict:
    normalized_paths = [path for path in paths if path.exists()]
    payload = []
    for path in normalized_paths:
        content = path.read_text(encoding="utf-8")
        payload.append(
            {
                "path": str(path),
                "sha256": hashlib.sha256(content.encode("utf-8")).hexdigest(),
            }
        )
    aggregate = hashlib.sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()
    return {
        "tracked_files": payload,
        "aggregate_sha256": aggregate,
    }


def _avg(values) -> float:
    values = list(values)
    return sum(values) / len(values) if values else 0.0


def _median(values) -> float:
    values = sorted(float(value) for value in values)
    if not values:
        return 0.0
    midpoint = len(values) // 2
    if len(values) % 2 == 1:
        return values[midpoint]
    return (values[midpoint - 1] + values[midpoint]) / 2.0
