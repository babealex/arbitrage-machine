from __future__ import annotations

import json
from collections import defaultdict
from pathlib import Path


HORIZONS = ("5m", "15m", "60m", "eod")


class ReplayReportWriter:
    def __init__(self, output_path: Path) -> None:
        self.output_path = output_path

    def write(self, results: list[dict], extra: dict | None = None) -> dict:
        extra = extra or {}
        confirmed = [row for row in results if row["confirmation"]["passed"] and row["simulation"]]
        report = {
            "summary": _summary_stats(confirmed),
            "by_event_type": _segment_breakdown(confirmed, lambda row: row["classification"]["event_type"]),
            "by_source": _segment_breakdown(confirmed, lambda row: row["news_event"]["source"]),
            "by_symbol": _segment_breakdown(confirmed, lambda row: row["trade_idea"]["symbol"]),
            "by_confirmation_count": _segment_breakdown(confirmed, lambda row: str(row["confirmation"]["agreeing_signals"])),
            "by_severity_bucket": _segment_breakdown(confirmed, lambda row: _severity_bucket(float(row["classification"]["severity"]))),
            "data_quality": _data_quality_summary(confirmed, extra),
            "cache_stats": dict(extra.get("cache_stats", {})),
            "research_data_stats": dict(extra.get("research_data_stats", {})),
            "outcome_dataset": dict(extra.get("outcome_dataset", {})),
            "edge_analysis": dict(extra.get("edge_analysis", {})),
            "run_manifest": dict(extra.get("run_manifest", {})),
            "per_event_results": results,
        }
        self.output_path.parent.mkdir(parents=True, exist_ok=True)
        self.output_path.write_text(json.dumps(report, indent=2), encoding="utf-8")
        return report


def _summary_stats(rows: list[dict]) -> dict:
    horizon_returns = {h: [row["simulation"]["returns"][h] for row in rows if row["simulation"]["returns"][h] is not None] for h in HORIZONS}
    return {
        "total_trades": len(rows),
        "win_rate_by_horizon": {h: _win_rate(values) for h, values in horizon_returns.items()},
        "average_return_by_horizon": {h: _avg(values) for h, values in horizon_returns.items()},
        "median_return_by_horizon": {h: _median(values) for h, values in horizon_returns.items()},
        "max_drawdown_by_horizon": {h: _max_drawdown(values) for h, values in horizon_returns.items()},
        "sharpe_like_by_horizon": {h: _sharpe_like(values) for h, values in horizon_returns.items()},
    }


def _segment_breakdown(rows: list[dict], key_fn) -> dict[str, dict]:
    buckets: dict[str, list[dict]] = defaultdict(list)
    for row in rows:
        buckets[str(key_fn(row))].append(row)
    return {key: _summary_stats(bucket) for key, bucket in sorted(buckets.items())}


def _severity_bucket(value: float) -> str:
    if value < 0.5:
        return "<0.50"
    if value < 0.7:
        return "0.50-0.69"
    if value < 0.85:
        return "0.70-0.84"
    return ">=0.85"


def _avg(values: list[float]) -> float:
    return sum(values) / len(values) if values else 0.0


def _median(values: list[float]) -> float:
    values = sorted(values)
    if not values:
        return 0.0
    midpoint = len(values) // 2
    if len(values) % 2:
        return values[midpoint]
    return (values[midpoint - 1] + values[midpoint]) / 2.0


def _win_rate(values: list[float]) -> float:
    return (sum(1 for value in values if value > 0) / len(values)) if values else 0.0


def _sharpe_like(values: list[float]) -> float:
    if len(values) < 2:
        return 0.0
    mean = _avg(values)
    variance = sum((value - mean) ** 2 for value in values) / (len(values) - 1)
    std = variance ** 0.5
    if std == 0:
        return 0.0
    return mean / std


def _max_drawdown(values: list[float]) -> float:
    if not values:
        return 0.0
    equity = 1.0
    peak = 1.0
    max_dd = 0.0
    for ret in values:
        equity *= (1.0 + ret)
        peak = max(peak, equity)
        drawdown = (equity / peak) - 1.0
        max_dd = min(max_dd, drawdown)
    return max_dd


def _data_quality_summary(rows: list[dict], extra: dict) -> dict:
    provider_granularity_counts: dict[str, int] = defaultdict(int)
    approximation_examples: list[dict] = []
    for row in rows:
        simulation = row.get("simulation") or {}
        metadata = simulation.get("metadata") or {}
        snapshot_meta = metadata.get("snapshot_metadata") or {}
        interval_returned = snapshot_meta.get("provider_interval_returned")
        requested_interval = snapshot_meta.get("requested_interval")
        if interval_returned:
            provider_granularity_counts[str(interval_returned)] += 1
            if requested_interval and interval_returned != requested_interval and len(approximation_examples) < 10:
                approximation_examples.append(
                    {
                        "symbol": row["trade_idea"]["symbol"],
                        "requested_interval": requested_interval,
                        "provider_interval_returned": interval_returned,
                        "headline": row["news_event"]["headline"],
                    }
                )
    return {
        "provider_granularity_counts": dict(sorted(provider_granularity_counts.items())),
        "approximation_examples": approximation_examples,
        "cache_enabled": bool(extra.get("cache_enabled", False)),
        "cache_only": bool(extra.get("cache_only", False)),
        "refresh_cache": bool(extra.get("refresh_cache", False)),
        "refresh_data": bool(extra.get("refresh_data", False)),
        "ephemeral": bool(extra.get("ephemeral", False)),
        "data_dir": extra.get("data_dir"),
        "provider_fetches_this_run": extra.get("provider_fetches_this_run"),
        "provider_fetches_lifetime": extra.get("provider_fetches_lifetime"),
        "full_store_hits_this_run": extra.get("full_store_hits_this_run"),
        "partial_store_hits_this_run": extra.get("partial_store_hits_this_run"),
        "granularity_downgrade_count": extra.get("granularity_downgrade_count"),
    }
