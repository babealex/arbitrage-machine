from __future__ import annotations

import json
from collections import defaultdict
from math import sqrt
from pathlib import Path


GROUPING_SPECS = (
    ("event_type", "mapped_symbol"),
    ("event_type", "mapped_symbol", "severity_bucket"),
    ("event_type", "mapped_symbol", "confirmation_count_bucket"),
    ("event_type", "mapped_symbol", "session_bucket"),
    ("event_type", "mapped_symbol", "source"),
    ("event_type", "mapped_symbol", "weekday"),
    ("event_type", "mapped_symbol", "pre_15m_return_bucket"),
    ("event_type", "mapped_symbol", "spy_context_bucket"),
    ("event_type", "mapped_symbol", "vix_context_bucket"),
    ("event_type", "mapped_symbol", "granularity_bucket"),
)


class ReplayEdgeAnalyzer:
    def __init__(self, min_samples: int = 5, max_warning_rate: float = 0.35) -> None:
        self.min_samples = min_samples
        self.max_warning_rate = max_warning_rate

    def analyze(self, rows: list[dict]) -> dict:
        analysis_rows = [self._enrich_row(row) for row in rows if row.get("confirmation_passed") and row.get("realized_return") is not None]
        by_horizon: dict[str, dict] = {}
        candidate_rules: list[dict] = []
        for horizon in ("5m", "15m", "60m", "eod"):
            horizon_rows = [row for row in analysis_rows if row["horizon"] == horizon]
            grouped = self._group_rows(horizon_rows)
            ranked = [self._setup_stats(spec_name, group_key, values) for spec_name, group_key, values in grouped]
            filtered = [item for item in ranked if item["sample_size"] >= self.min_samples]
            filtered.sort(key=lambda item: (item["expectancy_score"], item["average_return"], item["sample_size"]), reverse=True)
            worst = sorted(filtered, key=lambda item: (item["average_return"], item["win_rate"], -item["sample_size"]))[:10]
            top = filtered[:10]
            candidates = [item for item in filtered if self._is_candidate(item)][:10]
            for item in candidates:
                candidate_rules.append(
                    {
                        "horizon": horizon,
                        "setup_definition": item["setup_definition"],
                        "sample_size": item["sample_size"],
                        "win_rate": item["win_rate"],
                        "average_return": item["average_return"],
                        "warning_rate": item["warning_rate"],
                        "why_ranked_well": f"avg_return={item['average_return']:.6f}, sample={item['sample_size']}, stability_gap={item['subperiod_stability_gap']:.6f}",
                        "why_it_might_still_be_unsafe": f"warning_rate={item['warning_rate']:.2%}, granularity_warnings={item['warning_count']}",
                    }
                )
            by_horizon[horizon] = {
                "evaluated_rows": len(horizon_rows),
                "eligible_setups": len(filtered),
                "top_ranked_setups": top,
                "worst_ranked_setups": worst,
            }
        return {
            "config": {"min_samples": self.min_samples, "max_warning_rate": self.max_warning_rate},
            "candidate_decision_rules": candidate_rules,
            "candidate_rule_count": len(candidate_rules),
            "by_horizon": by_horizon,
        }

    def write(self, analysis: dict, output_dir: Path) -> str:
        output_dir.mkdir(parents=True, exist_ok=True)
        path = output_dir / "edge_analysis_report.json"
        path.write_text(json.dumps(analysis, indent=2), encoding="utf-8")
        return str(path)

    def _group_rows(self, rows: list[dict]):
        for spec in GROUPING_SPECS:
            buckets: dict[str, list[dict]] = defaultdict(list)
            for row in rows:
                key = "|".join(f"{field}={row.get(field)}" for field in spec)
                buckets[key].append(row)
            for group_key, values in sorted(buckets.items()):
                yield "+".join(spec), group_key, values

    def _setup_stats(self, spec_name: str, group_key: str, rows: list[dict]) -> dict:
        returns = [float(row["realized_return"]) for row in rows]
        abs_returns = [abs(value) for value in returns]
        warning_count = sum(1 for row in rows if _has_warning(row))
        early, late = _split_halves(rows)
        early_avg = _avg([float(row["realized_return"]) for row in early]) if early else 0.0
        late_avg = _avg([float(row["realized_return"]) for row in late]) if late else 0.0
        warning_rate = warning_count / len(rows) if rows else 0.0
        average_return = _avg(returns)
        return {
            "specification": spec_name,
            "setup_definition": group_key,
            "sample_size": len(rows),
            "win_rate": _win_rate(returns),
            "average_return": average_return,
            "median_return": _median(returns),
            "average_absolute_return": _avg(abs_returns),
            "max_drawdown_proxy": _max_drawdown(returns),
            "sharpe_like": _sharpe_like(returns),
            "warning_rate": warning_rate,
            "warning_count": warning_count,
            "subperiod_first_half_avg": early_avg,
            "subperiod_second_half_avg": late_avg,
            "subperiod_stability_gap": abs(early_avg - late_avg),
            "expectancy_score": average_return * sqrt(len(rows)) * max(0.0, 1.0 - warning_rate),
        }

    def _is_candidate(self, stats: dict) -> bool:
        return (
            stats["sample_size"] >= self.min_samples
            and stats["average_return"] > 0
            and stats["win_rate"] >= 0.5
            and stats["warning_rate"] <= self.max_warning_rate
        )

    def _enrich_row(self, row: dict) -> dict:
        enriched = dict(row)
        enriched["pre_15m_return_bucket"] = _return_bucket(row.get("pre_15m_return"))
        enriched["spy_context_bucket"] = _return_bucket(row.get("spy_pre_15m_return"))
        enriched["vix_context_bucket"] = _return_bucket(row.get("vix_pre_15m_return"))
        enriched["granularity_bucket"] = "downgraded" if row.get("granularity_downgrade_flag") else "native"
        return enriched


def _has_warning(row: dict) -> bool:
    return bool(
        row.get("granularity_downgrade_flag")
        or row.get("coarse_bar_context_flag")
        or row.get("nearest_bar_approximation_flag")
        or int(row.get("missing_context_count") or 0) > 0
    )


def _return_bucket(value) -> str:
    if value is None:
        return "missing"
    numeric = float(value)
    if numeric <= -0.01:
        return "<=-1.0%"
    if numeric <= -0.002:
        return "-1.0% to -0.2%"
    if numeric < 0.002:
        return "-0.2% to 0.2%"
    if numeric < 0.01:
        return "0.2% to 1.0%"
    return ">=1.0%"


def _avg(values: list[float]) -> float:
    return sum(values) / len(values) if values else 0.0


def _median(values: list[float]) -> float:
    ordered = sorted(values)
    if not ordered:
        return 0.0
    midpoint = len(ordered) // 2
    if len(ordered) % 2:
        return ordered[midpoint]
    return (ordered[midpoint - 1] + ordered[midpoint]) / 2.0


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
    drawdown = 0.0
    for value in values:
        equity *= (1.0 + value)
        peak = max(peak, equity)
        drawdown = min(drawdown, (equity / peak) - 1.0)
    return drawdown


def _split_halves(rows: list[dict]) -> tuple[list[dict], list[dict]]:
    ordered = sorted(rows, key=lambda row: row["event_timestamp_utc"])
    midpoint = len(ordered) // 2
    return ordered[:midpoint], ordered[midpoint:]
