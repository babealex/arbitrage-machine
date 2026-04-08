from __future__ import annotations

from collections import defaultdict
from decimal import Decimal

from app.core.money import to_decimal


def summarize_convexity_outcomes(rows: list[dict]) -> dict:
    if not rows:
        return {
            "total_trades": 0,
            "win_rate": "0",
            "avg_realized_multiple": "0",
            "median_realized_multiple": "0",
            "avg_loss_size": "0",
            "avg_winner_size": "0",
            "max_winner_multiple": "0",
            "pct_ge_2x": "0",
            "pct_ge_3x": "0",
            "pct_ge_5x": "0",
            "pct_ge_10x": "0",
            "pct_near_worthless": "0",
            "pnl_by_setup_type": {},
            "pnl_by_structure_type": {},
            "hit_rate_by_setup_type": {},
            "avg_ev_forecast_vs_realized_by_setup_type": {},
        }
    multiples = sorted(to_decimal(row["realized_multiple_decimal"]) for row in rows)
    pnls = [to_decimal(row["realized_pnl_decimal"]) for row in rows]
    wins = [pnl for pnl in pnls if pnl > 0]
    losses = [pnl for pnl in pnls if pnl <= 0]
    pnl_by_setup = defaultdict(Decimal)
    pnl_by_structure = defaultdict(Decimal)
    setup_hits = defaultdict(lambda: {"wins": 0, "count": 0})
    forecast_vs_realized = defaultdict(lambda: {"forecast": Decimal("0"), "realized": Decimal("0"), "count": 0})
    for row in rows:
        metadata = row.get("metadata_json")
        setup_type = "unknown" if not metadata else _metadata_dict(metadata).get("setup_type", "unknown")
        pnl = to_decimal(row["realized_pnl_decimal"])
        pnl_by_setup[setup_type] += pnl
        pnl_by_structure[str(row["structure_type"])] += pnl
        setup_hits[setup_type]["count"] += 1
        if pnl > 0:
            setup_hits[setup_type]["wins"] += 1
        expected = Decimal("0") if not metadata else to_decimal(_metadata_dict(metadata).get("expected_value_after_costs", "0"))
        forecast_vs_realized[setup_type]["forecast"] += expected
        forecast_vs_realized[setup_type]["realized"] += pnl
        forecast_vs_realized[setup_type]["count"] += 1
    return {
        "total_trades": len(rows),
        "win_rate": _ratio_str(len(wins), len(rows)),
        "avg_realized_multiple": _avg_str(multiples),
        "median_realized_multiple": format(multiples[len(multiples) // 2], "f"),
        "avg_loss_size": _avg_str([abs(item) for item in losses]),
        "avg_winner_size": _avg_str(wins),
        "max_winner_multiple": format(max(multiples), "f"),
        "pct_ge_2x": _ratio_str(sum(1 for item in multiples if item >= Decimal("2")), len(multiples)),
        "pct_ge_3x": _ratio_str(sum(1 for item in multiples if item >= Decimal("3")), len(multiples)),
        "pct_ge_5x": _ratio_str(sum(1 for item in multiples if item >= Decimal("5")), len(multiples)),
        "pct_ge_10x": _ratio_str(sum(1 for item in multiples if item >= Decimal("10")), len(multiples)),
        "pct_near_worthless": _ratio_str(sum(1 for item in multiples if item <= Decimal("-0.8")), len(multiples)),
        "pnl_by_setup_type": {key: format(value, "f") for key, value in pnl_by_setup.items()},
        "pnl_by_structure_type": {key: format(value, "f") for key, value in pnl_by_structure.items()},
        "hit_rate_by_setup_type": {
            key: _ratio_str(value["wins"], value["count"])
            for key, value in setup_hits.items()
        },
        "avg_ev_forecast_vs_realized_by_setup_type": {
            key: {
                "forecast": format(value["forecast"] / Decimal(value["count"]), "f"),
                "realized": format(value["realized"] / Decimal(value["count"]), "f"),
            }
            for key, value in forecast_vs_realized.items()
            if value["count"] > 0
        },
    }


def compare_expected_vs_realized(candidates: list[dict], outcomes: list[dict]) -> dict:
    by_candidate = {str(row["candidate_id"]): row for row in candidates}
    scored: list[tuple[Decimal, Decimal, Decimal, str]] = []
    for outcome in outcomes:
        candidate = by_candidate.get(str(outcome["candidate_id"]))
        if candidate is None:
            continue
        metadata = _metadata_dict(candidate.get("metadata_json"))
        scored.append(
            (
                to_decimal(candidate["convex_score_decimal"]),
                to_decimal(candidate["expected_value_after_costs_decimal"]),
                to_decimal(outcome["realized_pnl_decimal"]),
                metadata.get("setup_type", str(candidate["setup_type"])),
            )
        )
    if not scored:
        return {"score_buckets": {}, "move_edge_positive_outperformance": "0", "big_winner_setup_types": {}}
    high = [item for item in scored if item[0] >= Decimal("5")]
    low = [item for item in scored if item[0] < Decimal("5")]
    big_winners = defaultdict(int)
    for _, _, realized, setup in scored:
        if realized >= Decimal("0") and realized >= Decimal("100"):
            big_winners[setup] += 1
    return {
        "score_buckets": {
            "high_score_avg_realized": _avg_str([item[2] for item in high]),
            "low_score_avg_realized": _avg_str([item[2] for item in low]),
        },
        "move_edge_positive_outperformance": _ratio_str(sum(1 for _, expected, realized, _ in scored if expected > 0 and realized > 0), len(scored)),
        "big_winner_setup_types": dict(big_winners),
    }


def _avg_str(values: list[Decimal]) -> str:
    if not values:
        return "0"
    return format(sum(values) / Decimal(len(values)), "f")


def _ratio_str(numerator: int, denominator: int) -> str:
    if denominator <= 0:
        return "0"
    return format(Decimal(numerator) / Decimal(denominator), "f")


def _metadata_dict(raw) -> dict:
    if isinstance(raw, dict):
        return raw
    import json

    try:
        return json.loads(raw or "{}")
    except Exception:
        return {}
