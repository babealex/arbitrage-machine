from __future__ import annotations

import csv
import json
from collections import Counter
from pathlib import Path

from app.replay.data_models import NormalizedBar
from app.replay.features import FeatureExtractionResult, ReplayFeatureExtractor


OUTCOME_FIELD_ORDER = [
    "event_id",
    "event_timestamp_utc",
    "event_type",
    "source",
    "severity",
    "severity_bucket",
    "confirmation_count",
    "confirmation_count_bucket",
    "headline",
    "mapped_symbol",
    "trade_direction",
    "confirmation_passed",
    "session_bucket",
    "session_profile",
    "event_within_tradable_session",
    "session_boundary_flag",
    "entry_blocked_reason",
    "weekday",
    "pre_5m_return",
    "pre_15m_return",
    "pre_60m_return",
    "intraday_return_so_far",
    "rolling_volatility_60m",
    "feature_window_bar_count",
    "time_of_day_utc",
    "spy_pre_15m_return",
    "vix_pre_15m_return",
    "tlt_pre_15m_return",
    "clf_pre_15m_return",
    "entry_bar_timestamp_utc",
    "exit_bar_timestamp_utc",
    "actual_provider_interval_returned",
    "granularity_downgrade_flag",
    "coarse_bar_context_flag",
    "nearest_bar_approximation_flag",
    "feature_completeness_flag",
    "missing_context_count",
    "data_quality_flags",
    "horizon",
    "entry_price",
    "exit_price",
    "realized_return",
    "win",
    "absolute_return",
    "max_favorable_excursion",
    "max_adverse_excursion",
    "bars_used_count",
    "replay_execution_notes",
]


class ReplayOutcomeDatasetBuilder:
    def __init__(self, feature_extractor: ReplayFeatureExtractor) -> None:
        self.feature_extractor = feature_extractor

    def build(self, results: list[dict]) -> tuple[list[dict], dict]:
        rows: list[dict] = []
        skipped = Counter()
        for result in results:
            extraction = self.feature_extractor.extract(result)
            if extraction is None:
                skipped["missing_simulation"] += 1
                continue
            rows.extend(self._rows_for_result(result, extraction))
        rows = sorted(rows, key=lambda row: (row["event_timestamp_utc"], row["event_id"], row["mapped_symbol"], row["horizon"]))
        summary = {
            "row_count": len(rows),
            "skipped": dict(sorted(skipped.items())),
            "confirmed_row_count": sum(1 for row in rows if row["confirmation_passed"]),
            "horizon_counts": dict(sorted(Counter(row["horizon"] for row in rows).items())),
        }
        return rows, summary

    def write(self, rows: list[dict], output_dir: Path) -> dict:
        output_dir.mkdir(parents=True, exist_ok=True)
        json_path = output_dir / "event_outcomes.json"
        csv_path = output_dir / "event_outcomes.csv"
        json_path.write_text(json.dumps(rows, indent=2), encoding="utf-8")
        with csv_path.open("w", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(handle, fieldnames=OUTCOME_FIELD_ORDER, extrasaction="ignore")
            writer.writeheader()
            for row in rows:
                writer.writerow({field: row.get(field) for field in OUTCOME_FIELD_ORDER})
        return {"json_path": str(json_path), "csv_path": str(csv_path)}

    def _rows_for_result(self, result: dict, extraction: FeatureExtractionResult) -> list[dict]:
        rows: list[dict] = []
        simulation = result["simulation"]
        for horizon in ("5m", "15m", "60m", "eod"):
            exit_snapshot = _exit_snapshot(extraction, horizon)
            path_bars = _path_bars(extraction.bars, extraction.entry_bar, exit_snapshot.timestamp_utc if exit_snapshot else None)
            mfe, mae = _excursions(simulation["direction"], simulation["entry_price"], path_bars)
            realized_return = simulation["returns"].get(horizon)
            notes = []
            if not result["confirmation"]["passed"]:
                notes.append("confirmation_failed")
            if extraction.base_features["granularity_downgrade_flag"]:
                notes.append("coarse_provider_bars")
            if extraction.base_features["nearest_bar_approximation_flag"]:
                notes.append("nearest_bar_approximation")
            rows.append(
                {
                    **extraction.base_features,
                    "horizon": horizon,
                    "entry_price": simulation["entry_price"],
                    "exit_price": simulation["exit_prices"].get(horizon),
                    "exit_bar_timestamp_utc": exit_snapshot.timestamp_utc.isoformat() if exit_snapshot and exit_snapshot.timestamp_utc else None,
                    "realized_return": realized_return,
                    "win": bool(realized_return is not None and realized_return > 0),
                    "absolute_return": abs(realized_return) if realized_return is not None else None,
                    "max_favorable_excursion": mfe,
                    "max_adverse_excursion": mae,
                    "bars_used_count": len(path_bars),
                    "replay_execution_notes": "|".join(notes),
                }
            )
        return rows


def _exit_snapshot(extraction: FeatureExtractionResult, horizon: str):
    if horizon == "5m":
        return extraction.snapshot.plus_5m
    if horizon == "15m":
        return extraction.snapshot.plus_15m
    if horizon == "60m":
        return extraction.snapshot.plus_60m
    return extraction.snapshot.eod


def _path_bars(bars: list[NormalizedBar], entry_bar: NormalizedBar | None, exit_ts) -> list[NormalizedBar]:
    if entry_bar is None or exit_ts is None:
        return []
    return [bar for bar in bars if entry_bar.timestamp_utc <= bar.timestamp_utc <= exit_ts]


def _excursions(direction: str, entry_price: float, path_bars: list[NormalizedBar]) -> tuple[float | None, float | None]:
    if not path_bars or entry_price <= 0:
        return None, None
    returns = [_directional_return(direction, entry_price, bar.close) for bar in path_bars]
    returns = [value for value in returns if value is not None]
    if not returns:
        return None, None
    return max(returns), min(returns)


def _directional_return(direction: str, entry_price: float, exit_price: float | None) -> float | None:
    if exit_price is None or entry_price <= 0:
        return None
    if direction == "buy":
        return round((exit_price / entry_price) - 1.0, 8)
    return round((entry_price / exit_price) - 1.0, 8)
