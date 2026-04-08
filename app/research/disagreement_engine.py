from __future__ import annotations

import json
from collections import defaultdict, deque
from dataclasses import dataclass


@dataclass(slots=True)
class DisagreementResult:
    rows: list[dict]


class DisagreementEngine:
    def __init__(self, threshold: float, history_limit: int = 128) -> None:
        self.threshold = threshold
        self._history: dict[str, deque[float]] = defaultdict(lambda: deque(maxlen=history_limit))

    def build(self, belief_rows: list[dict], external_rows: list[dict]) -> DisagreementResult:
        rows: list[dict] = []
        candidates = [
            row
            for row in [*belief_rows, *external_rows]
            if not row.get("quality_flags_json", {}).get("data_placeholder")
        ]
        groups: dict[tuple[str, str], list[dict]] = defaultdict(list)
        for row in candidates:
            underlying = str(row.get("normalized_underlying_key") or "")
            metric = str(row.get("comparison_metric") or "")
            if not underlying or not metric:
                continue
            groups[(underlying, metric)].append(row)
        for (underlying, metric), group_rows in groups.items():
            for index, left in enumerate(group_rows):
                for right in group_rows[index + 1 :]:
                    if left["source_key"] == right["source_key"]:
                        continue
                    left_value = float(left["value"])
                    right_value = float(right["value"])
                    gross_disagreement = abs(left_value - right_value)
                    fees_bps = max(float(left.get("cost_estimate_bps") or 0.0), float(right.get("cost_estimate_bps") or 0.0))
                    slippage_bps = 12.0 if "options" in {left["source_type"], right["source_type"]} else 4.0
                    execution_uncertainty_bps = 8.0
                    total_cost = fees_bps + slippage_bps + execution_uncertainty_bps
                    net_disagreement = gross_disagreement - (total_cost / 10000.0)
                    event_key = str(left.get("event_key") or left.get("event_ticker") or right.get("event_key") or right.get("event_ticker"))
                    history_key = f"{underlying}:{metric}:{left['source_key']}:{right['source_key']}"
                    self._history[history_key].append(net_disagreement)
                    if net_disagreement < self.threshold:
                        continue
                    disagreement_score = gross_disagreement
                    event_group = f"{underlying}:{event_key}"
                    tradeable = net_disagreement > self.threshold
                    if not tradeable:
                        continue
                    rows.append(
                        {
                            "timestamp_utc": str(left["timestamp_utc"]),
                            "normalized_underlying_key": underlying,
                            "left_source_key": left["source_key"],
                            "right_source_key": right["source_key"],
                            "gross_disagreement": gross_disagreement,
                            "cost_estimate_bps": total_cost,
                            "net_disagreement": net_disagreement,
                            "tradeable": True,
                            "event_group": event_group,
                            "kalshi_reference": left["market_ticker"],
                            "external_reference": right["market_ticker"],
                            "disagreement_score": disagreement_score,
                            "zscore": rolling_zscore(self._history[history_key]),
                            "metadata_json": {
                                "event_key": event_key,
                                "comparison_metric": metric,
                                "left_source_key": left["source_key"],
                                "right_source_key": right["source_key"],
                                "left_value": left_value,
                                "right_value": right_value,
                                "left_belief_summary": left.get("belief_summary_json", {}),
                                "right_belief_summary": right.get("belief_summary_json", {}),
                                "fees_bps": fees_bps,
                                "slippage_bps": slippage_bps,
                                "execution_uncertainty_bps": execution_uncertainty_bps,
                                "net_disagreement": net_disagreement,
                            },
                        }
                    )
        return DisagreementResult(rows=rows)


def normalize_external_value(row: dict) -> float:
    if row.get("comparison_metric") == "probability":
        return float(row["value"])
    metadata = row.get("belief_summary_json", row.get("metadata_json", {}))
    zscore = float(metadata.get("rolling_zscore", 0.0))
    bounded = max(-3.0, min(3.0, zscore))
    return (bounded + 3.0) / 6.0


def rolling_zscore(history: deque[float]) -> float:
    if len(history) < 2:
        return 0.0
    values = list(history)
    mean = sum(values) / len(values)
    variance = sum((value - mean) ** 2 for value in values) / len(values)
    std = variance ** 0.5
    if std == 0:
        return 0.0
    return (values[-1] - mean) / std
