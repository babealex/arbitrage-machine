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
        external_candidates = [row for row in external_rows if row["source_type"] != "kalshi" and not row["metadata_json"].get("placeholder")]
        for belief in belief_rows:
            if belief["source_type"] != "kalshi":
                continue
            kalshi_probability = float(belief["value"])
            family = belief["family"]
            event_group = f"{family}:{belief['event_ticker']}"
            for external in external_candidates:
                normalized_external = normalize_external_value(external)
                disagreement_score = abs(kalshi_probability - normalized_external)
                history_key = f"{event_group}:{external['market_ticker']}"
                self._history[history_key].append(disagreement_score)
                if disagreement_score < self.threshold:
                    continue
                rows.append(
                    {
                        "timestamp_utc": belief["timestamp_utc"],
                        "event_group": event_group,
                        "kalshi_reference": belief["market_ticker"],
                        "external_reference": external["market_ticker"],
                        "disagreement_score": disagreement_score,
                        "zscore": rolling_zscore(self._history[history_key]),
                        "metadata_json": {
                            "kalshi_value": kalshi_probability,
                            "external_value": external["value"],
                            "external_normalized": normalized_external,
                            "kalshi_metadata": belief["metadata_json"],
                            "external_metadata": external["metadata_json"],
                        },
                    }
                )
        return DisagreementResult(rows=rows)


def normalize_external_value(row: dict) -> float:
    metadata = row.get("metadata_json", {})
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
