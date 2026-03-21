from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Callable


HORIZONS_SECONDS = (30, 60, 300, 900)
MAX_RETRIES_PER_HORIZON = 3


@dataclass(slots=True)
class DueOutcomeUpdate:
    row_id: int
    outcome: dict[str, Any]
    due_horizons: list[int]


class OutcomeObservationError(RuntimeError):
    def __init__(self, reason: str, observation: dict[str, Any]) -> None:
        super().__init__(reason)
        self.reason = reason
        self.observation = observation


class OutcomeTracker:
    def build_outcomes(self, disagreement_rows: list[dict]) -> list[dict[str, Any]]:
        outcomes: list[dict[str, Any]] = []
        for row in disagreement_rows:
            metadata = dict(row.get("metadata_json", {}))
            event_group = str(row["event_group"])
            family, event_ticker = _parse_event_group(event_group)
            detected_at = _parse_timestamp(str(row["timestamp_utc"]))
            tracking_metadata = {
                "event_group": event_group,
                "kalshi_reference": row["kalshi_reference"],
                "external_reference": row["external_reference"],
                "zscore": row["zscore"],
                "latest_value": metadata.get("kalshi_value"),
                "latest_value_timestamp_utc": row["timestamp_utc"],
                "latest_delta": 0.0,
                "scheduler": {
                    str(horizon): {
                        "attempts": 0,
                        "errors": 0,
                        "retry_count": 0,
                        "last_attempt_utc": None,
                        "last_success_utc": None,
                        "last_error": None,
                        "populated_at_utc": None,
                        "lateness_seconds": None,
                        "terminal_status": "not_due",
                        "terminal_reason": None,
                    }
                    for horizon in HORIZONS_SECONDS
                },
            }
            outcomes.append(
                {
                    "timestamp_detected": row["timestamp_utc"],
                    "event_ticker": event_ticker,
                    "family": family,
                    "initial_value": float(metadata.get("kalshi_value", 0.0)),
                    "disagreement_score": float(row["disagreement_score"]),
                    "metadata_json": tracking_metadata,
                    "due_at_30s": (detected_at + timedelta(seconds=30)).isoformat(),
                    "due_at_60s": (detected_at + timedelta(seconds=60)).isoformat(),
                    "due_at_300s": (detected_at + timedelta(seconds=300)).isoformat(),
                    "due_at_900s": (detected_at + timedelta(seconds=900)).isoformat(),
                    "value_after_30s": None,
                    "value_after_60s": None,
                    "value_after_300s": None,
                    "value_after_900s": None,
                    "max_favorable_move": 0.0,
                    "max_adverse_move": 0.0,
                    "resolved_direction": "unresolved",
                }
            )
        return outcomes

    def process_due_rows(
        self,
        due_rows: list[dict[str, Any]],
        fetch_observation: Callable[[str], dict[str, Any]],
        observed_at: datetime | None = None,
    ) -> list[DueOutcomeUpdate]:
        now = observed_at or datetime.now(timezone.utc)
        updates: list[DueOutcomeUpdate] = []
        for row in due_rows:
            outcome = dict(row)
            outcome["metadata_json"] = _metadata_dict(row["metadata_json"])
            due_horizons = _due_horizons(outcome, now)
            if not due_horizons:
                continue
            tracker = outcome["metadata_json"].setdefault("scheduler", {})
            market_ticker = str(outcome["metadata_json"].get("kalshi_reference", ""))
            try:
                observation = fetch_observation(market_ticker)
                observation_result = resolve_observation(observation)
                current_value = float(observation_result["value"])
                delta = current_value - float(outcome["initial_value"])
                outcome["max_favorable_move"] = max(float(outcome.get("max_favorable_move") or 0.0), delta)
                outcome["max_adverse_move"] = min(float(outcome.get("max_adverse_move") or 0.0), delta)
                outcome["metadata_json"]["latest_value"] = current_value
                outcome["metadata_json"]["latest_value_timestamp_utc"] = now.isoformat()
                outcome["metadata_json"]["latest_delta"] = delta
                for horizon in due_horizons:
                    horizon_tracker = tracker.setdefault(str(horizon), _empty_horizon_tracker())
                    horizon_tracker["attempts"] = int(horizon_tracker.get("attempts", 0)) + 1
                    horizon_tracker["retry_count"] = max(0, horizon_tracker["attempts"] - 1)
                    horizon_tracker["last_attempt_utc"] = now.isoformat()
                    horizon_tracker["last_success_utc"] = now.isoformat()
                    horizon_tracker["last_error"] = None
                    horizon_tracker["terminal_reason"] = None
                    field_name = f"value_after_{horizon}s"
                    if outcome.get(field_name) is None:
                        outcome[field_name] = current_value
                    due_at = _parse_timestamp(str(outcome[f"due_at_{horizon}s"]))
                    horizon_tracker["populated_at_utc"] = now.isoformat()
                    horizon_tracker["lateness_seconds"] = max(0.0, (now - due_at).total_seconds())
                    horizon_tracker["terminal_status"] = "populated"
                    horizon_tracker["terminal_reason"] = observation_result["terminal_reason"]
                    _attach_observation(horizon_tracker, observation_result)
                outcome["resolved_direction"] = _resolved_direction(outcome)
            except Exception as exc:
                for horizon in due_horizons:
                    horizon_tracker = tracker.setdefault(str(horizon), _empty_horizon_tracker())
                    horizon_tracker["attempts"] = int(horizon_tracker.get("attempts", 0)) + 1
                    horizon_tracker["retry_count"] = max(0, horizon_tracker["attempts"] - 1)
                    horizon_tracker["errors"] = int(horizon_tracker.get("errors", 0)) + 1
                    horizon_tracker["last_attempt_utc"] = now.isoformat()
                    horizon_tracker["last_error"] = str(exc)
                    reason = classify_error(exc)
                    horizon_tracker["terminal_reason"] = reason
                    horizon_tracker["observability_status"] = "unobservable"
                    horizon_tracker["observability_reason"] = reason
                    if isinstance(exc, OutcomeObservationError):
                        _attach_observation(horizon_tracker, _observation_fields(exc.observation) | {"observation_source": "unavailable", "observability_status": "unobservable", "observability_reason": reason})
                    if horizon_tracker["attempts"] >= MAX_RETRIES_PER_HORIZON:
                        horizon_tracker["terminal_status"] = "terminal_failed"
                    else:
                        horizon_tracker["terminal_status"] = "retrying"
            updates.append(DueOutcomeUpdate(row_id=int(row["id"]), outcome=outcome, due_horizons=due_horizons))
        return updates


def _parse_event_group(event_group: str) -> tuple[str, str]:
    if ":" in event_group:
        return tuple(event_group.split(":", 1))
    return "unknown", event_group


def _parse_timestamp(value: str) -> datetime:
    parsed = datetime.fromisoformat(value)
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _metadata_dict(raw: Any) -> dict[str, Any]:
    if isinstance(raw, dict):
        return raw
    import json

    try:
        return json.loads(raw) if raw else {}
    except Exception:
        return {}


def _empty_horizon_tracker() -> dict[str, Any]:
    return {
        "attempts": 0,
        "errors": 0,
        "retry_count": 0,
        "last_attempt_utc": None,
        "last_success_utc": None,
        "last_error": None,
        "populated_at_utc": None,
        "lateness_seconds": None,
        "terminal_status": "not_due",
        "terminal_reason": None,
        "observation_source": None,
        "observed_yes_bid": None,
        "observed_no_bid": None,
        "observed_yes_ask_inferred": None,
        "observed_no_ask_inferred": None,
        "observed_spread": None,
        "observed_has_two_sided_quote": False,
        "observed_last_trade_price": None,
        "observed_last_trade_ts": None,
        "observed_quote_age_seconds": None,
        "observability_status": "not_due",
        "observability_reason": None,
    }


def _due_horizons(outcome: dict[str, Any], now: datetime) -> list[int]:
    due: list[int] = []
    tracker = outcome.get("metadata_json", {}).get("scheduler", {})
    for horizon in HORIZONS_SECONDS:
        due_at = outcome.get(f"due_at_{horizon}s")
        value = outcome.get(f"value_after_{horizon}s")
        if value is not None or not due_at:
            continue
        horizon_tracker = tracker.get(str(horizon), {})
        if horizon_tracker.get("terminal_status") == "terminal_failed":
            continue
        if _parse_timestamp(str(due_at)) <= now:
            if horizon_tracker.get("terminal_status") not in {"populated", "retrying", "terminal_failed"}:
                horizon_tracker["terminal_status"] = "due"
            due.append(horizon)
        elif horizon_tracker.get("terminal_status") == "not_due":
            horizon_tracker["terminal_status"] = "not_due"
    return due


def _resolved_direction(outcome: dict[str, Any]) -> str:
    initial_value = float(outcome["initial_value"])
    for field_name in ("value_after_900s", "value_after_300s", "value_after_60s", "value_after_30s"):
        observed = outcome.get(field_name)
        if observed is None:
            continue
        if float(observed) > initial_value:
            return "up"
        if float(observed) < initial_value:
            return "down"
        return "flat"
    return "unresolved"


def classify_error(exc: Exception) -> str:
    message = str(exc).lower()
    if message in {
        "midpoint_missing_but_executable_quote_available",
        "no_two_sided_quote",
        "no_orderbook_depth",
        "no_recent_trade",
        "liquidity_unobservable",
    }:
        return message
    if "404" in message or "not found" in message:
        return "market_not_found"
    if "timeout" in message:
        return "timeout"
    if "connection" in message or "socket" in message or "dns" in message:
        return "connection_error"
    if "403" in message or "forbidden" in message or "401" in message or "unauthorized" in message:
        return "permission_error"
    return "unknown_error"


def resolve_observation(observation: dict[str, Any]) -> dict[str, Any]:
    midpoint = observation.get("midpoint_probability")
    if midpoint is not None:
        return {
            "value": float(midpoint),
            "observation_source": "midpoint",
            "observability_status": "observable",
            "observability_reason": None,
            "terminal_reason": None,
            **_observation_fields(observation),
        }
    if observation.get("observed_has_two_sided_quote"):
        yes_bid = observation.get("observed_yes_bid")
        yes_ask = observation.get("observed_yes_ask_inferred")
        executable_value = (float(yes_bid) + float(yes_ask)) / 200.0
        return {
            "value": executable_value,
            "observation_source": "executable_quote",
            "observability_status": "observable",
            "observability_reason": "midpoint_missing_but_executable_quote_available",
            "terminal_reason": "midpoint_missing_but_executable_quote_available",
            **_observation_fields(observation),
        }
    last_trade = observation.get("observed_last_trade_price")
    if last_trade is not None:
        return {
            "value": float(last_trade) / 100.0,
            "observation_source": "last_trade",
            "observability_status": "observable",
            "observability_reason": None,
            "terminal_reason": None,
            **_observation_fields(observation),
        }
    reason = classify_observability_failure(observation)
    raise OutcomeObservationError(reason, observation)


def classify_observability_failure(observation: dict[str, Any]) -> str:
    yes_bid = observation.get("observed_yes_bid")
    no_bid = observation.get("observed_no_bid")
    if yes_bid is None and no_bid is None:
        return "no_orderbook_depth"
    if not observation.get("observed_has_two_sided_quote"):
        if observation.get("observed_last_trade_price") is None:
            return "liquidity_unobservable"
        return "no_two_sided_quote"
    if observation.get("observed_last_trade_price") is None:
        return "no_recent_trade"
    return "liquidity_unobservable"


def _observation_fields(observation: dict[str, Any]) -> dict[str, Any]:
    return {
        "observed_yes_bid": observation.get("observed_yes_bid"),
        "observed_no_bid": observation.get("observed_no_bid"),
        "observed_yes_ask_inferred": observation.get("observed_yes_ask_inferred"),
        "observed_no_ask_inferred": observation.get("observed_no_ask_inferred"),
        "observed_spread": observation.get("observed_spread"),
        "observed_has_two_sided_quote": bool(observation.get("observed_has_two_sided_quote")),
        "observed_last_trade_price": observation.get("observed_last_trade_price"),
        "observed_last_trade_ts": observation.get("observed_last_trade_ts"),
        "observed_quote_age_seconds": observation.get("observed_quote_age_seconds"),
    }


def _attach_observation(horizon_tracker: dict[str, Any], observation_result: dict[str, Any]) -> None:
    for key in (
        "observation_source",
        "observed_yes_bid",
        "observed_no_bid",
        "observed_yes_ask_inferred",
        "observed_no_ask_inferred",
        "observed_spread",
        "observed_has_two_sided_quote",
        "observed_last_trade_price",
        "observed_last_trade_ts",
        "observed_quote_age_seconds",
        "observability_status",
        "observability_reason",
    ):
        horizon_tracker[key] = observation_result.get(key)
