from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any

from app.models import ExternalDistribution, Market
from app.options.models import OptionStateVector
from app.strategies.cross_market_snapshot import midpoint_probability


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def normalized_underlying_key(raw: str) -> str:
    value = str(raw or "").strip().lower()
    if any(token in value for token in ("fed", "fomc", "tbill", "rate")):
        return "macro:fed_policy"
    if "cpi" in value or "inflation" in value:
        return "macro:inflation"
    if value.startswith("^") or value in {"spy", "qqq", "tlt", "iwm", "efa", "eem", "gld", "dbc"}:
        return f"asset:{value.lstrip('^')}"
    return f"market:{value or 'unknown'}"


@dataclass(frozen=True, slots=True)
class NormalizedBeliefObject:
    timestamp_utc: str
    source_key: str
    event_key: str
    normalized_underlying_key: str
    family: str
    event_ticker: str
    market_ticker: str
    source_type: str
    object_type: str
    comparison_metric: str
    value: float
    confidence: float
    cost_estimate_bps: float
    payload_summary_json: dict[str, Any] = field(default_factory=dict)
    belief_summary_json: dict[str, Any] = field(default_factory=dict)
    quality_flags_json: dict[str, Any] = field(default_factory=dict)
    bid: float | None = None
    ask: float | None = None
    size_bid: int | None = None
    size_ask: int | None = None
    metadata_json: dict[str, Any] = field(default_factory=dict)

    def to_snapshot_row(self) -> dict[str, Any]:
        return {
            "timestamp_utc": self.timestamp_utc,
            "source_key": self.source_key,
            "event_key": self.event_key,
            "normalized_underlying_key": self.normalized_underlying_key,
            "family": self.family,
            "event_ticker": self.event_ticker,
            "market_ticker": self.market_ticker,
            "source_type": self.source_type,
            "object_type": self.object_type,
            "comparison_metric": self.comparison_metric,
            "value": self.value,
            "confidence": self.confidence,
            "cost_estimate_bps": self.cost_estimate_bps,
            "payload_summary_json": self.payload_summary_json,
            "belief_summary_json": self.belief_summary_json,
            "quality_flags_json": self.quality_flags_json,
            "bid": self.bid,
            "ask": self.ask,
            "size_bid": self.size_bid,
            "size_ask": self.size_ask,
            "metadata_json": self.metadata_json,
        }


def kalshi_market_belief(market: Market, family: str) -> NormalizedBeliefObject | None:
    probability = midpoint_probability(market)
    if probability is None:
        return None
    quote_diag = dict(market.metadata.get("quote_diagnostics", {}))
    confidence = 0.35
    if quote_diag.get("orderbook_snapshot_exists"):
        confidence += 0.25
    if quote_diag.get("best_yes_bid_size") and quote_diag.get("best_yes_ask_size"):
        confidence += 0.20
    return NormalizedBeliefObject(
        timestamp_utc=_utc_now_iso(),
        source_key="kalshi_event_market",
        event_key=market.event_ticker,
        normalized_underlying_key=normalized_underlying_key(family),
        family=family,
        event_ticker=market.event_ticker,
        market_ticker=market.ticker,
        source_type="kalshi",
        object_type="event_market_probability",
        comparison_metric="probability",
        value=float(probability),
        confidence=min(confidence, 0.95),
        cost_estimate_bps=35.0,
        payload_summary_json={
            "title": market.title,
            "status": market.status,
            "strike": market.strike,
        },
        belief_summary_json={
            "implied_probability": float(probability),
            "minutes_to_event": minutes_to_event(market.expiry),
        },
        quality_flags_json={
            "has_two_sided_quote": market.yes_bid is not None and market.yes_ask is not None,
            "orderbook_snapshot_exists": bool(quote_diag.get("orderbook_snapshot_exists")),
            "data_placeholder": False,
        },
        bid=market.yes_bid,
        ask=market.yes_ask,
        size_bid=int(quote_diag.get("best_yes_bid_size") or 0),
        size_ask=int(quote_diag.get("best_yes_ask_size") or 0),
        metadata_json={
            "quote_diagnostics": quote_diag,
            "session_hour_bucket": hour_bucket(),
        },
    )


def fed_futures_beliefs(distributions: dict[str, ExternalDistribution], *, timestamp_utc: str | None = None) -> list[NormalizedBeliefObject]:
    timestamp = timestamp_utc or _utc_now_iso()
    rows: list[NormalizedBeliefObject] = []
    for key, distribution in sorted(distributions.items()):
        implied_mean = sum(Decimal(str(rate)) * Decimal(str(prob)) for rate, prob in distribution.values.items())
        top_bucket = max(distribution.values.items(), key=lambda item: item[1])[0]
        rows.append(
            NormalizedBeliefObject(
                timestamp_utc=timestamp,
                source_key="fed_futures",
                event_key=f"fed:{key}",
                normalized_underlying_key="macro:fed_policy",
                family="FED",
                event_ticker=f"fed:{key}",
                market_ticker=f"fed:{key}",
                source_type="fed_futures",
                object_type="policy_path_summary",
                comparison_metric="probability",
                value=float(max(distribution.values.values())),
                confidence=0.80,
                cost_estimate_bps=8.0,
                payload_summary_json={"distribution_key": key},
                belief_summary_json={
                    "implied_mean_rate": float(implied_mean),
                    "top_bucket_rate": float(top_bucket),
                    "top_bucket_probability": float(max(distribution.values.values())),
                },
                quality_flags_json={
                    "data_placeholder": False,
                    "stale": False,
                    "distribution_points": len(distribution.values),
                },
                metadata_json={"distribution": {str(rate): float(prob) for rate, prob in distribution.values.items()}},
            )
        )
    return rows


def options_state_belief(state: OptionStateVector) -> NormalizedBeliefObject:
    event_premium = Decimal("0") if state.event_premium_proxy is None else state.event_premium_proxy
    atm_iv_near = Decimal("0") if state.atm_iv_near is None else state.atm_iv_near
    atm_iv_next = Decimal("0") if state.atm_iv_next is None else state.atm_iv_next
    term_slope = Decimal("0") if state.atm_iv_term_slope is None else state.atm_iv_term_slope
    comparable_value = float(max(Decimal("0"), min(Decimal("1"), event_premium + Decimal("0.5"))))
    return NormalizedBeliefObject(
        timestamp_utc=state.observed_at.astimezone(timezone.utc).isoformat(),
        source_key="options_surface",
        event_key=f"options:{state.underlying}",
        normalized_underlying_key=normalized_underlying_key(state.underlying),
        family=state.underlying,
        event_ticker=f"options:{state.underlying}",
        market_ticker=state.underlying,
        source_type="options",
        object_type="options_state_summary",
        comparison_metric="tail_proxy",
        value=comparable_value,
        confidence=0.70,
        cost_estimate_bps=25.0,
        payload_summary_json={"source": state.source},
        belief_summary_json={
            "atm_iv_near": float(atm_iv_near),
            "atm_iv_next": float(atm_iv_next),
            "atm_iv_term_slope": float(term_slope),
            "event_premium_proxy": float(event_premium),
            "vrp_proxy_near": None if state.vrp_proxy_near is None else float(state.vrp_proxy_near),
        },
        quality_flags_json={
            "data_placeholder": False,
            "total_variance_term_monotonic": bool(state.total_variance_term_monotonic),
        },
        metadata_json={},
    )


def proxy_belief(row: dict[str, Any]) -> NormalizedBeliefObject:
    metadata = dict(row.get("metadata_json", {}))
    rolling_zscore = float(metadata.get("rolling_zscore", 0.0))
    normalized = max(0.0, min(1.0, (rolling_zscore + 3.0) / 6.0))
    return NormalizedBeliefObject(
        timestamp_utc=str(row["timestamp_utc"]),
        source_key=str(row["source_type"]),
        event_key=str(row["event_ticker"]),
        normalized_underlying_key=normalized_underlying_key(str(row["event_ticker"])),
        family=str(row["family"]),
        event_ticker=str(row["event_ticker"]),
        market_ticker=str(row["market_ticker"]),
        source_type=str(row["source_type"]),
        object_type=str(row["object_type"]),
        comparison_metric="tail_proxy",
        value=normalized,
        confidence=0.20 if metadata.get("placeholder") else 0.55,
        cost_estimate_bps=0.0,
        payload_summary_json={"raw_value": row["value"]},
        belief_summary_json={"normalized_proxy_value": normalized, "rolling_zscore": rolling_zscore},
        quality_flags_json={"data_placeholder": bool(metadata.get("placeholder"))},
        metadata_json=metadata,
    )


def minutes_to_event(expiry: datetime | None) -> int | None:
    if expiry is None:
        return None
    now = datetime.now(timezone.utc)
    delta = expiry.astimezone(timezone.utc) - now
    return max(0, int(delta.total_seconds() // 60))


def hour_bucket(ts: datetime | None = None) -> str:
    current = ts or datetime.now(timezone.utc)
    return f"{current.hour:02d}:00"
