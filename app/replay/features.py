from __future__ import annotations

import hashlib
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from math import sqrt

from app.market_sessions import is_submission_allowed, session_profile, session_start_utc
from app.replay.data_models import NormalizedBar, parse_utc
from app.replay.market_data import HistoricalMarketData, SymbolReplaySnapshot


SESSION_BUCKETS = (
    (0, 12, "overnight"),
    (12, 17, "europe_us_overlap"),
    (17, 21, "us_session"),
    (21, 24, "late_session"),
)
CONTEXT_SYMBOLS = ("SPY", "^VIX", "TLT", "CL=F")


@dataclass(slots=True)
class FeatureExtractionResult:
    base_features: dict
    snapshot: SymbolReplaySnapshot
    bars: list[NormalizedBar]
    entry_bar: NormalizedBar | None


class ReplayFeatureExtractor:
    """Build deterministic replay features using only bars known at event time."""

    def __init__(self, market_data: HistoricalMarketData, *, refresh: bool = False, cache_only: bool = False) -> None:
        self.market_data = market_data
        self.refresh = refresh
        self.cache_only = cache_only

    def extract(self, result: dict) -> FeatureExtractionResult | None:
        simulation = result.get("simulation")
        if not simulation:
            return None
        event = result["news_event"]
        trade_idea = result["trade_idea"]
        event_ts = parse_utc(event["timestamp_utc"])
        symbol = str(trade_idea["symbol"])
        snapshot = self.market_data.snapshot_for_event(symbol, event_ts, refresh=self.refresh, cache_only=self.cache_only)
        interval = str(snapshot.metadata["requested_interval"])
        start = min(session_start_utc(symbol, event_ts), event_ts - timedelta(hours=6))
        end = _day_end_utc(event_ts)
        bars = self.market_data.store.get_bars(
            symbol=symbol,
            interval=interval,
            start=start,
            end=end,
            refresh=self.refresh,
            cache_only=self.cache_only,
        )
        entry_ts = snapshot.event_time_price.timestamp_utc
        entry_bar = _bar_by_timestamp(bars, entry_ts) if entry_ts else None
        symbol_context = _context_from_bars(symbol, bars, event_ts, entry_bar, snapshot)
        cross_asset_context = self._cross_asset_context(symbol, event_ts, interval)
        missing_context_count = sum(1 for key, value in cross_asset_context.items() if key.endswith("_missing") and value)
        missing_context_count += sum(
            1
            for key in ("pre_5m_return", "pre_15m_return", "pre_60m_return", "intraday_return_so_far", "rolling_volatility_60m")
            if symbol_context.get(key) is None
        )
        data_quality_flags = sorted(
            set(
                flag
                for flag, enabled in {
                    "granularity_downgrade": symbol_context["granularity_downgrade_flag"],
                    "coarse_bar_context": symbol_context["coarse_bar_context_flag"],
                    "nearest_bar_approximation": symbol_context["nearest_bar_approximation_flag"],
                    "missing_context": missing_context_count > 0,
                }.items()
                if enabled
            )
        )
        feature_completeness_flag = symbol_context["entry_bar_timestamp_utc"] is not None and missing_context_count == 0
        base_features = {
            "event_id": _event_id(event_ts, event.get("source", ""), event.get("headline", ""), symbol),
            "event_timestamp_utc": event_ts.isoformat(),
            "event_type": result["classification"]["event_type"],
            "source": event.get("source"),
            "severity": float(result["classification"]["severity"]),
            "severity_bucket": _severity_bucket(float(result["classification"]["severity"])),
            "confirmation_count": int(result["confirmation"]["agreeing_signals"]),
            "confirmation_count_bucket": _confirmation_bucket(int(result["confirmation"]["agreeing_signals"])),
            "headline": event.get("headline"),
            "mapped_symbol": symbol,
            "trade_direction": trade_idea["side"],
            "confirmation_passed": bool(result["confirmation"]["passed"]),
            "session_bucket": _session_bucket(event_ts),
            "session_profile": session_profile(symbol),
            "event_within_tradable_session": bool(snapshot.metadata.get("event_within_tradable_session", is_submission_allowed(symbol, event_ts))),
            "session_boundary_flag": bool(snapshot.metadata.get("session_boundary_flag", False)),
            "entry_blocked_reason": snapshot.metadata.get("entry_blocked_reason"),
            "weekday": event_ts.strftime("%A"),
            **symbol_context,
            **cross_asset_context,
            "feature_completeness_flag": feature_completeness_flag,
            "missing_context_count": missing_context_count,
            "data_quality_flags": "|".join(data_quality_flags),
        }
        return FeatureExtractionResult(
            base_features=base_features,
            snapshot=snapshot,
            bars=bars,
            entry_bar=entry_bar,
        )

    def _cross_asset_context(self, symbol: str, event_ts: datetime, interval: str) -> dict:
        context: dict[str, float | bool | None] = {}
        for proxy in CONTEXT_SYMBOLS:
            field = _field_name(proxy)
            if proxy == symbol:
                context[f"{field}_pre_15m_return"] = None
                context[f"{field}_missing"] = True
                continue
            bars = self.market_data.store.get_bars(
                symbol=proxy,
                interval=interval,
                start=event_ts - timedelta(hours=2),
                end=event_ts,
                refresh=self.refresh,
                cache_only=self.cache_only,
            )
            anchor = _latest_bar_at_or_before(bars, event_ts)
            prior = _latest_bar_at_or_before(bars, event_ts - timedelta(minutes=15))
            value = _pct_return(prior.close if prior else None, anchor.close if anchor else None)
            context[f"{field}_pre_15m_return"] = value
            context[f"{field}_missing"] = value is None
        return context


def _context_from_bars(
    symbol: str,
    bars: list[NormalizedBar],
    event_ts: datetime,
    entry_bar: NormalizedBar | None,
    snapshot: SymbolReplaySnapshot,
) -> dict:
    bars_up_to_event = [bar for bar in bars if bar.timestamp_utc <= event_ts]
    entry_ts = entry_bar.timestamp_utc if entry_bar else snapshot.event_time_price.timestamp_utc
    requested_interval = str(snapshot.metadata["requested_interval"])
    provider_interval = str(snapshot.metadata["provider_interval_returned"])
    day_start = session_start_utc(symbol, event_ts)
    intraday_anchor = _latest_bar_at_or_before(bars_up_to_event, event_ts)
    same_day_bars = [bar for bar in bars_up_to_event if bar.timestamp_utc >= day_start]
    intraday_open = same_day_bars[0] if same_day_bars else None
    return {
        "pre_5m_return": _window_return(bars_up_to_event, event_ts, timedelta(minutes=5)),
        "pre_15m_return": _window_return(bars_up_to_event, event_ts, timedelta(minutes=15)),
        "pre_60m_return": _window_return(bars_up_to_event, event_ts, timedelta(minutes=60)),
        "intraday_return_so_far": _pct_return(intraday_open.close if intraday_open else None, intraday_anchor.close if intraday_anchor else None),
        "rolling_volatility_60m": _rolling_volatility(bars_up_to_event, event_ts, timedelta(minutes=60)),
        "feature_window_bar_count": len(bars_up_to_event),
        "time_of_day_utc": event_ts.strftime("%H:%M"),
        "entry_bar_timestamp_utc": entry_ts.isoformat() if entry_ts else None,
        "actual_provider_interval_returned": provider_interval,
        "granularity_downgrade_flag": provider_interval != requested_interval,
        "coarse_bar_context_flag": provider_interval != requested_interval,
        "nearest_bar_approximation_flag": bool(entry_ts and abs((entry_ts - event_ts).total_seconds()) > 0),
    }


def _window_return(bars: list[NormalizedBar], event_ts: datetime, delta: timedelta) -> float | None:
    anchor = _latest_bar_at_or_before(bars, event_ts)
    previous = _latest_bar_at_or_before(bars, event_ts - delta)
    return _pct_return(previous.close if previous else None, anchor.close if anchor else None)


def _rolling_volatility(bars: list[NormalizedBar], event_ts: datetime, delta: timedelta) -> float | None:
    window = [bar for bar in bars if event_ts - delta <= bar.timestamp_utc <= event_ts]
    if len(window) < 3:
        return None
    returns = []
    for previous, current in zip(window, window[1:]):
        value = _pct_return(previous.close, current.close)
        if value is not None:
            returns.append(value)
    if len(returns) < 2:
        return None
    mean = sum(returns) / len(returns)
    variance = sum((item - mean) ** 2 for item in returns) / (len(returns) - 1)
    return round(sqrt(variance), 8)


def _pct_return(start_price: float | None, end_price: float | None) -> float | None:
    if start_price is None or end_price is None or start_price <= 0:
        return None
    return round((end_price / start_price) - 1.0, 8)


def _latest_bar_at_or_before(bars: list[NormalizedBar], target: datetime) -> NormalizedBar | None:
    eligible = [bar for bar in bars if bar.timestamp_utc <= target]
    if not eligible:
        return None
    return eligible[-1]


def _bar_by_timestamp(bars: list[NormalizedBar], target: datetime | None) -> NormalizedBar | None:
    if target is None:
        return None
    for bar in bars:
        if bar.timestamp_utc == target:
            return bar
    return None


def _event_id(event_ts: datetime, source: str, headline: str, symbol: str) -> str:
    payload = f"{event_ts.isoformat()}|{source}|{headline}|{symbol}"
    return hashlib.sha1(payload.encode("utf-8")).hexdigest()[:20]


def _severity_bucket(value: float) -> str:
    if value < 0.5:
        return "<0.50"
    if value < 0.7:
        return "0.50-0.69"
    if value < 0.85:
        return "0.70-0.84"
    return ">=0.85"


def _confirmation_bucket(value: int) -> str:
    if value <= 0:
        return "0"
    if value == 1:
        return "1"
    if value == 2:
        return "2"
    return "3+"


def _session_bucket(event_ts: datetime) -> str:
    hour = parse_utc(event_ts).hour
    for start, end, label in SESSION_BUCKETS:
        if start <= hour < end:
            return label
    return "unknown"
def _day_end_utc(event_ts: datetime) -> datetime:
    normalized = parse_utc(event_ts)
    return datetime(normalized.year, normalized.month, normalized.day, tzinfo=timezone.utc) + timedelta(days=1) - timedelta(seconds=1)


def _field_name(symbol: str) -> str:
    return symbol.lower().replace("^", "").replace("=", "").replace("-", "_")
