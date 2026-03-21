from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path

from app.market_sessions import is_bar_tradable, is_session_boundary, is_submission_allowed, same_trading_session, session_profile
from app.replay.data_models import NormalizedBar, parse_utc
from app.replay.data_store import DEFAULT_RESEARCH_DATA_DIR, HistoricalResearchDataStore
from app.replay.providers import HistoricalBarProvider, YahooHistoricalBarProvider


DEFAULT_DATA_DIR = DEFAULT_RESEARCH_DATA_DIR


@dataclass(slots=True)
class HorizonSnapshot:
    timestamp_utc: datetime | None
    price: float | None


@dataclass(slots=True)
class SymbolReplaySnapshot:
    symbol: str
    pre_event_price: HorizonSnapshot
    event_time_price: HorizonSnapshot
    plus_5m: HorizonSnapshot
    plus_15m: HorizonSnapshot
    plus_60m: HorizonSnapshot
    eod: HorizonSnapshot
    metadata: dict


class HistoricalMarketData:
    def __init__(
        self,
        store: HistoricalResearchDataStore | None = None,
        timeout_seconds: int = 10,
        data_dir: Path | None = None,
        persist: bool = True,
    ) -> None:
        if store is None:
            provider: HistoricalBarProvider = YahooHistoricalBarProvider(timeout_seconds=timeout_seconds)
            store = HistoricalResearchDataStore(root=data_dir or DEFAULT_DATA_DIR, provider=provider, persist=persist)
        self.store = store

    @property
    def research_data_stats(self) -> dict:
        return dict(self.store.stats)

    def snapshot_for_event(self, symbol: str, event_ts: datetime, *, refresh: bool = False, cache_only: bool = False) -> SymbolReplaySnapshot:
        interval, lookback = _interval_and_lookback(event_ts)
        normalized_event_ts = parse_utc(event_ts)
        bars = self.store.get_bars(
            symbol=symbol,
            interval=interval,
            start=normalized_event_ts - lookback,
            end=_day_end_utc(normalized_event_ts),
            refresh=refresh,
            cache_only=cache_only,
        )
        tradable_bars = [bar for bar in bars if is_bar_tradable(symbol, bar.timestamp_utc)]
        pre_event = _latest_bar_at_or_before(tradable_bars, normalized_event_ts - timedelta(minutes=15))
        event_within_session = is_submission_allowed(symbol, normalized_event_ts)
        session_boundary_flag = is_session_boundary(symbol, normalized_event_ts)
        entry_blocked_reason = None
        if event_within_session:
            event_time = _first_bar_at_or_after(tradable_bars, normalized_event_ts, session_anchor=normalized_event_ts, symbol=symbol)
            if session_boundary_flag and event_time.timestamp_utc is not None and event_time.timestamp_utc > normalized_event_ts:
                event_time = HorizonSnapshot(timestamp_utc=None, price=None)
                entry_blocked_reason = "boundary_forward_bar_disallowed"
        else:
            event_time = HorizonSnapshot(timestamp_utc=None, price=None)
            entry_blocked_reason = "outside_tradable_session"
        entry_anchor = event_time.timestamp_utc
        plus_5m = _first_bar_at_or_after(tradable_bars, entry_anchor + timedelta(minutes=5), session_anchor=entry_anchor, symbol=symbol) if entry_anchor else HorizonSnapshot(timestamp_utc=None, price=None)
        plus_15m = _first_bar_at_or_after(tradable_bars, entry_anchor + timedelta(minutes=15), session_anchor=entry_anchor, symbol=symbol) if entry_anchor else HorizonSnapshot(timestamp_utc=None, price=None)
        plus_60m = _first_bar_at_or_after(tradable_bars, entry_anchor + timedelta(minutes=60), session_anchor=entry_anchor, symbol=symbol) if entry_anchor else HorizonSnapshot(timestamp_utc=None, price=None)
        eod = _same_day_last_bar(tradable_bars, entry_anchor) if entry_anchor else HorizonSnapshot(timestamp_utc=None, price=None)
        provider_interval_returned = bars[0].provider_interval_returned if bars else interval
        event_to_entry_delay_seconds = None
        if event_time.timestamp_utc is not None:
            event_to_entry_delay_seconds = max(0.0, (event_time.timestamp_utc - normalized_event_ts).total_seconds())
        return SymbolReplaySnapshot(
            symbol=symbol,
            pre_event_price=pre_event,
            event_time_price=event_time,
            plus_5m=plus_5m,
            plus_15m=plus_15m,
            plus_60m=plus_60m,
            eod=eod,
            metadata={
                "bar_count": len(bars),
                "requested_interval": interval,
                "provider_interval_returned": provider_interval_returned,
                "provider": bars[0].provider if bars else self.store.provider.provider_name,
                "provider_version": bars[0].provider_version if bars else self.store.provider.provider_version,
                "entry_anchor_mode": "first_tradable_bar_at_or_after_event_within_same_session",
                "exit_anchor_mode": "first_tradable_bar_at_or_after_entry_plus_horizon_within_same_session",
                "event_timestamp_utc": normalized_event_ts.isoformat(),
                "event_to_entry_delay_seconds": event_to_entry_delay_seconds,
                "entry_uses_future_bar": bool(event_to_entry_delay_seconds and event_to_entry_delay_seconds > 0),
                "event_within_tradable_session": event_within_session,
                "session_boundary_flag": session_boundary_flag,
                "entry_blocked_reason": entry_blocked_reason,
                "session_profile": session_profile(symbol),
                "tradable_bar_count": len(tradable_bars),
            },
        )

    def batch_snapshot_for_event(
        self,
        symbols: list[str],
        event_ts: datetime,
        *,
        refresh: bool = False,
        cache_only: bool = False,
    ) -> dict[str, SymbolReplaySnapshot]:
        return {
            symbol: self.snapshot_for_event(symbol, event_ts, refresh=refresh, cache_only=cache_only)
            for symbol in symbols
        }


def _interval_and_lookback(event_ts: datetime) -> tuple[str, timedelta]:
    # Deterministic replay policy: always request 1m bars and let provider
    # downgrade explicitly when historical retention is coarser.
    return "1m", timedelta(hours=6)


def _first_bar_at_or_after(
    bars: list[NormalizedBar],
    target: datetime,
    *,
    session_anchor: datetime | None = None,
    symbol: str | None = None,
) -> HorizonSnapshot:
    for bar in bars:
        if bar.timestamp_utc < target:
            continue
        if session_anchor is not None and symbol is not None and not same_trading_session(symbol, bar.timestamp_utc, session_anchor):
            continue
        if bar.timestamp_utc >= target:
            return HorizonSnapshot(timestamp_utc=bar.timestamp_utc, price=bar.close)
    return HorizonSnapshot(timestamp_utc=None, price=None)


def _latest_bar_at_or_before(bars: list[NormalizedBar], target: datetime) -> HorizonSnapshot:
    eligible = [bar for bar in bars if bar.timestamp_utc <= target]
    if not eligible:
        return HorizonSnapshot(timestamp_utc=None, price=None)
    best = eligible[-1]
    return HorizonSnapshot(timestamp_utc=best.timestamp_utc, price=best.close)


def _same_day_last_bar(bars: list[NormalizedBar], target: datetime) -> HorizonSnapshot:
    same_day = [bar for bar in bars if bar.timestamp_utc.date() == target.date()]
    if not same_day:
        return HorizonSnapshot(timestamp_utc=None, price=None)
    best = same_day[-1]
    return HorizonSnapshot(timestamp_utc=best.timestamp_utc, price=best.close)


def _day_end_utc(value: datetime) -> datetime:
    normalized = parse_utc(value)
    return datetime(normalized.year, normalized.month, normalized.day, tzinfo=timezone.utc) + timedelta(days=1) - timedelta(seconds=1)
