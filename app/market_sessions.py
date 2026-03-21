from __future__ import annotations

from datetime import datetime, time, timedelta
from zoneinfo import ZoneInfo


NEW_YORK = ZoneInfo("America/New_York")
US_EQUITY_OPEN = time(hour=9, minute=30)
US_EQUITY_CLOSE = time(hour=16, minute=0)


def session_profile(symbol: str) -> str:
    upper = symbol.upper()
    if upper == "BTC-USD":
        return "crypto_24_7"
    if upper.endswith("=F"):
        return "us_futures_extended"
    return "us_equity_regular"


def is_submission_allowed(symbol: str, timestamp_utc: datetime) -> bool:
    profile = session_profile(symbol)
    if profile == "crypto_24_7":
        return True
    local = _to_new_york(timestamp_utc)
    if profile == "us_equity_regular":
        if local.weekday() >= 5:
            return False
        current = local.timetz().replace(tzinfo=None)
        return US_EQUITY_OPEN <= current < US_EQUITY_CLOSE
    return _is_futures_session(local)


def is_bar_tradable(symbol: str, timestamp_utc: datetime) -> bool:
    profile = session_profile(symbol)
    if profile == "crypto_24_7":
        return True
    local = _to_new_york(timestamp_utc)
    if profile == "us_equity_regular":
        if local.weekday() >= 5:
            return False
        current = local.timetz().replace(tzinfo=None)
        return US_EQUITY_OPEN <= current <= US_EQUITY_CLOSE
    return _is_futures_session(local)


def is_session_boundary(symbol: str, timestamp_utc: datetime, buffer_seconds: int = 300) -> bool:
    profile = session_profile(symbol)
    if profile == "crypto_24_7" or buffer_seconds <= 0:
        return False
    local = _to_new_york(timestamp_utc)
    if profile == "us_equity_regular":
        if local.weekday() >= 5:
            return False
        current = local.timetz().replace(tzinfo=None)
        open_delta = _seconds_between(US_EQUITY_OPEN, current)
        close_delta = _seconds_between(current, US_EQUITY_CLOSE)
        return 0 <= open_delta <= buffer_seconds or 0 <= close_delta <= buffer_seconds
    maintenance_start = time(hour=17, minute=0)
    maintenance_end = time(hour=18, minute=0)
    current = local.timetz().replace(tzinfo=None)
    open_gap = _seconds_between(maintenance_end, current)
    close_gap = _seconds_between(current, maintenance_start)
    return 0 <= open_gap <= buffer_seconds or 0 <= close_gap <= buffer_seconds


def same_trading_session(symbol: str, left_utc: datetime, right_utc: datetime) -> bool:
    profile = session_profile(symbol)
    if profile == "crypto_24_7":
        return left_utc.date() == right_utc.date()
    left_local = _to_new_york(left_utc)
    right_local = _to_new_york(right_utc)
    if profile == "us_equity_regular":
        return left_local.date() == right_local.date()
    return _futures_session_anchor(left_local) == _futures_session_anchor(right_local)


def session_start_utc(symbol: str, timestamp_utc: datetime) -> datetime:
    profile = session_profile(symbol)
    local = _to_new_york(timestamp_utc)
    if profile == "us_equity_regular":
        session_local = datetime.combine(local.date(), US_EQUITY_OPEN, tzinfo=NEW_YORK)
        return session_local.astimezone(timestamp_utc.tzinfo or ZoneInfo("UTC"))
    if profile == "us_futures_extended":
        anchor_date = _futures_session_anchor(local)
        session_local = datetime.combine(anchor_date - timedelta(days=1), time(hour=18, minute=0), tzinfo=NEW_YORK)
        return session_local.astimezone(timestamp_utc.tzinfo or ZoneInfo("UTC"))
    normalized = timestamp_utc if timestamp_utc.tzinfo is not None else timestamp_utc.replace(tzinfo=ZoneInfo("UTC"))
    return normalized.replace(hour=0, minute=0, second=0, microsecond=0)


def _to_new_york(timestamp_utc: datetime) -> datetime:
    if timestamp_utc.tzinfo is None:
        timestamp_utc = timestamp_utc.replace(tzinfo=ZoneInfo("UTC"))
    return timestamp_utc.astimezone(NEW_YORK)


def _is_futures_session(local: datetime) -> bool:
    weekday = local.weekday()
    current = local.timetz().replace(tzinfo=None)
    if weekday == 5:
        return False
    if weekday == 6:
        return current >= time(hour=18, minute=0)
    if current < time(hour=17, minute=0):
        return True
    return current >= time(hour=18, minute=0)


def _futures_session_anchor(local: datetime) -> datetime.date:
    current = local.timetz().replace(tzinfo=None)
    if current >= time(hour=18, minute=0):
        return (local + timedelta(days=1)).date()
    return local.date()


def _seconds_between(start: time, end: time) -> int:
    start_seconds = start.hour * 3600 + start.minute * 60 + start.second
    end_seconds = end.hour * 3600 + end.minute * 60 + end.second
    return end_seconds - start_seconds
