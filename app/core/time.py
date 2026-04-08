from __future__ import annotations

from datetime import datetime, timedelta, timezone


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def is_stale(observed_at: datetime, max_age_seconds: float, *, now: datetime | None = None) -> bool:
    current = now or utc_now()
    return observed_at < current - timedelta(seconds=max_age_seconds)
