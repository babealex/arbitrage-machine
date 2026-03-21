from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone


SCHEMA_VERSION = "historical-research-v1"


@dataclass(slots=True)
class NormalizedBar:
    symbol: str
    timestamp_utc: datetime
    open: float | None
    high: float | None
    low: float | None
    close: float
    volume: float | None
    requested_interval: str
    provider_interval_returned: str
    provider: str
    provider_version: str
    ingest_timestamp_utc: datetime
    schema_version: str = SCHEMA_VERSION

    def to_json(self) -> dict:
        return {
            "symbol": self.symbol,
            "timestamp_utc": _iso(self.timestamp_utc),
            "open": _rounded(self.open),
            "high": _rounded(self.high),
            "low": _rounded(self.low),
            "close": round(float(self.close), 8),
            "volume": _rounded(self.volume),
            "requested_interval": self.requested_interval,
            "provider_interval_returned": self.provider_interval_returned,
            "provider": self.provider,
            "provider_version": self.provider_version,
            "ingest_timestamp_utc": _iso(self.ingest_timestamp_utc),
            "schema_version": self.schema_version,
        }

    @classmethod
    def from_json(cls, payload: dict) -> "NormalizedBar":
        return cls(
            symbol=str(payload["symbol"]),
            timestamp_utc=parse_utc(payload["timestamp_utc"]),
            open=_float_or_none(payload.get("open")),
            high=_float_or_none(payload.get("high")),
            low=_float_or_none(payload.get("low")),
            close=float(payload["close"]),
            volume=_float_or_none(payload.get("volume")),
            requested_interval=str(payload["requested_interval"]),
            provider_interval_returned=str(payload["provider_interval_returned"]),
            provider=str(payload["provider"]),
            provider_version=str(payload["provider_version"]),
            ingest_timestamp_utc=parse_utc(payload["ingest_timestamp_utc"]),
            schema_version=str(payload.get("schema_version") or SCHEMA_VERSION),
        )


@dataclass(slots=True)
class ResearchChunkMetadata:
    symbol: str
    interval: str
    provider: str
    provider_version: str
    chunk_key: str
    covered_start_utc: str
    covered_end_utc: str
    bar_count: int
    schema_version: str
    provider_interval_returned: str
    created_at_utc: str
    updated_at_utc: str
    file_path: str
    data_quality_flags: list[str]
    conflict_count: int = 0

    def to_json(self) -> dict:
        return {
            "symbol": self.symbol,
            "interval": self.interval,
            "provider": self.provider,
            "provider_version": self.provider_version,
            "chunk_key": self.chunk_key,
            "covered_start_utc": self.covered_start_utc,
            "covered_end_utc": self.covered_end_utc,
            "bar_count": self.bar_count,
            "schema_version": self.schema_version,
            "provider_interval_returned": self.provider_interval_returned,
            "created_at_utc": self.created_at_utc,
            "updated_at_utc": self.updated_at_utc,
            "file_path": self.file_path,
            "data_quality_flags": list(self.data_quality_flags),
            "conflict_count": self.conflict_count,
        }

    @classmethod
    def from_json(cls, payload: dict) -> "ResearchChunkMetadata":
        return cls(
            symbol=str(payload["symbol"]),
            interval=str(payload["interval"]),
            provider=str(payload["provider"]),
            provider_version=str(payload["provider_version"]),
            chunk_key=str(payload["chunk_key"]),
            covered_start_utc=str(payload["covered_start_utc"]),
            covered_end_utc=str(payload["covered_end_utc"]),
            bar_count=int(payload["bar_count"]),
            schema_version=str(payload["schema_version"]),
            provider_interval_returned=str(payload["provider_interval_returned"]),
            created_at_utc=str(payload["created_at_utc"]),
            updated_at_utc=str(payload["updated_at_utc"]),
            file_path=str(payload["file_path"]),
            data_quality_flags=[str(item) for item in payload.get("data_quality_flags", [])],
            conflict_count=int(payload.get("conflict_count", 0)),
        )


def parse_utc(value: str | datetime) -> datetime:
    if isinstance(value, datetime):
        parsed = value
    else:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _iso(value: datetime) -> str:
    return parse_utc(value).isoformat()


def _rounded(value: float | None) -> float | None:
    if value is None:
        return None
    return round(float(value), 8)


def _float_or_none(value) -> float | None:
    if value is None:
        return None
    return float(value)
