from __future__ import annotations

import json
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path

from app.replay.data_models import NormalizedBar, ResearchChunkMetadata, SCHEMA_VERSION, parse_utc
from app.replay.providers import HistoricalBarProvider


DEFAULT_RESEARCH_DATA_DIR = Path("C:/Users/alexa/arbitrage-machine/data/research/historical")


def _stable_json_dumps(payload: dict) -> str:
    # Keep research artifacts deterministic while avoiding pretty-print bloat.
    return json.dumps(payload, sort_keys=True, separators=(",", ":"))


class HistoricalResearchDataStore:
    def __init__(
        self,
        root: Path,
        provider: HistoricalBarProvider,
        persist: bool = True,
    ) -> None:
        self.root = root
        self.provider = provider
        self.persist = persist
        self.root.mkdir(parents=True, exist_ok=True)
        self.stats = {
            "provider_fetch_count": 0,
            "full_store_hits": 0,
            "partial_fetches": 0,
            "chunk_files_used": 0,
            "chunk_files_written": 0,
            "granularity_downgrade_count": 0,
            "data_quality_warnings": [],
            "symbols_fetched": [],
            "symbols_store_hit": [],
        }

    def get_bars(
        self,
        symbol: str,
        interval: str,
        start: datetime,
        end: datetime,
        *,
        refresh: bool = False,
        cache_only: bool = False,
    ) -> list[NormalizedBar]:
        start = parse_utc(start)
        end = parse_utc(end)
        chunk_keys = list(_chunk_keys_for_range(start, end))
        manifest = self._load_manifest(symbol, interval)
        bars: list[NormalizedBar] = []
        missing_chunks: list[str] = []
        used_chunks = 0
        for chunk_key in chunk_keys:
            metadata = manifest.get(chunk_key)
            if metadata and not refresh:
                chunk_bars = self._read_chunk(Path(metadata.file_path))
                bars.extend(chunk_bars)
                used_chunks += 1
                continue
            if cache_only:
                raise RuntimeError(f"cache_only_missing_chunk:{symbol}:{interval}:{chunk_key}")
            missing_chunks.append(chunk_key)
        if used_chunks == len(chunk_keys) and chunk_keys:
            self.stats["full_store_hits"] += 1
            self.stats["symbols_store_hit"].append(symbol)
        elif used_chunks > 0:
            self.stats["partial_fetches"] += 1
        for chunk_key in missing_chunks:
            chunk_start, chunk_end = _chunk_bounds(chunk_key)
            fetch_result = self.provider.fetch(symbol, interval, chunk_start, chunk_end)
            normalized, conflict_count, warnings = _normalize_and_validate(fetch_result.bars, interval)
            self.stats["provider_fetch_count"] += 1
            self.stats["symbols_fetched"].append(symbol)
            if fetch_result.provider_interval_returned != interval:
                self.stats["granularity_downgrade_count"] += 1
            self.stats["data_quality_warnings"].extend(fetch_result.warnings + warnings)
            bars.extend(normalized)
            if self.persist:
                chunk_path = self._chunk_path(symbol, interval, chunk_key)
                chunk_path.parent.mkdir(parents=True, exist_ok=True)
                metadata = ResearchChunkMetadata(
                    symbol=symbol,
                    interval=interval,
                    provider=self.provider.provider_name,
                    provider_version=self.provider.provider_version,
                    chunk_key=chunk_key,
                    covered_start_utc=chunk_start.isoformat(),
                    covered_end_utc=chunk_end.isoformat(),
                    bar_count=len(normalized),
                    schema_version=SCHEMA_VERSION,
                    provider_interval_returned=fetch_result.provider_interval_returned,
                    created_at_utc=datetime.now(timezone.utc).isoformat(),
                    updated_at_utc=datetime.now(timezone.utc).isoformat(),
                    file_path=str(chunk_path),
                    data_quality_flags=sorted(set(fetch_result.warnings + warnings)),
                    conflict_count=conflict_count,
                )
                self._write_chunk(chunk_path, metadata, normalized)
                manifest[chunk_key] = metadata
                self.stats["chunk_files_written"] += 1
        if self.persist:
            self._write_manifest(symbol, interval, manifest)
        self.stats["chunk_files_used"] += used_chunks
        filtered = [bar for bar in _merge_bars(bars) if start <= bar.timestamp_utc <= end]
        return filtered

    def _symbol_interval_dir(self, symbol: str, interval: str) -> Path:
        return self.root / self.provider.provider_name / symbol / interval

    def _manifest_path(self, symbol: str, interval: str) -> Path:
        return self._symbol_interval_dir(symbol, interval) / "manifest.json"

    def _chunk_path(self, symbol: str, interval: str, chunk_key: str) -> Path:
        return self._symbol_interval_dir(symbol, interval) / f"{chunk_key}.json"

    def _load_manifest(self, symbol: str, interval: str) -> dict[str, ResearchChunkMetadata]:
        path = self._manifest_path(symbol, interval)
        if not path.exists():
            return {}
        payload = json.loads(path.read_text(encoding="utf-8"))
        return {key: ResearchChunkMetadata.from_json(value) for key, value in payload.get("chunks", {}).items()}

    def _write_manifest(self, symbol: str, interval: str, manifest: dict[str, ResearchChunkMetadata]) -> None:
        path = self._manifest_path(symbol, interval)
        path.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "symbol": symbol,
            "interval": interval,
            "provider": self.provider.provider_name,
            "provider_version": self.provider.provider_version,
            "schema_version": SCHEMA_VERSION,
            "chunks": {key: meta.to_json() for key, meta in sorted(manifest.items())},
        }
        path.write_text(_stable_json_dumps(payload), encoding="utf-8")

    def _read_chunk(self, path: Path) -> list[NormalizedBar]:
        payload = json.loads(path.read_text(encoding="utf-8"))
        return [NormalizedBar.from_json(item) for item in payload.get("bars", [])]

    def _write_chunk(self, path: Path, metadata: ResearchChunkMetadata, bars: list[NormalizedBar]) -> None:
        payload = {
            "metadata": metadata.to_json(),
            "bars": [bar.to_json() for bar in bars],
        }
        path.write_text(_stable_json_dumps(payload), encoding="utf-8")


def _normalize_and_validate(bars: list[NormalizedBar], requested_interval: str) -> tuple[list[NormalizedBar], int, list[str]]:
    warnings: list[str] = []
    normalized_by_ts: dict[str, NormalizedBar] = {}
    conflict_count = 0
    for bar in bars:
        ts = parse_utc(bar.timestamp_utc).replace(microsecond=0)
        if bar.high is not None and bar.low is not None and bar.high < bar.low:
            raise ValueError("invalid_ohlc_high_low")
        normalized = NormalizedBar(
            symbol=bar.symbol,
            timestamp_utc=ts,
            open=None if bar.open is None else round(float(bar.open), 8),
            high=None if bar.high is None else round(float(bar.high), 8),
            low=None if bar.low is None else round(float(bar.low), 8),
            close=round(float(bar.close), 8),
            volume=None if bar.volume is None else round(float(bar.volume), 8),
            requested_interval=requested_interval,
            provider_interval_returned=bar.provider_interval_returned,
            provider=bar.provider,
            provider_version=bar.provider_version,
            ingest_timestamp_utc=parse_utc(bar.ingest_timestamp_utc),
            schema_version=SCHEMA_VERSION,
        )
        key = normalized.timestamp_utc.isoformat()
        if key in normalized_by_ts:
            conflict_count += 1
        normalized_by_ts[key] = normalized
        if normalized.provider_interval_returned != requested_interval:
            warnings.append(f"provider_granularity_downgrade:{requested_interval}->{normalized.provider_interval_returned}")
    normalized = sorted(normalized_by_ts.values(), key=lambda item: item.timestamp_utc)
    for previous, current in zip(normalized, normalized[1:]):
        if current.timestamp_utc <= previous.timestamp_utc:
            raise ValueError("non_monotonic_timestamp_sequence")
    return normalized, conflict_count, sorted(set(warnings))


def _merge_bars(bars: list[NormalizedBar]) -> list[NormalizedBar]:
    deduped: dict[str, NormalizedBar] = {}
    for bar in bars:
        deduped[bar.timestamp_utc.isoformat()] = bar
    return sorted(deduped.values(), key=lambda item: item.timestamp_utc)


def _chunk_keys_for_range(start: datetime, end: datetime):
    cursor = datetime(start.year, start.month, 1, tzinfo=timezone.utc)
    last = datetime(end.year, end.month, 1, tzinfo=timezone.utc)
    while cursor <= last:
        yield cursor.strftime("%Y-%m")
        if cursor.month == 12:
            cursor = datetime(cursor.year + 1, 1, 1, tzinfo=timezone.utc)
        else:
            cursor = datetime(cursor.year, cursor.month + 1, 1, tzinfo=timezone.utc)


def _chunk_bounds(chunk_key: str) -> tuple[datetime, datetime]:
    year_str, month_str = chunk_key.split("-")
    year = int(year_str)
    month = int(month_str)
    start = datetime(year, month, 1, tzinfo=timezone.utc)
    if month == 12:
        next_month = datetime(year + 1, 1, 1, tzinfo=timezone.utc)
    else:
        next_month = datetime(year, month + 1, 1, tzinfo=timezone.utc)
    end = next_month - timedelta(seconds=1)
    return start, end
