from __future__ import annotations

import json
import re
import time
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path

import requests

from app.config import Settings
from app.models import ExternalDistribution


JSON_BLOCK_PATTERN = re.compile(r"(\{.*?\"probabilities\".*?\})", re.DOTALL)
RATE_PATTERN = re.compile(r"(\d+(?:\.\d+)?)")


@dataclass(slots=True)
class ExternalDataResult:
    distributions: dict[str, ExternalDistribution]
    stale: bool
    errors: list[str]
    fetched_at: datetime
    source: str


class FedFuturesProvider:
    def __init__(self, settings: Settings, session: requests.Session | None = None) -> None:
        self.settings = settings
        self.session = session or requests.Session()

    def fetch(self) -> ExternalDataResult:
        fetched_at = datetime.now(timezone.utc)
        live_result = self._fetch_live(fetched_at)
        if live_result.distributions and not live_result.stale:
            self._write_cache(live_result)
            return live_result
        cached_result = self._read_cache(fetched_at)
        if cached_result is not None:
            cached_result.errors.extend(live_result.errors)
            return cached_result
        return live_result

    def _fetch_live(self, fetched_at: datetime) -> ExternalDataResult:
        errors: list[str] = []
        for attempt in range(self.settings.external_fetch_retries):
            try:
                response = self.session.get(self.settings.fedwatch_url, timeout=20)
                response.raise_for_status()
                distributions, parse_errors = self._parse_payload(response.text, fetched_at)
                stale = self._is_stale(distributions, fetched_at)
                errors.extend(parse_errors)
                return ExternalDataResult(distributions=distributions, stale=stale, errors=errors, fetched_at=fetched_at, source="live")
            except Exception as exc:
                errors.append(f"download_failed_attempt_{attempt + 1}:{exc}")
                if attempt + 1 < self.settings.external_fetch_retries:
                    time.sleep(self.settings.external_fetch_backoff_seconds * (attempt + 1))
        return ExternalDataResult(distributions={}, stale=True, errors=errors, fetched_at=fetched_at, source="failed")

    def _is_stale(self, distributions: dict[str, ExternalDistribution], fetched_at: datetime) -> bool:
        if not distributions:
            return True
        cutoff = fetched_at - timedelta(seconds=min(self.settings.fedwatch_max_staleness_seconds, self.settings.max_external_data_staleness_seconds))
        return any(item.as_of < cutoff for item in distributions.values())

    def _parse_payload(self, html: str, fetched_at: datetime) -> tuple[dict[str, ExternalDistribution], list[str]]:
        errors: list[str] = []
        distributions = self._parse_json_blocks(html, fetched_at)
        if not distributions:
            distributions = self._parse_table(html, fetched_at)
        if not distributions:
            errors.append("no_distributions_found")
        invalid = [key for key, dist in distributions.items() if not self._validate_distribution(dist)]
        if invalid:
            errors.append(f"invalid_distribution:{','.join(invalid)}")
            distributions = {}
        return distributions, errors

    def _validate_distribution(self, distribution: ExternalDistribution) -> bool:
        if not distribution.values:
            return False
        total = sum(distribution.values.values())
        return abs(total - 1.0) <= 0.05 and all(prob >= 0 for prob in distribution.values.values())

    def _normalize_distribution(self, values: dict[float, float]) -> dict[float, float]:
        total = sum(values.values())
        if total <= 0:
            return {}
        return {rate: prob / total for rate, prob in values.items()}

    def _parse_json_blocks(self, html: str, fetched_at: datetime) -> dict[str, ExternalDistribution]:
        distributions: dict[str, ExternalDistribution] = {}
        for match in JSON_BLOCK_PATTERN.finditer(html):
            try:
                obj = json.loads(match.group(1))
            except json.JSONDecodeError:
                continue
            distribution_key = str(obj.get("meeting") or obj.get("key") or obj.get("label") or "fed")
            values: dict[float, float] = {}
            for row in obj.get("probabilities", []):
                rate = _extract_rate(row.get("range") or row.get("label") or row.get("target") or "")
                probability = row.get("probability")
                if rate is None or probability is None:
                    continue
                values[rate] = float(probability)
            normalized = self._normalize_distribution(values)
            if normalized:
                distributions[distribution_key] = ExternalDistribution(
                    source="fed_futures",
                    key=distribution_key,
                    values=normalized,
                    as_of=fetched_at,
                    metadata={"source_format": "json", "row_count": len(values)},
                )
        return distributions

    def _parse_table(self, html: str, fetched_at: datetime) -> dict[str, ExternalDistribution]:
        try:
            from bs4 import BeautifulSoup
        except ModuleNotFoundError:
            return {}
        soup = BeautifulSoup(html, "html.parser")
        rows = []
        for row in soup.select("table tr"):
            cols = [col.get_text(" ", strip=True) for col in row.find_all(["td", "th"])]
            if len(cols) >= 2:
                rows.append(cols)
        if not rows:
            return {}
        values: dict[float, float] = {}
        for cols in rows:
            rate = _extract_rate(cols[0])
            try:
                probability = float(cols[-1].replace("%", "").strip()) / 100.0
            except ValueError:
                continue
            if rate is not None:
                values[rate] = probability
        normalized = self._normalize_distribution(values)
        if not normalized:
            return {}
        return {
            "fed": ExternalDistribution(
                source="fed_futures",
                key="fed",
                values=normalized,
                as_of=fetched_at,
                metadata={"source_format": "table", "row_count": len(values)},
            )
        }

    def _write_cache(self, result: ExternalDataResult) -> None:
        payload = {
            "fetched_at": result.fetched_at.isoformat(),
            "source": result.source,
            "distributions": {
                key: {
                    "source": dist.source,
                    "key": dist.key,
                    "values": dist.values,
                    "as_of": dist.as_of.isoformat(),
                    "metadata": dist.metadata,
                }
                for key, dist in result.distributions.items()
            },
        }
        self.settings.external_cache_path.write_text(json.dumps(payload), encoding="utf-8")

    def _read_cache(self, fetched_at: datetime) -> ExternalDataResult | None:
        cache_path = Path(self.settings.external_cache_path)
        if not cache_path.exists():
            return None
        try:
            payload = json.loads(cache_path.read_text(encoding="utf-8"))
            cached_at = datetime.fromisoformat(payload["fetched_at"])
        except Exception:
            return None
        if (fetched_at - cached_at).total_seconds() > self.settings.external_cache_freshness_seconds:
            return None
        distributions = {
            key: ExternalDistribution(
                source=value["source"],
                key=value["key"],
                values={float(rate): float(prob) for rate, prob in value["values"].items()},
                as_of=datetime.fromisoformat(value["as_of"]),
                metadata=value.get("metadata", {}),
            )
            for key, value in payload.get("distributions", {}).items()
        }
        if not distributions:
            return None
        return ExternalDataResult(distributions=distributions, stale=False, errors=[], fetched_at=fetched_at, source="cache")


def _extract_rate(text: str) -> float | None:
    match = RATE_PATTERN.search(str(text))
    if not match:
        return None
    return float(match.group(1))
