from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone

import requests

from app.replay.data_models import NormalizedBar, parse_utc


YAHOO_CHART_URL = "https://query1.finance.yahoo.com/v8/finance/chart/{symbol}"
YAHOO_HEADERS = {"User-Agent": "arbitrage-machine-replay/0.1"}


@dataclass(slots=True)
class ProviderFetchResult:
    bars: list[NormalizedBar]
    provider_interval_returned: str
    warnings: list[str]


class HistoricalBarProvider:
    provider_name = "provider"
    provider_version = "provider-v1"

    def fetch(self, symbol: str, interval: str, start: datetime, end: datetime) -> ProviderFetchResult:
        raise NotImplementedError


class YahooHistoricalBarProvider(HistoricalBarProvider):
    provider_name = "yahoo"
    provider_version = "yahoo-chart-v1"

    def __init__(self, timeout_seconds: int = 10) -> None:
        self.timeout_seconds = timeout_seconds
        self.session = requests.Session()

    def fetch(self, symbol: str, interval: str, start: datetime, end: datetime) -> ProviderFetchResult:
        response = self.session.get(
            YAHOO_CHART_URL.format(symbol=symbol),
            params={
                "period1": int(parse_utc(start).timestamp()),
                "period2": int(parse_utc(end).timestamp()),
                "interval": interval,
                "includePrePost": "true",
            },
            timeout=self.timeout_seconds,
            headers=YAHOO_HEADERS,
        )
        response.raise_for_status()
        payload = response.json()
        result = (((payload.get("chart") or {}).get("result") or [None])[0]) or {}
        meta = result.get("meta") or {}
        timestamps = result.get("timestamp") or []
        quote = (((result.get("indicators") or {}).get("quote") or [{}])[0]) or {}
        opens = quote.get("open") or []
        highs = quote.get("high") or []
        lows = quote.get("low") or []
        closes = quote.get("close") or []
        volumes = quote.get("volume") or []
        provider_interval_returned = str(meta.get("dataGranularity") or interval)
        warnings: list[str] = []
        if provider_interval_returned != interval:
            warnings.append(f"provider_granularity_downgrade:{interval}->{provider_interval_returned}")
        bars: list[NormalizedBar] = []
        ingest_ts = datetime.now(timezone.utc)
        for index, raw_ts in enumerate(timestamps):
            close = closes[index] if index < len(closes) else None
            if close is None:
                continue
            open_value = opens[index] if index < len(opens) else None
            high_value = highs[index] if index < len(highs) else None
            low_value = lows[index] if index < len(lows) else None
            if high_value is not None and low_value is not None and high_value < low_value:
                warnings.append("invalid_ohlc_relationship")
                continue
            bars.append(
                NormalizedBar(
                    symbol=symbol,
                    timestamp_utc=datetime.fromtimestamp(int(raw_ts), tz=timezone.utc),
                    open=float(open_value) if open_value is not None else None,
                    high=float(high_value) if high_value is not None else None,
                    low=float(low_value) if low_value is not None else None,
                    close=float(close),
                    volume=float(volumes[index]) if index < len(volumes) and volumes[index] is not None else None,
                    requested_interval=interval,
                    provider_interval_returned=provider_interval_returned,
                    provider=self.provider_name,
                    provider_version=self.provider_version,
                    ingest_timestamp_utc=ingest_ts,
                )
            )
        return ProviderFetchResult(bars=bars, provider_interval_returned=provider_interval_returned, warnings=warnings)
