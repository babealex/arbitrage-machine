from __future__ import annotations

from collections import deque
from dataclasses import dataclass
from datetime import datetime, timezone

from app.market_data.macro_fetcher import _fetch_symbol_price


@dataclass(slots=True)
class ExternalProxySnapshot:
    rows: list[dict]
    fetch_errors: list[str]


class ExternalProxyIngestor:
    def __init__(self, history_limit: int = 64) -> None:
        self.history_limit = history_limit
        self._history: dict[str, deque[float]] = {
            "SPY": deque(maxlen=history_limit),
            "QQQ": deque(maxlen=history_limit),
            "TLT": deque(maxlen=history_limit),
            "BTC-USD": deque(maxlen=history_limit),
            "^VIX": deque(maxlen=history_limit),
        }

    def fetch(self, enabled: bool = True) -> ExternalProxySnapshot:
        timestamp = datetime.now(timezone.utc).isoformat()
        rows: list[dict] = []
        errors: list[str] = []
        proxy_specs = [
            ("SPY", "equity_proxy", "risk_proxy"),
            ("QQQ", "equity_proxy", "risk_proxy"),
            ("TLT", "equity_proxy", "rate_expectation"),
            ("BTC-USD", "crypto_proxy", "risk_proxy"),
            ("^VIX", "equity_proxy", "risk_neutral_tail_proxy"),
        ]
        if enabled:
            for symbol, source_type, object_type in proxy_specs:
                try:
                    price = _fetch_symbol_price(symbol)
                except Exception as exc:
                    errors.append(f"{symbol}:{exc}")
                    continue
                if price is None:
                    errors.append(f"{symbol}:missing_price")
                    continue
                self._history[symbol].append(price)
                rows.append(
                    {
                        "timestamp_utc": timestamp,
                        "family": symbol,
                        "event_ticker": symbol,
                        "market_ticker": symbol,
                        "source_type": source_type,
                        "object_type": object_type,
                        "value": price,
                        "bid": None,
                        "ask": None,
                        "size_bid": None,
                        "size_ask": None,
                        "metadata_json": {
                            "rolling_return": rolling_return(self._history[symbol]),
                            "rolling_zscore": rolling_zscore(self._history[symbol]),
                            "session_hour_bucket": hour_bucket(),
                        },
                    }
                )
        rows.extend(self._placeholder_rows(timestamp))
        return ExternalProxySnapshot(rows=rows, fetch_errors=errors)

    def _placeholder_rows(self, timestamp: str) -> list[dict]:
        placeholders = [
            ("FEDWATCH_PROXY", "fedwatch_proxy", "rate_expectation"),
            ("TIPS_PROXY", "tips_proxy", "inflation_compensation"),
        ]
        return [
            {
                "timestamp_utc": timestamp,
                "family": name,
                "event_ticker": name,
                "market_ticker": name,
                "source_type": source_type,
                "object_type": object_type,
                "value": 0.0,
                "bid": None,
                "ask": None,
                "size_bid": None,
                "size_ask": None,
                "metadata_json": {"placeholder": True},
            }
            for name, source_type, object_type in placeholders
        ]


def rolling_return(history: deque[float]) -> float:
    if len(history) < 2:
        return 0.0
    start = history[0]
    end = history[-1]
    if start == 0:
        return 0.0
    return (end - start) / start


def rolling_zscore(history: deque[float]) -> float:
    if len(history) < 2:
        return 0.0
    values = list(history)
    mean = sum(values) / len(values)
    variance = sum((value - mean) ** 2 for value in values) / len(values)
    std = variance ** 0.5
    if std == 0:
        return 0.0
    return (values[-1] - mean) / std


def hour_bucket(ts: datetime | None = None) -> str:
    current = ts or datetime.now(timezone.utc)
    return f"{current.hour:02d}:00"
