from __future__ import annotations

from dataclasses import dataclass

import requests


YAHOO_CHART_URL = "https://query1.finance.yahoo.com/v8/finance/chart/{symbol}"
YAHOO_TIMEOUT_SECONDS = 5
YAHOO_HEADERS = {"User-Agent": "arbitrage-machine/0.1"}


@dataclass(slots=True)
class MacroPrices:
    spy_price: float | None
    qqq_price: float | None
    tlt_price: float | None
    btc_price: float | None


def fetch_spy_price() -> float | None:
    return _fetch_symbol_price("SPY")


def fetch_qqq_price() -> float | None:
    return _fetch_symbol_price("QQQ")


def fetch_tlt_price() -> float | None:
    return _fetch_symbol_price("TLT")


def fetch_btc_price() -> float | None:
    return _fetch_symbol_price("BTC-USD")


def fetch_symbol_price(symbol: str) -> float | None:
    return _fetch_symbol_price(symbol)


def fetch_symbol_prices(symbols: list[str]) -> dict[str, float | None]:
    return {symbol: _fetch_symbol_price(symbol) for symbol in symbols}


def fetch_macro_prices() -> MacroPrices:
    return MacroPrices(
        spy_price=fetch_spy_price(),
        qqq_price=fetch_qqq_price(),
        tlt_price=fetch_tlt_price(),
        btc_price=fetch_btc_price(),
    )


def _fetch_symbol_price(symbol: str) -> float | None:
    response = requests.get(
        YAHOO_CHART_URL.format(symbol=symbol),
        params={"interval": "1m", "range": "1d"},
        timeout=YAHOO_TIMEOUT_SECONDS,
        headers=YAHOO_HEADERS,
    )
    response.raise_for_status()
    payload = response.json()
    result = (((payload.get("chart") or {}).get("result") or [None])[0]) or {}
    meta = result.get("meta") or {}
    regular_market_price = meta.get("regularMarketPrice")
    previous_close = meta.get("chartPreviousClose")
    if regular_market_price is not None:
        return float(regular_market_price)
    if previous_close is not None:
        return float(previous_close)
    close_prices = (((result.get("indicators") or {}).get("quote") or [{}])[0]).get("close") or []
    for value in reversed(close_prices):
        if value is not None:
            return float(value)
    return None
