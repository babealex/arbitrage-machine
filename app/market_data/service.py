from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone

from app.config import Settings
from app.kalshi.rest_client import KalshiRestClient
from app.market_data.filters import MarketFilterResult, filter_structural_markets
from app.market_data.ladder_builder import build_ladders
from app.market_data.orderbook import best_quotes_in_cents, snapshot_from_orderbook_payload
from app.models import Market, OrderBookSnapshot


@dataclass(slots=True)
class DiscoveryStats:
    discovery_mode_used: str = "generic_open_markets"
    candidate_series_found: int = 0
    candidate_events_found: int = 0
    candidate_markets_found: int = 0
    fallback_used: bool = False


@dataclass(slots=True)
class QuoteRefreshStats:
    usable_markets: int = 0
    dead_markets_skipped: int = 0
    orderbook_snapshots: int = 0
    orderbook_failures: int = 0
    quote_completeness_stats: dict[str, int] = field(default_factory=dict)


class MarketDataService:
    def __init__(self, client: KalshiRestClient, settings: Settings | None = None) -> None:
        self.client = client
        self.settings = settings
        self.orderbooks: dict[str, OrderBookSnapshot] = {}
        self.last_update = datetime.now(timezone.utc)
        self.last_discovery_stats = DiscoveryStats()
        self.last_quote_stats = QuoteRefreshStats()

    def refresh_markets(self) -> list[Market]:
        if self.settings is not None and self.settings.app_mode == "safe":
            raw_markets = self._discover_macro_markets()
            if not raw_markets:
                self.last_discovery_stats.fallback_used = True
                self.last_discovery_stats.discovery_mode_used = "generic_open_markets_fallback"
                raw_markets = self._discover_generic_open_markets()
        else:
            raw_markets = self._discover_generic_open_markets()
        markets = [_to_market(raw) for raw in raw_markets]
        self.last_update = datetime.now(timezone.utc)
        return markets

    def _discover_generic_open_markets(self) -> list[dict]:
        raw_markets: list[dict] = []
        cursor: str | None = None
        pages = max(1, self.settings.market_fetch_pages if self.settings is not None else 1)
        for _ in range(pages):
            payload = self.client.list_markets_page(status="open", cursor=cursor)
            page_markets = payload.get("markets", payload.get("data", []))
            raw_markets.extend(page_markets)
            cursor = payload.get("cursor")
            if not cursor or not page_markets:
                break
        self.last_discovery_stats = DiscoveryStats(
            discovery_mode_used="generic_open_markets",
            candidate_markets_found=len(raw_markets),
        )
        return raw_markets

    def _discover_macro_markets(self) -> list[dict]:
        series = self.client.list_series()
        candidate_series = [item for item in series if _series_matches_macro(item, self.settings.allowed_market_terms)]
        candidate_series.sort(key=lambda item: _series_priority(item, self.settings.allowed_market_terms), reverse=True)
        candidate_markets: list[dict] = []
        seen_market_tickers: set[str] = set()
        candidate_events_found = 0
        scanned_series = 0
        max_series_to_scan = max(self.settings.macro_series_limit, 120)
        target_market_count = max(20, self.settings.diagnostic_market_sample_size)

        for item in candidate_series:
            if scanned_series >= max_series_to_scan:
                break
            series_ticker = item.get("ticker", "")
            scanned_series += 1
            payload = self.client._request("GET", "/markets", params={"status": "open", "series_ticker": series_ticker}, auth=False)
            page_markets = payload.get("markets", payload.get("data", []))
            if page_markets:
                try:
                    events_payload = self.client.list_events_page(series_ticker=series_ticker)
                    candidate_events_found += len(events_payload.get("events", []))
                except Exception:
                    pass
            for market in page_markets:
                ticker = market.get("ticker", "")
                if ticker and ticker not in seen_market_tickers:
                    seen_market_tickers.add(ticker)
                    candidate_markets.append(market)
            if len(candidate_markets) >= target_market_count:
                break

        self.last_discovery_stats = DiscoveryStats(
            discovery_mode_used="macro_series_first",
            candidate_series_found=min(len(candidate_series), scanned_series),
            candidate_events_found=candidate_events_found,
            candidate_markets_found=len(candidate_markets),
        )
        return candidate_markets

    def refresh_orderbook(self, ticker: str) -> OrderBookSnapshot:
        payload = self.client.get_orderbook(ticker)
        snapshot = snapshot_from_orderbook_payload(ticker, payload)
        self.orderbooks[ticker] = snapshot
        self.last_update = datetime.now(timezone.utc)
        return snapshot

    def refresh_market_by_ticker(self, ticker: str) -> Market:
        raw = self.client.get_market(ticker)
        if not raw or not raw.get("ticker"):
            raw = {
                "ticker": ticker,
                "event_ticker": "",
                "title": ticker,
                "subtitle": "",
                "status": "unknown",
            }
        market = _to_market(raw)
        snapshot = self.refresh_orderbook(ticker)
        best_quotes = best_quotes_in_cents(snapshot)
        _apply_quote_fallbacks(market, best_quotes)
        market.metadata["quote_diagnostics"] = {
            "ticker": market.ticker,
            "title": market.title,
            "event_ticker": market.event_ticker,
            "yes_bid": market.yes_bid,
            "yes_ask": market.yes_ask,
            "no_bid": market.no_bid,
            "no_ask": market.no_ask,
            "orderbook_snapshot_exists": snapshot is not None,
            **best_quotes,
            "missing_fields": _missing_quote_fields(market),
        }
        return market

    def refresh_market_observation(self, ticker: str) -> dict:
        """Refresh a single tracked market with REST-only observability fields.

        Kalshi's binary orderbook publishes YES bids and NO bids. Executable asks are
        inferred from the opposite side:
        - YES ask = 100 - best NO bid
        - NO ask = 100 - best YES bid
        This helper preserves that rule and exposes whether the market has a usable
        two-sided executable quote even when midpoint probability is unavailable.
        """
        market = self.refresh_market_by_ticker(ticker)
        snapshot = self.orderbooks.get(ticker)
        quote_diag = dict(market.metadata.get("quote_diagnostics", {}))
        last_trade = self._latest_trade(ticker)
        observed_yes_bid = quote_diag.get("best_yes_bid")
        observed_no_bid = quote_diag.get("best_no_bid")
        observed_yes_ask = quote_diag.get("best_yes_ask")
        observed_no_ask = quote_diag.get("best_no_ask")
        has_two_sided_quote = observed_yes_bid is not None and observed_yes_ask is not None
        spread = None
        if has_two_sided_quote:
            spread = float(observed_yes_ask) - float(observed_yes_bid)
        quote_age_seconds = None
        updated_at = _parse_market_timestamp(market.metadata)
        if updated_at is not None:
            quote_age_seconds = max(0.0, (datetime.now(timezone.utc) - updated_at).total_seconds())
        return {
            "market": market,
            "snapshot": snapshot,
            "midpoint_probability": _safe_midpoint_probability(market),
            "observed_yes_bid": observed_yes_bid,
            "observed_no_bid": observed_no_bid,
            "observed_yes_ask_inferred": observed_yes_ask,
            "observed_no_ask_inferred": observed_no_ask,
            "observed_spread": spread,
            "observed_has_two_sided_quote": has_two_sided_quote,
            "observed_last_trade_price": last_trade.get("price"),
            "observed_last_trade_ts": last_trade.get("ts"),
            "observed_quote_age_seconds": quote_age_seconds,
            "observed_trade_count": last_trade.get("count", 0),
        }

    def _latest_trade(self, ticker: str) -> dict:
        try:
            trades = self.client.get_trades(ticker, limit=1)
        except Exception:
            return {"price": None, "ts": None, "count": 0, "unsupported": True}
        if not trades:
            return {"price": None, "ts": None, "count": 0, "unsupported": False}
        trade = trades[0]
        price = _price_field(trade, "price")
        ts = None
        for key in ("created_time", "created_at", "timestamp", "ts"):
            raw = trade.get(key)
            if not raw:
                continue
            try:
                ts = datetime.fromisoformat(str(raw).replace("Z", "+00:00")).astimezone(timezone.utc).isoformat()
                break
            except ValueError:
                continue
        return {"price": price, "ts": ts, "count": len(trades), "unsupported": False}

    def hydrate_structural_quotes(self, markets: list[Market]) -> list[Market]:
        usable: list[Market] = []
        dead_markets = 0
        orderbook_snapshots = 0
        orderbook_failures = 0
        stats = {
            "summary_complete": 0,
            "summary_partial": 0,
            "orderbook_complete": 0,
            "orderbook_partial": 0,
            "enriched_from_orderbook": 0,
            "dead_after_enrichment": 0,
        }
        for market in markets:
            summary_missing = _missing_quote_fields(market)
            if not summary_missing:
                stats["summary_complete"] += 1
            elif len(summary_missing) < 4:
                stats["summary_partial"] += 1
            snapshot = None
            try:
                snapshot = self.refresh_orderbook(market.ticker)
                orderbook_snapshots += 1
            except Exception:
                orderbook_failures += 1
            best_quotes = best_quotes_in_cents(snapshot)
            if snapshot is not None:
                orderbook_missing = [name for name, value in best_quotes.items() if value is None]
                if not orderbook_missing:
                    stats["orderbook_complete"] += 1
                elif len(orderbook_missing) < 4:
                    stats["orderbook_partial"] += 1
            enriched = _apply_quote_fallbacks(market, best_quotes)
            if enriched:
                stats["enriched_from_orderbook"] += 1
            missing_after = _missing_quote_fields(market)
            has_executable_quotes = _has_executable_quotes(market)
            diagnostics = {
                "ticker": market.ticker,
                "title": market.title,
                "event_ticker": market.event_ticker,
                "yes_bid": market.yes_bid,
                "yes_ask": market.yes_ask,
                "no_bid": market.no_bid,
                "no_ask": market.no_ask,
                "orderbook_snapshot_exists": snapshot is not None,
                **best_quotes,
                "missing_fields": missing_after,
            }
            market.metadata["quote_diagnostics"] = diagnostics
            if not has_executable_quotes:
                dead_markets += 1
                stats["dead_after_enrichment"] += 1
                continue
            usable.append(market)
        self.last_quote_stats = QuoteRefreshStats(
            usable_markets=len(usable),
            dead_markets_skipped=dead_markets,
            orderbook_snapshots=orderbook_snapshots,
            orderbook_failures=orderbook_failures,
            quote_completeness_stats=stats,
        )
        return usable

    def build_ladders(self, markets: list[Market]) -> dict[str, list[Market]]:
        return build_ladders(markets)

    def filter_markets(self, markets: list[Market]) -> MarketFilterResult:
        if self.settings is None:
            return MarketFilterResult(included=markets, excluded=[], diagnostics=[])
        return filter_structural_markets(markets, self.settings)


def _series_matches_macro(item: dict, allowed_terms: list[str]) -> bool:
    category = str(item.get("category", "")).lower()
    tags = item.get("tags", [])
    tag_text = " ".join(tags if isinstance(tags, list) else [])
    text = " ".join([str(item.get("ticker", "")), str(item.get("title", "")), category, tag_text]).lower()
    if category in {"economics", "financials"} and any(term in text for term in allowed_terms):
        return True
    # allow a few explicitly macro/policy political series like fed chair/rate cuts
    if any(term in text for term in ("fed", "fomc", "rate", "cpi", "inflation", "treasury", "yield", "unemployment", "gdp")):
        return True
    return False


def _series_priority(item: dict, allowed_terms: list[str]) -> tuple[int, str]:
    category = str(item.get("category", "")).lower()
    text = " ".join(
        [
            str(item.get("ticker", "")),
            str(item.get("title", "")),
            category,
            " ".join(item.get("tags", []) if isinstance(item.get("tags", []), list) else []),
        ]
    ).lower()
    score = 0
    if category in {"economics", "financials"}:
        score += 5
    for term in allowed_terms:
        if term in text:
            score += 4
    for term in ("how high", "how low", "above", "below", "range", "between", "max", "min"):
        if term in text:
            score += 5
    for term in ("unemployment", "cpi", "inflation", "yield", "gdp", "effr", "fed decision", "rate cut", "rate hike"):
        if term in text:
            score += 6
    for term in ("chair", "confirm", "trump", "end the federal reserve", "nominee"):
        if term in text:
            score -= 8
    return score, str(item.get("ticker", ""))


def _to_market(raw: dict) -> Market:
    return Market(
        ticker=raw["ticker"],
        event_ticker=raw.get("event_ticker", raw.get("eventTicker", "")),
        title=raw.get("title", raw["ticker"]),
        subtitle=raw.get("subtitle", ""),
        status=raw.get("status", "unknown"),
        yes_ask=_price_field(raw, "yes_ask"),
        yes_bid=_price_field(raw, "yes_bid"),
        no_ask=_price_field(raw, "no_ask"),
        no_bid=_price_field(raw, "no_bid"),
        expiry=_parse_expiry(raw),
        metadata=raw,
    )


def _parse_expiry(raw: dict) -> datetime | None:
    for key in ("expiration_time", "close_time", "expirationTime", "closeTime", "settlement_time"):
        value = raw.get(key)
        if not value:
            continue
        try:
            return datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        except ValueError:
            continue
    return None


def _parse_market_timestamp(raw: dict) -> datetime | None:
    for key in ("updated_time", "updated_at", "last_updated_time", "updatedTime"):
        value = raw.get(key)
        if not value:
            continue
        try:
            parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
            if parsed.tzinfo is None:
                return parsed.replace(tzinfo=timezone.utc)
            return parsed.astimezone(timezone.utc)
        except ValueError:
            continue
    return None


def _safe_midpoint_probability(market: Market) -> float | None:
    if market.yes_bid is not None and market.yes_ask is not None:
        return max(0.0, min(1.0, (float(market.yes_bid) + float(market.yes_ask)) / 200.0))
    if market.no_bid is not None and market.no_ask is not None:
        yes_mid = 100.0 - ((float(market.no_bid) + float(market.no_ask)) / 2.0)
        return max(0.0, min(1.0, yes_mid / 100.0))
    return None


def _price_field(raw: dict, key: str) -> float | None:
    for candidate in (key, f"{key}_dollars", key.replace("_", "") + "_dollars", key.replace("_", "")):
        value = raw.get(candidate)
        if value is None:
            continue
        number = float(value)
        if number <= 1.0:
            number *= 100.0
        return number
    return None


def _apply_quote_fallbacks(market: Market, best_quotes: dict[str, int | None]) -> bool:
    enriched = False
    field_map = {
        "yes_bid": "best_yes_bid",
        "yes_ask": "best_yes_ask",
        "no_bid": "best_no_bid",
        "no_ask": "best_no_ask",
    }
    for market_field, quote_field in field_map.items():
        best_value = best_quotes.get(quote_field)
        if best_value is None:
            continue
        if getattr(market, market_field) != best_value:
            setattr(market, market_field, best_value)
            enriched = True
    return enriched


def _missing_quote_fields(market: Market) -> list[str]:
    missing: list[str] = []
    for field_name in ("yes_bid", "yes_ask", "no_bid", "no_ask"):
        if getattr(market, field_name) is None:
            missing.append(field_name)
    return missing


def _has_executable_quotes(market: Market) -> bool:
    return any(getattr(market, field_name) is not None for field_name in ("yes_bid", "yes_ask", "no_bid", "no_ask"))
