from __future__ import annotations

from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timezone

from app.market_data.macro_fetcher import MacroPrices
from app.models import Market


@dataclass(slots=True)
class CrossMarketCycleStats:
    snapshots_collected: int = 0
    macro_prices: dict[str, float | None] = field(default_factory=dict)
    avg_disagreement_score: float = 0.0
    highest_disagreement_events: list[dict] = field(default_factory=list)


class CrossMarketSnapshotEngine:
    def __init__(self, families: list[str], history_limit: int = 64) -> None:
        self.families = tuple(families)
        self.history_limit = history_limit
        self._macro_history: dict[str, deque[float]] = {
            "spy": deque(maxlen=history_limit),
            "qqq": deque(maxlen=history_limit),
            "tlt": deque(maxlen=history_limit),
            "btc": deque(maxlen=history_limit),
        }

    def build_snapshots(self, markets: list[Market], macro_prices: MacroPrices) -> tuple[list[dict], CrossMarketCycleStats]:
        self._update_history(macro_prices)
        risk_score = self.compute_risk_score(macro_prices)
        snapshots: list[dict] = []
        disagreements: list[dict] = []
        relevant = [market for market in markets if market.ticker.startswith(self.families)]
        for market in relevant:
            midpoint = midpoint_probability(market)
            if midpoint is None:
                continue
            quote_diag = dict(market.metadata.get("quote_diagnostics", {}))
            contract_pair = market.ticker
            disagreement = compute_disagreement_score(midpoint, risk_score)
            snapshot = {
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "event_ticker": market.event_ticker,
                "contract_pair": contract_pair,
                "kalshi_probability": midpoint,
                "kalshi_bid": float(market.yes_bid or 0.0),
                "kalshi_ask": float(market.yes_ask or 0.0),
                "kalshi_yes_bid_size": int(quote_diag.get("best_yes_bid_size") or 0),
                "kalshi_yes_ask_size": int(quote_diag.get("best_yes_ask_size") or 0),
                "spy_price": macro_prices.spy_price,
                "qqq_price": macro_prices.qqq_price,
                "tlt_price": macro_prices.tlt_price,
                "btc_price": macro_prices.btc_price,
                "derived_risk_score": disagreement,
                "notes": f"risk_score={risk_score:.6f}",
            }
            snapshots.append(snapshot)
            disagreements.append(
                {
                    "event_ticker": market.event_ticker,
                    "ticker": market.ticker,
                    "contract_pair": contract_pair,
                    "kalshi_probability": midpoint,
                    "risk_score": risk_score,
                    "disagreement_score": disagreement,
                }
            )
        avg_disagreement = sum(item["disagreement_score"] for item in disagreements) / len(disagreements) if disagreements else 0.0
        disagreements.sort(key=lambda item: item["disagreement_score"], reverse=True)
        stats = CrossMarketCycleStats(
            snapshots_collected=len(snapshots),
            macro_prices={
                "spy_price": macro_prices.spy_price,
                "qqq_price": macro_prices.qqq_price,
                "tlt_price": macro_prices.tlt_price,
                "btc_price": macro_prices.btc_price,
                "risk_score": risk_score,
            },
            avg_disagreement_score=avg_disagreement,
            highest_disagreement_events=disagreements[:10],
        )
        return snapshots, stats

    def compute_risk_score(self, macro_prices: MacroPrices) -> float:
        normalized = {
            "spy": _normalize_change(_latest_pct_change(self._macro_history["spy"])),
            "qqq": _normalize_change(_latest_pct_change(self._macro_history["qqq"])),
            "tlt": _normalize_change(_latest_pct_change(self._macro_history["tlt"])),
            "btc": _normalize_change(_latest_pct_change(self._macro_history["btc"])),
        }
        return max(0.0, min(1.0, (normalized["spy"] + normalized["qqq"] + (1.0 - normalized["tlt"]) + normalized["btc"]) / 4.0))

    def _update_history(self, macro_prices: MacroPrices) -> None:
        values = {
            "spy": macro_prices.spy_price,
            "qqq": macro_prices.qqq_price,
            "tlt": macro_prices.tlt_price,
            "btc": macro_prices.btc_price,
        }
        for key, value in values.items():
            if value is not None:
                self._macro_history[key].append(float(value))


def midpoint_probability(market: Market) -> float | None:
    if market.yes_bid is not None and market.yes_ask is not None:
        return max(0.0, min(1.0, (float(market.yes_bid) + float(market.yes_ask)) / 200.0))
    if market.no_bid is not None and market.no_ask is not None:
        yes_mid = 100.0 - ((float(market.no_bid) + float(market.no_ask)) / 2.0)
        return max(0.0, min(1.0, yes_mid / 100.0))
    return None


def compute_disagreement_score(probability: float, risk_score: float) -> float:
    probability = max(0.0, min(1.0, probability))
    risk_score = max(0.0, min(1.0, risk_score))
    return abs(probability - risk_score)


def _latest_pct_change(history: deque[float]) -> float:
    if len(history) < 2:
        return 0.0
    previous = history[0]
    current = history[-1]
    if previous == 0:
        return 0.0
    return (current - previous) / previous


def _normalize_change(change: float, clamp: float = 0.03) -> float:
    bounded = max(-clamp, min(clamp, change))
    return (bounded + clamp) / (2 * clamp)
