from __future__ import annotations

from dataclasses import asdict

from app.classification.service import EventClassifier
from app.events.models import NewsItem
from app.events.runtime import CONFIRMATION_SYMBOLS, confirm_trade
from app.execution.broker import PriceSnapshot
from app.mapping.engine import EventTradeMapper
from app.replay.market_data import HistoricalMarketData
from app.replay.simulator import ReplayTradeSimulator


class ReplayEngine:
    def __init__(
        self,
        classifier: EventClassifier,
        mapper: EventTradeMapper,
        market_data: HistoricalMarketData,
        simulator: ReplayTradeSimulator,
        confirmation_needed: int,
        scale_steps: int,
        scale_interval_seconds: int,
        min_severity: float,
        refresh_data: bool = False,
        cache_only: bool = False,
    ) -> None:
        self.classifier = classifier
        self.mapper = mapper
        self.market_data = market_data
        self.simulator = simulator
        self.confirmation_needed = confirmation_needed
        self.scale_steps = scale_steps
        self.scale_interval_seconds = scale_interval_seconds
        self.min_severity = min_severity
        self.refresh_data = refresh_data
        self.cache_only = cache_only

    def run(self, news_items: list[NewsItem]) -> list[dict]:
        results: list[dict] = []
        for item in news_items:
            classified = self.classifier.classify(item)
            if classified is None or classified.severity < self.min_severity:
                continue
            ideas = self.mapper.map_event(
                classified,
                scale_steps=self.scale_steps,
                scale_interval_seconds=self.scale_interval_seconds,
                confirmation_needed=self.confirmation_needed,
            )
            all_symbols = sorted(set(CONFIRMATION_SYMBOLS + classified.assets_affected + [idea.symbol for idea in ideas]))
            snapshots = self.market_data.batch_snapshot_for_event(
                all_symbols,
                item.timestamp_utc,
                refresh=self.refresh_data,
                cache_only=self.cache_only,
            )
            proxy_snapshots = {
                symbol: PriceSnapshot(
                    symbol=symbol,
                    price=snapshot.event_time_price.price,
                    previous_price=snapshot.pre_event_price.price,
                    ts=snapshot.event_time_price.timestamp_utc or item.timestamp_utc,
                )
                for symbol, snapshot in snapshots.items()
            }
            for idea in ideas:
                confirmation = confirm_trade(classified, idea, proxy_snapshots)
                trade_snapshot = snapshots.get(idea.symbol)
                simulated = self.simulator.simulate(idea, trade_snapshot) if trade_snapshot else None
                results.append(
                    {
                        "news_event": {
                            "external_id": item.external_id,
                            "timestamp_utc": item.timestamp_utc.isoformat(),
                            "source": item.source,
                            "headline": item.headline,
                            "body": item.body,
                            "url": item.url,
                        },
                        "classification": {
                            "event_type": classified.event_type,
                            "region": classified.region,
                            "severity": classified.severity,
                            "assets_affected": classified.assets_affected,
                            "directional_bias": classified.directional_bias,
                            "tags": classified.tags,
                            "rationale": classified.rationale,
                        },
                        "trade_idea": {
                            "symbol": idea.symbol,
                            "side": idea.side,
                            "confidence": idea.confidence,
                            "event_type": idea.event_type,
                            "thesis": idea.thesis,
                            "metadata": idea.metadata,
                        },
                        "confirmation": {
                            "passed": confirmation.passed,
                            "agreeing_signals": confirmation.agreeing_signals,
                            "checked_signals": confirmation.checked_signals,
                            "details": confirmation.details,
                        },
                        "simulation": asdict(simulated) if simulated else None,
                    }
                )
        return results
