from __future__ import annotations

from app.events.models import ClassifiedEvent, TradeIdea
from app.mapping.rules import MAPPING_RULES, MAPPING_VERSION


class EventTradeMapper:
    def map_event(self, event: ClassifiedEvent, scale_steps: int, scale_interval_seconds: int, confirmation_needed: int) -> list[TradeIdea]:
        ideas: list[TradeIdea] = []
        for rule in MAPPING_RULES:
            if rule.event_type != event.event_type:
                continue
            if rule.region and rule.region != event.region:
                continue
            if rule.directional_bias and rule.directional_bias != event.directional_bias:
                continue
            if any(tag not in event.tags for tag in rule.required_tags):
                continue
            for symbol in rule.long_symbols:
                ideas.append(
                    TradeIdea(
                        symbol=symbol,
                        side="buy",
                        confidence=min(1.0, rule.confidence * event.severity),
                        event_type=event.event_type,
                        thesis=rule.thesis,
                        confirmation_needed=confirmation_needed,
                        scale_steps=scale_steps,
                        scale_interval_seconds=scale_interval_seconds,
                        metadata={"mapping_rule": rule.name, "mapping_version": MAPPING_VERSION},
                    )
                )
            for symbol in rule.short_symbols:
                ideas.append(
                    TradeIdea(
                        symbol=symbol,
                        side="sell",
                        confidence=min(1.0, rule.confidence * event.severity),
                        event_type=event.event_type,
                        thesis=rule.thesis,
                        confirmation_needed=confirmation_needed,
                        scale_steps=scale_steps,
                        scale_interval_seconds=scale_interval_seconds,
                        metadata={"mapping_rule": rule.name, "mapping_version": MAPPING_VERSION},
                    )
                )
        return ideas
