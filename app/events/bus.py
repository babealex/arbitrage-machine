from __future__ import annotations

from collections import defaultdict
from typing import Any, Callable


Handler = Callable[[Any], None]


class InProcessEventBus:
    def __init__(self) -> None:
        self._handlers: dict[str, list[Handler]] = defaultdict(list)

    def subscribe(self, topic: str, handler: Handler) -> None:
        self._handlers[topic].append(handler)

    def publish(self, topic: str, payload: Any) -> None:
        for handler in self._handlers.get(topic, []):
            handler(payload)
