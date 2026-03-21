from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import Awaitable, Callable

import websockets

from app.config import Settings
from app.kalshi.auth import KalshiAuthAdapter


class KalshiWsClient:
    def __init__(self, settings: Settings, logger: logging.Logger) -> None:
        self.settings = settings
        self.logger = logger
        self._running = False
        self.auth = KalshiAuthAdapter(settings)

    async def stream(self, subscribe_payload: dict, handler: Callable[[dict], Awaitable[None]]) -> None:
        self._running = True
        backoff = 1
        while self._running:
            try:
                async with websockets.connect(
                    self.settings.kalshi_ws_url,
                    additional_headers=self.auth.build_websocket_headers() or None,
                    ping_interval=20,
                    ping_timeout=20,
                ) as ws:
                    await ws.send(json.dumps(subscribe_payload))
                    self.logger.info("ws_connected", extra={"event": {"type": "ws_connected"}})
                    backoff = 1
                    async for raw in ws:
                        await handler(json.loads(raw))
            except Exception as exc:
                self.logger.error("ws_error", extra={"event": {"type": "ws_error", "error": str(exc)}})
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 60)

    def stop(self) -> None:
        self._running = False
