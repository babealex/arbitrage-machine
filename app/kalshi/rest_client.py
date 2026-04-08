from __future__ import annotations

import json
import time
from typing import Any
from urllib.parse import urlencode

import requests

from app.config import Settings
from app.kalshi.auth import KalshiAuthAdapter


class KalshiRestClient:
    def __init__(self, settings: Settings, session: requests.Session | None = None) -> None:
        self.settings = settings
        self.session = session or requests.Session()
        self.auth = KalshiAuthAdapter(settings)
        self._warned_data_mode = False
        self.last_error: str | None = None
        self.last_error_path: str | None = None
        session_headers = getattr(self.session, "headers", None)
        if hasattr(session_headers, "setdefault"):
            session_headers.setdefault("User-Agent", "arbitrage-machine/0.1")

    @property
    def data_only_mode(self) -> bool:
        return not self.auth.configured()

    def _warn_data_mode(self) -> None:
        if self.data_only_mode and not self._warned_data_mode:
            self._warned_data_mode = True
            print('{"level":"WARNING","message":"kalshi_data_mode_no_auth"}')

    def _request(self, method: str, path: str, *, params: dict | None = None, payload: dict | None = None, auth: bool = False) -> dict[str, Any]:
        query = f"?{urlencode(params)}" if params else ""
        body = json.dumps(payload) if payload else ""
        url = f"{self.settings.kalshi_api_url}{path}{query}"
        headers = {"Content-Type": "application/json"}
        if auth:
            if self.data_only_mode:
                self._warn_data_mode()
                raise RuntimeError("kalshi_data_mode_no_auth")
            headers.update(self.auth.build_rest_headers(method, url, body))
        try:
            for attempt in range(self.settings.kalshi_request_retries + 1):
                try:
                    response = self.session.request(
                        method,
                        url,
                        data=body or None,
                        headers=headers,
                        timeout=self.settings.kalshi_request_timeout_seconds,
                    )
                    response.raise_for_status()
                    # Clear stale request-failure state after a successful call.
                    self.last_error = None
                    self.last_error_path = None
                    return response.json() if response.content else {}
                except Exception as exc:
                    self.last_error = str(exc)
                    self.last_error_path = path
                    if attempt >= self.settings.kalshi_request_retries:
                        if not auth:
                            return {}
                        raise
                    time.sleep(self.settings.kalshi_request_backoff_seconds)
        except Exception:
            if not auth:
                return {}
            raise

    def list_markets_page(self, status: str = "open", cursor: str | None = None) -> dict[str, Any]:
        params: dict[str, str] = {"status": status}
        if cursor:
            params["cursor"] = cursor
        return self._request("GET", "/markets", params=params, auth=False)

    def list_markets(self, status: str = "open") -> list[dict[str, Any]]:
        payload = self.list_markets_page(status=status)
        return payload.get("markets", payload.get("data", []))

    def list_series(self) -> list[dict[str, Any]]:
        payload = self._request("GET", "/series", auth=False)
        return payload.get("series", payload.get("data", []))

    def list_events_page(self, cursor: str | None = None, series_ticker: str | None = None) -> dict[str, Any]:
        params: dict[str, str] = {}
        if cursor:
            params["cursor"] = cursor
        if series_ticker:
            params["series_ticker"] = series_ticker
        return self._request("GET", "/events", params=params or None, auth=False)

    def get_orderbook(self, ticker: str, depth: int = 10) -> dict[str, Any]:
        return self._request("GET", f"/markets/{ticker}/orderbook", params={"depth": depth}, auth=False)

    def get_market(self, ticker: str) -> dict[str, Any]:
        payload = self._request("GET", f"/markets/{ticker}", auth=False)
        if "market" in payload:
            return payload["market"]
        if "data" in payload and isinstance(payload["data"], dict):
            return payload["data"]
        return payload

    def get_trades(self, ticker: str, limit: int = 1) -> list[dict[str, Any]]:
        payload = self._request("GET", "/markets/trades", params={"ticker": ticker, "limit": limit}, auth=False)
        return payload.get("trades", payload.get("data", []))

    def create_order(self, order: dict[str, Any]) -> dict[str, Any]:
        return self._request("POST", "/portfolio/orders", payload=order, auth=True)

    def get_order(self, order_id: str) -> dict[str, Any]:
        return self._request("GET", f"/portfolio/orders/{order_id}", auth=True)

    def list_orders(self, status: str | None = None) -> list[dict[str, Any]]:
        if self.data_only_mode:
            self._warn_data_mode()
            return []
        params = {"status": status} if status else None
        payload = self._request("GET", "/portfolio/orders", params=params, auth=True)
        return payload.get("orders", payload.get("data", []))

    def cancel_order(self, order_id: str) -> dict[str, Any]:
        return self._request("DELETE", f"/portfolio/orders/{order_id}", auth=True)

    def get_positions(self) -> list[dict[str, Any]]:
        if self.data_only_mode:
            self._warn_data_mode()
            return []
        payload = self._request("GET", "/portfolio/positions", auth=True)
        return payload.get("positions", payload.get("data", []))

    def get_balance(self) -> dict[str, Any]:
        if self.data_only_mode:
            self._warn_data_mode()
            return {}
        return self._request("GET", "/portfolio/balance", auth=True)

    def get_fills(self) -> list[dict[str, Any]]:
        if self.data_only_mode:
            self._warn_data_mode()
            return []
        payload = self._request("GET", "/portfolio/fills", auth=True)
        return payload.get("fills", payload.get("data", []))
