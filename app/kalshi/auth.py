from __future__ import annotations

import base64
import time
from pathlib import Path
from urllib.parse import urlparse

from app.config import Settings


class KalshiAuthAdapter:
    """
    Isolates current Kalshi signing assumptions behind one layer.

    Documented requirements change occasionally; callers should depend only on
    `build_rest_headers` and `build_websocket_headers`.
    """

    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self._private_key = None

    def configured(self) -> bool:
        return bool(self.settings.kalshi_api_key_id and self.settings.kalshi_private_key_path)

    def _load_private_key(self):
        if self._private_key is not None:
            return self._private_key
        if not self.settings.kalshi_private_key_path:
            return None
        from cryptography.hazmat.primitives import serialization

        key_bytes = Path(self.settings.kalshi_private_key_path).read_bytes()
        password = self.settings.kalshi_passphrase.encode() if self.settings.kalshi_passphrase else None
        self._private_key = serialization.load_pem_private_key(key_bytes, password=password)
        return self._private_key

    def _sign(self, method: str, canonical_path: str, body: str = "") -> dict[str, str]:
        from cryptography.hazmat.primitives import hashes
        from cryptography.hazmat.primitives.asymmetric import padding

        timestamp = str(int(time.time() * 1000))
        message = f"{timestamp}{method.upper()}{canonical_path}{body}".encode()
        signature = self._load_private_key().sign(
            message,
            padding.PSS(mgf=padding.MGF1(hashes.SHA256()), salt_length=padding.PSS.DIGEST_LENGTH),
            hashes.SHA256(),
        )
        return {
            "KALSHI-ACCESS-KEY": self.settings.kalshi_api_key_id,
            "KALSHI-ACCESS-TIMESTAMP": timestamp,
            "KALSHI-ACCESS-SIGNATURE": base64.b64encode(signature).decode(),
        }

    def build_rest_headers(self, method: str, url: str, body: str = "") -> dict[str, str]:
        if not self.configured():
            return {}
        parsed = urlparse(url)
        canonical_path = parsed.path
        return self._sign(method, canonical_path, body)

    def build_websocket_headers(self) -> dict[str, str]:
        if not self.configured():
            return {}
        parsed = urlparse(self.settings.kalshi_ws_url)
        return self._sign("GET", parsed.path, "")
