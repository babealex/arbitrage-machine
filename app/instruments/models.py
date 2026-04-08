from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class InstrumentRef:
    symbol: str
    venue: str = "kalshi"
    contract_side: str | None = None

    def to_dict(self) -> dict[str, str]:
        payload = {"symbol": self.symbol, "venue": self.venue}
        if self.contract_side is not None:
            payload["contract_side"] = self.contract_side
        return payload

    @classmethod
    def from_dict(cls, payload: dict) -> "InstrumentRef":
        return cls(
            symbol=str(payload.get("symbol") or payload.get("ticker") or ""),
            venue=str(payload.get("venue") or "kalshi"),
            contract_side=str(payload["contract_side"]) if payload.get("contract_side") is not None else None,
        )
