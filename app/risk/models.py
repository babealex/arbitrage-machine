from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

from app.core.time import utc_now


@dataclass(frozen=True, slots=True)
class RiskTransitionEvent:
    state: str
    reason: str
    details: dict[str, Any] = field(default_factory=dict)
    occurred_at: datetime = field(default_factory=utc_now)

