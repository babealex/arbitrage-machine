from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal

from app.options.models import OptionStateVector


@dataclass(frozen=True, slots=True)
class OptionRegime:
    name: str
    prefer_calendars: bool
    allow_long_convexity: bool
    allow_downside_skew_trades: bool


def classify_regime(state_vector: OptionStateVector) -> OptionRegime:
    term_slope = state_vector.atm_iv_term_slope or Decimal("0")
    event_premium = state_vector.event_premium_proxy or Decimal("0")
    vrp = state_vector.vrp_proxy_near or Decimal("0")
    if event_premium > Decimal("0") and term_slope < Decimal("0"):
        return OptionRegime(
            name="event_term_structure",
            prefer_calendars=True,
            allow_long_convexity=False,
            allow_downside_skew_trades=False,
        )
    if vrp < Decimal("0") or not state_vector.total_variance_term_monotonic:
        return OptionRegime(
            name="unstable_surface",
            prefer_calendars=False,
            allow_long_convexity=False,
            allow_downside_skew_trades=False,
        )
    if term_slope < Decimal("0"):
        return OptionRegime(
            name="downside_skew",
            prefer_calendars=False,
            allow_long_convexity=False,
            allow_downside_skew_trades=True,
        )
    return OptionRegime(
        name="balanced",
        prefer_calendars=False,
        allow_long_convexity=True,
        allow_downside_skew_trades=False,
    )
