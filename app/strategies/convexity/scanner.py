from __future__ import annotations

from datetime import timedelta
from decimal import Decimal

from app.strategies.convexity.expected_move import implied_move_from_atm_iv, implied_move_from_atm_straddle, move_edge, realized_event_move_estimate
from app.strategies.convexity.models import ConvexityOpportunity, OptionContractQuote


def scan_convexity_opportunities(market_state, calendars, news_state) -> list[ConvexityOpportunity]:
    opportunities: list[ConvexityOpportunity] = []
    for symbol, state in market_state.items():
        spot = state["spot_price"]
        quotes: list[OptionContractQuote] = state["option_chain"]
        realized_vol = state["realized_vol_pct"]
        implied_vol = state.get("implied_vol_pct")
        breakout_score = state.get("breakout_score")
        volume_spike_ratio = state.get("volume_spike_ratio")
        implied_move = implied_move_from_atm_straddle(quotes=quotes, spot_price=spot)
        if implied_move is None:
            implied_move = implied_move_from_atm_iv(atm_iv=implied_vol, days_to_expiry=_nearest_dte(quotes, state["as_of"].date()))
        if implied_move is None:
            continue
        for event in calendars.get(symbol, []):
            days = (event["event_time"].date() - state["as_of"].date()).days
            if event["setup_type"] == "earnings" and 3 <= days <= 10:
                expected = realized_event_move_estimate(
                    realized_vol_lookahead_estimate=realized_vol,
                    event_history_median_move=Decimal(str(event.get("event_history_median_move", realized_vol))),
                    breakout_adjusted_move=Decimal(str(event.get("breakout_adjusted_move", breakout_score or Decimal("0")))),
                )
                if move_edge(expected_move_pct=expected, implied_move_pct=implied_move) > Decimal("0"):
                    opportunities.append(
                        ConvexityOpportunity(
                            symbol=symbol,
                            setup_type="earnings",
                            event_time=event["event_time"],
                            spot_price=spot,
                            expected_move_pct=expected,
                            implied_move_pct=implied_move,
                            realized_vol_pct=realized_vol,
                            implied_vol_pct=implied_vol or Decimal("0"),
                            volume_spike_ratio=volume_spike_ratio,
                            breakout_score=breakout_score,
                            metadata={"direction": event.get("direction", "neutral")},
                        )
                    )
            if event["setup_type"] == "macro" and 1 <= days <= 5:
                expected = realized_event_move_estimate(
                    realized_vol_lookahead_estimate=realized_vol,
                    event_history_median_move=Decimal(str(event.get("event_history_median_move", realized_vol))),
                    breakout_adjusted_move=Decimal(str(event.get("breakout_adjusted_move", breakout_score or Decimal("0")))),
                )
                if move_edge(expected_move_pct=expected, implied_move_pct=implied_move) > Decimal("0"):
                    opportunities.append(
                        ConvexityOpportunity(
                            symbol=symbol,
                            setup_type="macro",
                            event_time=event["event_time"],
                            spot_price=spot,
                            expected_move_pct=expected,
                            implied_move_pct=implied_move,
                            realized_vol_pct=realized_vol,
                            implied_vol_pct=implied_vol or Decimal("0"),
                            volume_spike_ratio=volume_spike_ratio,
                            breakout_score=breakout_score,
                            metadata={"direction": event.get("direction", "neutral")},
                        )
                    )
        if volume_spike_ratio is not None and breakout_score is not None and volume_spike_ratio >= Decimal("1.5") and breakout_score > Decimal("0"):
            if implied_vol is None or implied_vol <= realized_vol * Decimal("1.25"):
                opportunities.append(
                    ConvexityOpportunity(
                        symbol=symbol,
                        setup_type="momentum",
                        event_time=None,
                        spot_price=spot,
                        expected_move_pct=max(realized_vol, breakout_score),
                        implied_move_pct=implied_move,
                        realized_vol_pct=realized_vol,
                        implied_vol_pct=implied_vol or Decimal("0"),
                        volume_spike_ratio=volume_spike_ratio,
                        breakout_score=breakout_score,
                        metadata={"direction": news_state.get(symbol, {}).get("direction", "bullish")},
                    )
                )
    return opportunities


def _nearest_dte(quotes: list[OptionContractQuote], as_of) -> int:
    expiries = sorted({quote.expiry for quote in quotes})
    if not expiries:
        return 0
    return (expiries[0] - as_of).days
