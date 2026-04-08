from __future__ import annotations

from decimal import Decimal

from app.strategies.convexity.models import ConvexTradeCandidate, ConvexityAccountState


def risk_budget_for_trade(nav: Decimal, max_risk_per_trade_pct: Decimal, absolute_max: Decimal | None = None) -> Decimal:
    budget = nav * max_risk_per_trade_pct
    if absolute_max is not None:
        budget = min(budget, absolute_max)
    return budget


def passes_convexity_risk_limits(
    *,
    candidate: ConvexTradeCandidate,
    account_state: ConvexityAccountState,
    max_total_exposure_pct: Decimal,
    max_open_trades: int,
    max_symbol_exposure_pct: Decimal,
    max_correlated_event_exposure_pct: Decimal,
) -> tuple[bool, str | None]:
    if account_state.open_trade_count >= max_open_trades:
        return False, "max_open_trades"
    if account_state.open_convexity_risk + candidate.max_loss > account_state.nav * max_total_exposure_pct:
        return False, "max_total_exposure"
    if account_state.symbol_risk.get(candidate.symbol, Decimal("0")) + candidate.max_loss > account_state.nav * max_symbol_exposure_pct:
        return False, "max_symbol_exposure"
    bucket = str(candidate.metadata.get("correlated_event_bucket", candidate.symbol))
    if account_state.correlated_event_risk.get(bucket, Decimal("0")) + candidate.max_loss > account_state.nav * max_correlated_event_exposure_pct:
        return False, "max_correlated_event_exposure"
    return True, None
