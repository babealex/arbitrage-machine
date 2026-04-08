from __future__ import annotations

from decimal import Decimal

from app.execution.orders import build_order_intent
from app.instruments.models import InstrumentRef
from app.strategies.convexity.models import ConvexTradeCandidate, ConvexityAccountState
from app.strategies.convexity.risk import passes_convexity_risk_limits, risk_budget_for_trade


def select_convexity_orders(
    candidates: list[ConvexTradeCandidate],
    account_state: ConvexityAccountState,
    *,
    max_risk_per_trade_pct: Decimal,
    max_total_exposure_pct: Decimal,
    max_open_trades: int,
    max_symbol_exposure_pct: Decimal,
    max_correlated_event_exposure_pct: Decimal,
    top_n: int = 3,
) -> list:
    selected = []
    sorted_candidates = sorted(candidates, key=lambda item: item.convex_score, reverse=True)
    for candidate in sorted_candidates:
        budget = risk_budget_for_trade(account_state.nav, max_risk_per_trade_pct)
        if candidate.max_loss > budget:
            continue
        okay, _reason = passes_convexity_risk_limits(
            candidate=candidate,
            account_state=account_state,
            max_total_exposure_pct=max_total_exposure_pct,
            max_open_trades=max_open_trades,
            max_symbol_exposure_pct=max_symbol_exposure_pct,
            max_correlated_event_exposure_pct=max_correlated_event_exposure_pct,
        )
        if not okay:
            continue
        trade_id = candidate.candidate_id
        for index, leg in enumerate(candidate.legs):
            qty = abs(int(leg["quantity"]))
            if qty <= 0:
                continue
            selected.append(
                build_order_intent(
                    instrument=InstrumentRef(symbol=str(leg["symbol"]), venue="listed_option"),
                    side="buy" if int(leg["quantity"]) > 0 else "sell",
                    quantity=qty,
                    order_type="limit",
                    strategy_name="convexity",
                    signal_ref=trade_id,
                    seed=f"{trade_id}:{index}",
                    limit_price=Decimal(str(leg["premium"])),
                    time_in_force="DAY",
                )
            )
        if len({intent.signal_ref for intent in selected}) >= top_n:
            break
    return selected
