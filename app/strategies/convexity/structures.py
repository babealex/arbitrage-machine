from __future__ import annotations

from datetime import date
from decimal import Decimal

from app.strategies.convexity.expected_move import opportunity_move_edge
from app.strategies.convexity.models import ConvexTradeCandidate, ConvexityAccountState, ConvexityOpportunity, OptionContractQuote
from app.strategies.convexity.scorer import convex_score, estimate_all_in_cost, liquidity_score, scenario_expected_value


def build_convex_trade_candidates(
    opportunity: ConvexityOpportunity,
    option_chain: list[OptionContractQuote],
    account_state: ConvexityAccountState,
    *,
    allowed_dte_min: int = 7,
    allowed_dte_max: int = 30,
    min_oi: int = 100,
    min_volume: int = 10,
    max_bid_ask_pct_of_debit: Decimal = Decimal("0.25"),
    commission_cost: Decimal = Decimal("1"),
    slippage_multiplier: Decimal = Decimal("1"),
    min_reward_to_risk: Decimal = Decimal("5"),
    min_expected_value_after_costs: Decimal = Decimal("0"),
) -> list[ConvexTradeCandidate]:
    accepted, _, _ = evaluate_convex_trade_candidates(
        opportunity,
        option_chain,
        account_state,
        allowed_dte_min=allowed_dte_min,
        allowed_dte_max=allowed_dte_max,
        min_oi=min_oi,
        min_volume=min_volume,
        max_bid_ask_pct_of_debit=max_bid_ask_pct_of_debit,
        commission_cost=commission_cost,
        slippage_multiplier=slippage_multiplier,
        min_reward_to_risk=min_reward_to_risk,
        min_expected_value_after_costs=min_expected_value_after_costs,
    )
    return accepted


def evaluate_convex_trade_candidates(
    opportunity: ConvexityOpportunity,
    option_chain: list[OptionContractQuote],
    account_state: ConvexityAccountState,
    *,
    allowed_dte_min: int = 7,
    allowed_dte_max: int = 30,
    min_oi: int = 100,
    min_volume: int = 10,
    max_bid_ask_pct_of_debit: Decimal = Decimal("0.25"),
    commission_cost: Decimal = Decimal("1"),
    slippage_multiplier: Decimal = Decimal("1"),
    min_reward_to_risk: Decimal = Decimal("5"),
    min_expected_value_after_costs: Decimal = Decimal("0"),
) -> tuple[list[ConvexTradeCandidate], dict[str, str], list[ConvexTradeCandidate]]:
    expiries = _eligible_expiries(option_chain, allowed_dte_min, allowed_dte_max, opportunity.event_time)
    candidates: list[ConvexTradeCandidate] = []
    for expiry in expiries:
        expiry_quotes = [quote for quote in option_chain if quote.expiry == expiry]
        if opportunity.setup_type in {"earnings", "macro"} and opportunity_move_edge(opportunity) > 0:
            straddle = _build_straddle_candidate(opportunity, expiry_quotes, expiry)
            if straddle is not None:
                candidates.append(straddle)
        directional = _build_directional_candidate(opportunity, expiry_quotes, expiry)
        if directional is not None:
            candidates.append(directional)
        spread = _build_spread_candidate(opportunity, expiry_quotes, expiry)
        if spread is not None:
            candidates.append(spread)
    accepted: list[ConvexTradeCandidate] = []
    rejected: dict[str, str] = {}
    evaluated: list[ConvexTradeCandidate] = []
    for candidate in candidates:
        quotes = _candidate_quotes(candidate, option_chain)
        liq = liquidity_score(quotes, min_oi=min_oi, min_volume=min_volume)
        all_in_cost = Decimal("0") if liq <= 0 else estimate_all_in_cost(quotes=quotes, commission_cost=commission_cost, slippage_multiplier=slippage_multiplier)
        bid_ask_pct = Decimal("0") if candidate.debit <= 0 else all_in_cost / candidate.debit
        ev, ev_after = scenario_expected_value(candidate=candidate, opportunity=opportunity, quotes=quotes)
        reward_to_risk = candidate.reward_to_risk
        enriched = ConvexTradeCandidate(
            symbol=candidate.symbol,
            setup_type=candidate.setup_type,
            structure_type=candidate.structure_type,
            expiry=candidate.expiry,
            legs=candidate.legs,
            debit=candidate.debit,
            max_loss=candidate.max_loss,
            max_profit_estimate=candidate.max_profit_estimate,
            reward_to_risk=reward_to_risk,
            expected_value=ev,
            expected_value_after_costs=ev_after,
            convex_score=Decimal("0"),
            breakout_sensitivity=candidate.breakout_sensitivity,
            liquidity_score=liq,
            confidence_score=candidate.confidence_score,
            metadata={**candidate.metadata, "all_in_cost": all_in_cost, "bid_ask_pct_of_debit": bid_ask_pct},
        )
        scored = ConvexTradeCandidate(
            symbol=enriched.symbol,
            setup_type=enriched.setup_type,
            structure_type=enriched.structure_type,
            expiry=enriched.expiry,
            legs=enriched.legs,
            debit=enriched.debit,
            max_loss=enriched.max_loss,
            max_profit_estimate=enriched.max_profit_estimate,
            reward_to_risk=enriched.reward_to_risk,
            expected_value=enriched.expected_value,
            expected_value_after_costs=enriched.expected_value_after_costs,
            convex_score=convex_score(candidate=enriched, opportunity=opportunity),
            breakout_sensitivity=enriched.breakout_sensitivity,
            liquidity_score=enriched.liquidity_score,
            confidence_score=enriched.confidence_score,
            metadata=enriched.metadata,
        )
        evaluated.append(scored)
        reason: str | None = None
        if liq <= 0:
            reason = "liquidity_too_low"
        elif bid_ask_pct > max_bid_ask_pct_of_debit:
            reason = "spread_too_wide"
        elif reward_to_risk < min_reward_to_risk:
            reason = "reward_to_risk_too_low"
        elif ev_after <= min_expected_value_after_costs:
            reason = "negative_ev_after_costs"
        if reason is None:
            accepted.append(scored)
        else:
            rejected[scored.candidate_id] = reason
    accepted_sorted = sorted(accepted, key=lambda item: item.convex_score, reverse=True)
    evaluated_sorted = sorted(evaluated, key=lambda item: item.convex_score, reverse=True)
    return accepted_sorted, rejected, evaluated_sorted


def _eligible_expiries(option_chain: list[OptionContractQuote], allowed_dte_min: int, allowed_dte_max: int, event_time) -> list[date]:
    if not option_chain:
        return []
    expiries = sorted({quote.expiry for quote in option_chain})
    if event_time is None:
        return expiries
    origin = event_time.date()
    return [expiry for expiry in expiries if allowed_dte_min <= (expiry - origin).days <= allowed_dte_max]


def _build_directional_candidate(opportunity: ConvexityOpportunity, quotes: list[OptionContractQuote], expiry: date) -> ConvexTradeCandidate | None:
    preferred_type = "call" if opportunity.metadata.get("direction", "bullish") != "bearish" else "put"
    filtered = [quote for quote in quotes if quote.option_type == preferred_type and quote.delta is not None and Decimal("0.15") <= abs(quote.delta) <= Decimal("0.35")]
    if not filtered:
        return None
    selected = min(filtered, key=lambda quote: abs(abs(quote.delta or Decimal("0.25")) - Decimal("0.25")))
    max_gain = _intrinsic_target_gain(opportunity, selected)
    return ConvexTradeCandidate(
        symbol=opportunity.symbol,
        setup_type=opportunity.setup_type,
        structure_type="long_call" if preferred_type == "call" else "long_put",
        expiry=expiry,
        legs=[_leg_payload(selected, 1)],
        debit=selected.ask * Decimal("100"),
        max_loss=selected.ask * Decimal("100"),
        max_profit_estimate=max_gain,
        reward_to_risk=Decimal("0") if selected.ask <= 0 else max_gain / (selected.ask * Decimal("100")),
        expected_value=Decimal("0"),
        expected_value_after_costs=Decimal("0"),
        convex_score=Decimal("0"),
        breakout_sensitivity=opportunity.breakout_score or Decimal("0"),
        liquidity_score=Decimal("0"),
        confidence_score=max(Decimal("0"), opportunity_move_edge(opportunity)),
        metadata={"direction": opportunity.metadata.get("direction", "bullish"), "event_time": opportunity.event_time},
    )


def _build_spread_candidate(opportunity: ConvexityOpportunity, quotes: list[OptionContractQuote], expiry: date) -> ConvexTradeCandidate | None:
    direction = opportunity.metadata.get("direction", "bullish")
    option_type = "call" if direction != "bearish" else "put"
    filtered = sorted(
        [quote for quote in quotes if quote.option_type == option_type and quote.delta is not None and Decimal("0.15") <= abs(quote.delta) <= Decimal("0.35")],
        key=lambda quote: quote.strike,
    )
    if len(filtered) < 2:
        return None
    long_leg = filtered[0] if option_type == "call" else filtered[-1]
    short_leg = filtered[1] if option_type == "call" else filtered[-2]
    debit = max(Decimal("0"), (long_leg.ask - short_leg.bid) * Decimal("100"))
    width = abs(short_leg.strike - long_leg.strike) * Decimal("100")
    max_gain = max(Decimal("0"), width - debit)
    return ConvexTradeCandidate(
        symbol=opportunity.symbol,
        setup_type=opportunity.setup_type,
        structure_type="call_spread" if option_type == "call" else "put_spread",
        expiry=expiry,
        legs=[_leg_payload(long_leg, 1), _leg_payload(short_leg, -1)],
        debit=debit,
        max_loss=debit,
        max_profit_estimate=max_gain,
        reward_to_risk=Decimal("0") if debit <= 0 else max_gain / debit,
        expected_value=Decimal("0"),
        expected_value_after_costs=Decimal("0"),
        convex_score=Decimal("0"),
        breakout_sensitivity=opportunity.breakout_score or Decimal("0"),
        liquidity_score=Decimal("0"),
        confidence_score=max(Decimal("0"), opportunity_move_edge(opportunity)),
        metadata={"direction": direction, "event_time": opportunity.event_time},
    )


def _build_straddle_candidate(opportunity: ConvexityOpportunity, quotes: list[OptionContractQuote], expiry: date) -> ConvexTradeCandidate | None:
    strikes = sorted({quote.strike for quote in quotes})
    if not strikes:
        return None
    atm = min(strikes, key=lambda strike: abs(strike - opportunity.spot_price))
    call = next((quote for quote in quotes if quote.strike == atm and quote.option_type == "call"), None)
    put = next((quote for quote in quotes if quote.strike == atm and quote.option_type == "put"), None)
    if call is None or put is None:
        return None
    debit = (call.ask + put.ask) * Decimal("100")
    max_gain = max(Decimal("0"), (opportunity.expected_move_pct * opportunity.spot_price * Decimal("100")) - debit)
    return ConvexTradeCandidate(
        symbol=opportunity.symbol,
        setup_type=opportunity.setup_type,
        structure_type="straddle",
        expiry=expiry,
        legs=[_leg_payload(call, 1), _leg_payload(put, 1)],
        debit=debit,
        max_loss=debit,
        max_profit_estimate=max_gain,
        reward_to_risk=Decimal("0") if debit <= 0 else max_gain / debit,
        expected_value=Decimal("0"),
        expected_value_after_costs=Decimal("0"),
        convex_score=Decimal("0"),
        breakout_sensitivity=opportunity.breakout_score or Decimal("0"),
        liquidity_score=Decimal("0"),
        confidence_score=max(Decimal("0"), opportunity_move_edge(opportunity)),
        metadata={"direction": "neutral", "event_time": opportunity.event_time},
    )


def _candidate_quotes(candidate: ConvexTradeCandidate, option_chain: list[OptionContractQuote]) -> list[OptionContractQuote]:
    keys = {(leg["expiry"], Decimal(str(leg["strike"])), leg["option_type"]) for leg in candidate.legs}
    return [
        quote
        for quote in option_chain
        if (quote.expiry, quote.strike, quote.option_type) in keys
    ]


def _leg_payload(quote: OptionContractQuote, quantity: int) -> dict:
    return {
        "symbol": quote.symbol,
        "expiry": quote.expiry,
        "strike": quote.strike,
        "option_type": quote.option_type,
        "quantity": quantity,
        "premium": quote.ask if quantity > 0 else quote.bid,
    }


def _intrinsic_target_gain(opportunity: ConvexityOpportunity, quote: OptionContractQuote) -> Decimal:
    if quote.option_type == "call":
        target = opportunity.spot_price * (Decimal("1") + opportunity.expected_move_pct)
        intrinsic = max(Decimal("0"), target - quote.strike)
    else:
        target = opportunity.spot_price * (Decimal("1") - opportunity.expected_move_pct)
        intrinsic = max(Decimal("0"), quote.strike - target)
    return max(Decimal("0"), intrinsic * Decimal("100") - (quote.ask * Decimal("100")))
