from __future__ import annotations

from decimal import Decimal

from app.options.pricing import intrinsic_value, years_to_expiry
from app.strategies.convexity.expected_move import opportunity_move_edge
from app.strategies.convexity.models import ConvexTradeCandidate, ConvexityOpportunity, OptionContractQuote


def estimate_all_in_cost(
    *,
    quotes: list[OptionContractQuote],
    commission_cost: Decimal,
    slippage_multiplier: Decimal,
) -> Decimal:
    spread_cost = sum(((quote.ask - quote.bid) / Decimal("2")) * Decimal("100") for quote in quotes)
    slippage_cost = spread_cost * slippage_multiplier
    return spread_cost + commission_cost + slippage_cost


def liquidity_score(quotes: list[OptionContractQuote], *, min_oi: int, min_volume: int) -> Decimal:
    if not quotes:
        return Decimal("0")
    scores: list[Decimal] = []
    for quote in quotes:
        if quote.ask <= 0 or quote.bid < 0 or quote.ask < quote.bid:
            return Decimal("0")
        spread_pct = (quote.ask - quote.bid) / quote.ask if quote.ask > 0 else Decimal("1")
        oi_factor = Decimal("1") if (quote.open_interest or 0) >= min_oi else Decimal("0.25")
        volume_factor = Decimal("1") if (quote.volume or 0) >= min_volume else Decimal("0.25")
        scores.append(max(Decimal("0"), Decimal("1") - spread_pct) * oi_factor * volume_factor)
    return sum(scores) / Decimal(len(scores))


def scenario_expected_value(
    *,
    candidate: ConvexTradeCandidate,
    opportunity: ConvexityOpportunity,
    quotes: list[OptionContractQuote],
    time_value_remainder: Decimal = Decimal("0.25"),
) -> tuple[Decimal, Decimal]:
    scenarios = [
        ("no_move", Decimal("0.45"), Decimal("0")),
        ("moderate_move", Decimal("0.30"), opportunity.expected_move_pct * Decimal("0.6")),
        ("large_move", Decimal("0.15"), opportunity.expected_move_pct * Decimal("1.2")),
        ("breakout", Decimal("0.10"), opportunity.expected_move_pct * Decimal("2.5")),
    ]
    expected_value = Decimal("0")
    for _, probability, move_pct in scenarios:
        payoff = estimate_candidate_payoff(
            candidate=candidate,
            opportunity=opportunity,
            move_pct=move_pct,
            time_value_remainder=time_value_remainder,
        )
        expected_value += probability * payoff
    return expected_value, expected_value - candidate.metadata.get("all_in_cost", Decimal("0"))


def estimate_candidate_payoff(
    *,
    candidate: ConvexTradeCandidate,
    opportunity: ConvexityOpportunity,
    move_pct: Decimal,
    time_value_remainder: Decimal,
) -> Decimal:
    target_spot = opportunity.spot_price * (Decimal("1") + (move_pct if opportunity.metadata.get("direction", "bullish") != "bearish" else -move_pct))
    value = Decimal("0")
    for leg in candidate.legs:
        strike = Decimal(str(leg["strike"]))
        qty = Decimal(str(leg["quantity"]))
        option_type = str(leg["option_type"])
        premium = Decimal(str(leg["premium"]))
        intrinsic = intrinsic_value(spot=target_spot, strike=strike, option_type=option_type)
        value += qty * ((intrinsic + (premium * time_value_remainder)) * Decimal("100"))
    return value - candidate.debit


def payoff_nonlinearity(candidate: ConvexTradeCandidate) -> Decimal:
    if candidate.max_profit_estimate is None or candidate.max_loss <= 0:
        return Decimal("0")
    return min(Decimal("10"), candidate.max_profit_estimate / candidate.max_loss)


def convex_score(
    *,
    candidate: ConvexTradeCandidate,
    opportunity: ConvexityOpportunity,
    weights: dict[str, Decimal] | None = None,
) -> Decimal:
    w = {
        "reward": Decimal("1.0"),
        "ev": Decimal("1.0"),
        "nonlinearity": Decimal("0.75"),
        "breakout": Decimal("0.75"),
        "move_edge": Decimal("0.75"),
        "spread_penalty": Decimal("0.75"),
        "illiquidity_penalty": Decimal("1.0"),
    }
    w.update(weights or {})
    spread_penalty = Decimal(str(candidate.metadata.get("bid_ask_pct_of_debit", "0")))
    illiquidity_penalty = Decimal("1") - candidate.liquidity_score
    return (
        w["reward"] * candidate.reward_to_risk
        + w["ev"] * candidate.expected_value_after_costs
        + w["nonlinearity"] * payoff_nonlinearity(candidate)
        + w["breakout"] * candidate.breakout_sensitivity
        + w["move_edge"] * opportunity_move_edge(opportunity)
        - w["spread_penalty"] * spread_penalty
        - w["illiquidity_penalty"] * illiquidity_penalty
    )
