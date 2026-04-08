from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from decimal import Decimal
from uuid import uuid4

from app.allocation.coordinator import PendingAllocationAction
from app.allocation.models import TradeCandidate
from app.core.money import to_decimal
from app.execution.models import OrderIntent
from app.options.models import OptionChainSnapshot, OptionQuote
from app.persistence.convexity_repo import ConvexityRepository
from app.strategies.convexity.execution_adapter import select_convexity_orders
from app.strategies.convexity.models import ConvexTradeCandidate, ConvexTradeOutcome, ConvexityAccountState, ConvexityOpportunity
from app.strategies.convexity.risk import passes_convexity_risk_limits, risk_budget_for_trade
from app.strategies.convexity.scanner import scan_convexity_opportunities
from app.strategies.convexity.structures import evaluate_convex_trade_candidates


@dataclass(frozen=True, slots=True)
class ConvexityRunSummary:
    run_id: str
    timestamp: str
    scanned_opportunities: int
    candidate_trades: int
    accepted_trades: int
    rejected_for_liquidity: int
    rejected_for_negative_ev: int
    rejected_for_risk_budget: int
    rejected_other: int
    open_convexity_risk: Decimal
    avg_reward_to_risk: Decimal
    avg_expected_value_after_costs: Decimal
    metadata_json: dict = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class ConvexityCycleResult:
    run_id: str
    opportunities: list[ConvexityOpportunity]
    candidates: list[ConvexTradeCandidate]
    accepted_candidates: list[ConvexTradeCandidate]
    selected_candidates: list[ConvexTradeCandidate]
    order_intents: list[OrderIntent]
    summary: ConvexityRunSummary
    rejection_reasons: dict[str, str]


def run_convexity_cycle(
    db,
    config,
    market_state,
    calendars,
    news_state,
    option_chains,
    account_state: ConvexityAccountState,
    run_metadata: dict | None = None,
) -> ConvexityCycleResult:
    repo = ConvexityRepository(db)
    persistence_enabled = getattr(config, "convexity_persistence_enabled", True)
    run_id = f"convexity:{uuid4().hex[:16]}"
    now = datetime.now(timezone.utc)
    opportunities = scan_convexity_opportunities(market_state, calendars, news_state)
    opportunity_ids = [] if not persistence_enabled else repo.persist_opportunities(run_id, opportunities, timestamp=now)
    symbol_to_opportunity_id = {item.symbol: opportunity_ids[index] for index, item in enumerate(opportunities)}

    candidates: list[ConvexTradeCandidate] = []
    statuses: dict[str, str] = {}
    rejection_reasons: dict[str, str] = {}
    accepted_candidates: list[ConvexTradeCandidate] = []
    for opportunity in opportunities:
        chain = option_chains.get(opportunity.symbol, [])
        built, builder_rejections, evaluated = evaluate_convex_trade_candidates(
            opportunity,
            chain,
            account_state,
            allowed_dte_min=config.convexity_allowed_dte_min,
            allowed_dte_max=config.convexity_allowed_dte_max,
            min_oi=config.convexity_min_oi,
            min_volume=config.convexity_min_volume,
            max_bid_ask_pct_of_debit=Decimal(str(config.convexity_max_bid_ask_pct_of_debit)),
            min_reward_to_risk=Decimal(str(config.convexity_min_reward_to_risk)),
            min_expected_value_after_costs=Decimal(str(config.convexity_min_expected_value_after_costs)),
        )
        for candidate in evaluated:
            candidates.append(candidate)
            reason = builder_rejections.get(candidate.candidate_id)
            if reason is None:
                reason = classify_rejection_reason(candidate, account_state, config)
            if reason is None:
                statuses[candidate.candidate_id] = "accepted"
                accepted_candidates.append(candidate)
            else:
                statuses[candidate.candidate_id] = "rejected"
                rejection_reasons[candidate.candidate_id] = reason
    if persistence_enabled:
        repo.persist_candidates(
            run_id,
            candidates,
            statuses,
            rejection_reasons,
            opportunity_id_map=symbol_to_opportunity_id,
            timestamp=now,
        )
    orders = select_convexity_orders(
        accepted_candidates,
        account_state,
        max_risk_per_trade_pct=Decimal(str(config.convexity_max_risk_per_trade_pct)),
        max_total_exposure_pct=Decimal(str(config.convexity_max_total_exposure_pct)),
        max_open_trades=config.convexity_max_open_trades,
        max_symbol_exposure_pct=Decimal("0.05"),
        max_correlated_event_exposure_pct=Decimal("0.10"),
    )
    accepted_order_candidate_ids = [intent.signal_ref for intent in orders]
    accepted_lookup = {candidate.candidate_id: candidate for candidate in accepted_candidates}
    order_status = "created" if config.convexity_paper_execution_enabled else "unsubmitted"
    if persistence_enabled:
        repo.persist_order_intents(run_id, orders, accepted_order_candidate_ids, candidate_lookup=accepted_lookup, timestamp=now, status=order_status)
    selected_candidates = [accepted_lookup[candidate_id] for candidate_id in accepted_order_candidate_ids if candidate_id in accepted_lookup]
    summary = build_run_summary(
        run_id=run_id,
        now=now,
        opportunities=opportunities,
        candidates=candidates,
        accepted_candidates=selected_candidates,
        rejection_reasons=rejection_reasons,
        account_state=account_state,
        paper_execution_enabled=config.convexity_paper_execution_enabled,
        run_metadata=run_metadata or {},
    )
    if persistence_enabled:
        repo.persist_run_summary(asdict(summary))
    return ConvexityCycleResult(
        run_id=run_id,
        opportunities=opportunities,
        candidates=candidates,
        accepted_candidates=accepted_candidates,
        selected_candidates=selected_candidates,
        order_intents=orders,
        summary=summary,
        rejection_reasons=rejection_reasons,
    )


def classify_rejection_reason(candidate: ConvexTradeCandidate, account_state: ConvexityAccountState, config) -> str | None:
    if candidate.expected_value_after_costs <= Decimal(str(config.convexity_min_expected_value_after_costs)):
        return "negative_ev_after_costs"
    if candidate.reward_to_risk < Decimal(str(config.convexity_min_reward_to_risk)):
        return "reward_to_risk_too_low"
    if candidate.liquidity_score <= Decimal("0"):
        return "liquidity_too_low"
    bid_ask_pct = Decimal(str(candidate.metadata.get("bid_ask_pct_of_debit", "0")))
    if bid_ask_pct > Decimal(str(config.convexity_max_bid_ask_pct_of_debit)):
        return "spread_too_wide"
    budget = risk_budget_for_trade(account_state.nav, Decimal(str(config.convexity_max_risk_per_trade_pct)))
    if candidate.max_loss > budget:
        return "risk_budget_exceeded"
    okay, reason = passes_convexity_risk_limits(
        candidate=candidate,
        account_state=account_state,
        max_total_exposure_pct=Decimal(str(config.convexity_max_total_exposure_pct)),
        max_open_trades=config.convexity_max_open_trades,
        max_symbol_exposure_pct=Decimal("0.05"),
        max_correlated_event_exposure_pct=Decimal("0.10"),
    )
    if not okay:
        return "exposure_cap_exceeded" if reason != "max_open_trades" else "max_open_trades_exceeded"
    return None


def build_run_summary(
    *,
    run_id: str,
    now: datetime,
    opportunities: list[ConvexityOpportunity],
    candidates: list[ConvexTradeCandidate],
    accepted_candidates: list[ConvexTradeCandidate],
    rejection_reasons: dict[str, str],
    account_state: ConvexityAccountState,
    paper_execution_enabled: bool,
    run_metadata: dict,
) -> ConvexityRunSummary:
    def _count(reason: str) -> int:
        return sum(1 for item in rejection_reasons.values() if item == reason)

    return ConvexityRunSummary(
        run_id=run_id,
        timestamp=now.isoformat(),
        scanned_opportunities=len(opportunities),
        candidate_trades=len(candidates),
        accepted_trades=len(accepted_candidates),
        rejected_for_liquidity=_count("liquidity_too_low") + _count("spread_too_wide"),
        rejected_for_negative_ev=_count("negative_ev_after_costs") + _count("reward_to_risk_too_low"),
        rejected_for_risk_budget=_count("risk_budget_exceeded") + _count("exposure_cap_exceeded") + _count("max_open_trades_exceeded"),
        rejected_other=max(0, len(candidates) - len(accepted_candidates) - (_count("liquidity_too_low") + _count("spread_too_wide") + _count("negative_ev_after_costs") + _count("reward_to_risk_too_low") + _count("risk_budget_exceeded") + _count("exposure_cap_exceeded") + _count("max_open_trades_exceeded"))),
        open_convexity_risk=account_state.open_convexity_risk,
        avg_reward_to_risk=_avg([item.reward_to_risk for item in accepted_candidates]),
        avg_expected_value_after_costs=_avg([item.expected_value_after_costs for item in accepted_candidates]),
        metadata_json={
            "paper_execution_only": True,
            "paper_execution_enabled": paper_execution_enabled,
            **run_metadata,
        },
    )


def export_allocator_candidates(cycle_result: ConvexityCycleResult) -> list[PendingAllocationAction]:
    pending: list[PendingAllocationAction] = []
    for candidate in cycle_result.selected_candidates:
        pending.append(
            PendingAllocationAction(
                candidate=TradeCandidate(
                    candidate_id=candidate.candidate_id,
                    strategy_family="convexity",
                    source_kind="convexity_candidate",
                    instrument_key=candidate.symbol,
                    sleeve="attack",
                    regime_name=candidate.setup_type,
                    observed_at=datetime.fromisoformat(cycle_result.summary.timestamp),
                    expected_pnl_after_costs=candidate.expected_value_after_costs,
                    cvar_95_loss=abs(candidate.max_loss),
                    uncertainty_penalty=max(Decimal("0.1"), Decimal("1") - candidate.confidence_score),
                    liquidity_penalty=max(Decimal("0"), Decimal("1") - candidate.liquidity_score),
                    horizon_days=max(1, (candidate.expiry - datetime.fromisoformat(cycle_result.summary.timestamp).date()).days),
                    regime_stability=candidate.confidence_score,
                    notional_reference=candidate.debit,
                    max_loss_estimate=candidate.max_loss,
                    correlation_bucket=str(candidate.metadata.get("correlated_event_bucket", candidate.symbol)),
                    state_ok=True,
                ),
                execute=None,
            )
        )
    return pending


def update_paper_convexity_outcomes(db, pricing_snapshot_loader, now: datetime) -> int:
    repo = ConvexityRepository(db)
    orders = [dict(row) for row in db.fetchall("SELECT * FROM convexity_order_intents WHERE status IN ('created', 'paper_filled', 'unsubmitted') ORDER BY id ASC")]
    created = 0
    outcomes: list[ConvexTradeOutcome] = []
    for order in orders:
        payload = _json_dict(order["order_intent_json"])
        quotes = pricing_snapshot_loader(str(order["symbol"]))
        if not quotes:
            continue
        limit_debit = to_decimal(order["limit_debit_decimal"])
        current_mid = _estimate_position_value(quotes)
        realized_pnl = current_mid - limit_debit
        open_ts = datetime.fromisoformat(str(order["timestamp"]))
        if now >= open_ts.replace(tzinfo=now.tzinfo) and (abs(realized_pnl) >= limit_debit * Decimal("0.5") or now.date() > open_ts.date()):
            outcomes.append(
                ConvexTradeOutcome(
                    candidate_id=str(order["candidate_id"]),
                    symbol=str(order["symbol"]),
                    structure_type=str(order["structure_type"]),
                    entry_cost=limit_debit,
                    max_loss=to_decimal(order["risk_amount_decimal"]),
                    realized_pnl=realized_pnl,
                    realized_multiple=Decimal("0") if limit_debit <= 0 else realized_pnl / limit_debit,
                    held_to_exit=now.date() > open_ts.date(),
                    exit_reason="paper_mark_exit",
                    metadata={"order_intent_id": int(order["id"]), "timestamp_opened": order["timestamp"], "timestamp_closed": now.isoformat(), "setup_type": payload.get("signal_ref", "")},
                )
            )
            db.execute("UPDATE convexity_order_intents SET status = ? WHERE id = ?", ("expired", int(order["id"])))
            created += 1
    repo.persist_trade_outcomes(outcomes)
    return created


def _avg(values: list[Decimal]) -> Decimal:
    return Decimal("0") if not values else sum(values) / Decimal(len(values))


def _json_dict(raw) -> dict:
    if isinstance(raw, dict):
        return raw
    import json

    try:
        return json.loads(raw or "{}")
    except Exception:
        return {}


def _estimate_position_value(quotes: list) -> Decimal:
    mids: list[Decimal] = []
    for quote in quotes:
        bid = Decimal(str(getattr(quote, "bid", 0)))
        ask = Decimal(str(getattr(quote, "ask", 0)))
        if ask > 0 and ask >= bid:
            mids.append(((bid + ask) / Decimal("2")) * Decimal("100"))
    return sum(mids) if mids else Decimal("0")
