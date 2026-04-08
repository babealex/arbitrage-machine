from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from uuid import uuid4

from app.allocation.outcomes import AllocationAdjustment

from .models import AllocationDecision, AllocationRun, CapitalSleeve, PortfolioRiskState, TradeCandidate


@dataclass(frozen=True, slots=True)
class AllocationConfig:
    minimum_expected_pnl: Decimal = Decimal("1")
    maximum_uncertainty_penalty: Decimal = Decimal("1.00")
    maximum_tail_loss_fraction: Decimal = Decimal("0.03")
    kelly_fraction_scale: Decimal = Decimal("0.35")
    minimum_score: Decimal = Decimal("0.01")


class AllocationEngine:
    def __init__(
        self,
        *,
        sleeves: list[CapitalSleeve] | None = None,
        config: AllocationConfig | None = None,
    ) -> None:
        self.sleeves = {sleeve.name: sleeve for sleeve in (sleeves or default_sleeves())}
        self.config = config or AllocationConfig()

    def run(
        self,
        *,
        candidates: list[TradeCandidate],
        risk_state: PortfolioRiskState,
        adjustments: list[AllocationAdjustment] | None = None,
    ) -> tuple[AllocationRun, list[AllocationDecision]]:
        run_id = f"alloc:{uuid4()}"
        decisions: list[AllocationDecision] = []
        remaining_by_sleeve = {
            sleeve.name: max(
                Decimal("0"),
                (risk_state.portfolio_equity * sleeve.capital_cap_fraction)
                - risk_state.allocated_notional_by_sleeve.get(sleeve.name, Decimal("0")),
            )
            for sleeve in self.sleeves.values()
        }
        remaining_by_bucket = {
            key: max(
                Decimal("0"),
                (risk_state.portfolio_equity * risk_state.bucket_cap_fraction) - value,
            )
            for key, value in risk_state.allocated_notional_by_bucket.items()
        }
        gross_remaining = max(Decimal("0"), risk_state.gross_notional_cap - risk_state.current_gross_notional)
        ordered = sorted(candidates, key=lambda candidate: self._score(candidate, adjustments=adjustments or []), reverse=True)
        for candidate in ordered:
            decision = self._decide_candidate(
                run_id=run_id,
                candidate=candidate,
                risk_state=risk_state,
                remaining_by_sleeve=remaining_by_sleeve,
                remaining_by_bucket=remaining_by_bucket,
                gross_remaining=gross_remaining,
                adjustments=adjustments or [],
            )
            decisions.append(decision)
            if decision.approved:
                remaining_by_sleeve[candidate.sleeve] = max(Decimal("0"), remaining_by_sleeve[candidate.sleeve] - decision.target_notional)
                remaining_by_bucket[candidate.correlation_bucket] = max(
                    Decimal("0"),
                    remaining_by_bucket.get(candidate.correlation_bucket, risk_state.portfolio_equity * risk_state.bucket_cap_fraction)
                    - decision.target_notional,
                )
                gross_remaining = max(Decimal("0"), gross_remaining - decision.target_notional)
        run = AllocationRun(
            run_id=run_id,
            observed_at=risk_state.observed_at,
            portfolio_equity=risk_state.portfolio_equity,
            daily_realized_pnl=risk_state.daily_realized_pnl,
            daily_loss_stop_active=risk_state.daily_loss_stop_active,
            gross_notional_cap=risk_state.gross_notional_cap,
            candidate_count=len(candidates),
            approved_count=sum(1 for decision in decisions if decision.approved),
            rejected_count=sum(1 for decision in decisions if not decision.approved),
            allocated_capital=sum((decision.target_notional for decision in decisions if decision.approved), Decimal("0")),
            expected_portfolio_ev=sum((decision.expected_pnl_after_costs for decision in decisions if decision.approved), Decimal("0")),
            realized_pnl=None,
        )
        return run, decisions

    def _decide_candidate(
        self,
        *,
        run_id: str,
        candidate: TradeCandidate,
        risk_state: PortfolioRiskState,
        remaining_by_sleeve: dict[str, Decimal],
        remaining_by_bucket: dict[str, Decimal],
        gross_remaining: Decimal,
        adjustments: list[AllocationAdjustment],
    ) -> AllocationDecision:
        score = self._score(candidate, adjustments=adjustments)
        rejection_reason = self._rejection_reason(
            candidate=candidate,
            score=score,
            risk_state=risk_state,
            remaining_by_sleeve=remaining_by_sleeve,
            remaining_by_bucket=remaining_by_bucket,
            gross_remaining=gross_remaining,
        )
        if rejection_reason is not None:
            return AllocationDecision(
                run_id=run_id,
                candidate_id=candidate.candidate_id,
                strategy_family=candidate.strategy_family,
                source_kind=candidate.source_kind,
                sleeve=candidate.sleeve,
                regime_name=candidate.regime_name,
                observed_at=candidate.observed_at,
                score=score,
                approved=False,
                rejection_reason=rejection_reason,
                target_notional=Decimal("0"),
                max_loss_budget=Decimal("0"),
                expected_pnl_after_costs=candidate.expected_pnl_after_costs,
                cvar_95_loss=candidate.cvar_95_loss,
                uncertainty_penalty=candidate.uncertainty_penalty,
                liquidity_penalty=candidate.liquidity_penalty,
                horizon_days=candidate.horizon_days,
                correlation_bucket=candidate.correlation_bucket,
            )
        sleeve = self.sleeves[candidate.sleeve]
        max_loss_budget = min(
            risk_state.portfolio_equity * sleeve.max_loss_budget_fraction,
            risk_state.portfolio_equity * self.config.maximum_tail_loss_fraction,
        )
        raw_fraction = min(sleeve.max_trade_fraction, score * self.config.kelly_fraction_scale)
        target_notional = candidate.notional_reference * raw_fraction
        loss_fraction = Decimal("1") if candidate.notional_reference <= 0 else candidate.max_loss_estimate / candidate.notional_reference
        if loss_fraction > 0:
            target_notional = min(target_notional, max_loss_budget / loss_fraction)
        target_notional = min(
            target_notional,
            remaining_by_sleeve.get(candidate.sleeve, Decimal("0")),
            remaining_by_bucket.get(candidate.correlation_bucket, risk_state.portfolio_equity * risk_state.bucket_cap_fraction),
            gross_remaining,
        )
        if target_notional <= 0:
            return AllocationDecision(
                run_id=run_id,
                candidate_id=candidate.candidate_id,
                strategy_family=candidate.strategy_family,
                source_kind=candidate.source_kind,
                sleeve=candidate.sleeve,
                regime_name=candidate.regime_name,
                observed_at=candidate.observed_at,
                score=score,
                approved=False,
                rejection_reason="no_capacity_remaining",
                target_notional=Decimal("0"),
                max_loss_budget=max_loss_budget,
                expected_pnl_after_costs=candidate.expected_pnl_after_costs,
                cvar_95_loss=candidate.cvar_95_loss,
                uncertainty_penalty=candidate.uncertainty_penalty,
                liquidity_penalty=candidate.liquidity_penalty,
                horizon_days=candidate.horizon_days,
                correlation_bucket=candidate.correlation_bucket,
            )
        return AllocationDecision(
            run_id=run_id,
            candidate_id=candidate.candidate_id,
            strategy_family=candidate.strategy_family,
            source_kind=candidate.source_kind,
            sleeve=candidate.sleeve,
            regime_name=candidate.regime_name,
            observed_at=candidate.observed_at,
            score=score,
            approved=True,
            rejection_reason=None,
            target_notional=target_notional,
            max_loss_budget=max_loss_budget,
            expected_pnl_after_costs=candidate.expected_pnl_after_costs,
            cvar_95_loss=candidate.cvar_95_loss,
            uncertainty_penalty=candidate.uncertainty_penalty,
            liquidity_penalty=candidate.liquidity_penalty,
            horizon_days=candidate.horizon_days,
            correlation_bucket=candidate.correlation_bucket,
        )

    def _rejection_reason(
        self,
        *,
        candidate: TradeCandidate,
        score: Decimal,
        risk_state: PortfolioRiskState,
        remaining_by_sleeve: dict[str, Decimal],
        remaining_by_bucket: dict[str, Decimal],
        gross_remaining: Decimal,
    ) -> str | None:
        if risk_state.daily_loss_stop_active:
            return "daily_loss_stop_active"
        if not candidate.state_ok:
            return "stale_or_broken_state"
        if candidate.expected_pnl_after_costs < self.config.minimum_expected_pnl:
            return "expected_edge_too_small"
        if candidate.uncertainty_penalty > self.config.maximum_uncertainty_penalty:
            return "uncertainty_too_high"
        if candidate.max_loss_estimate > (risk_state.portfolio_equity * self.config.maximum_tail_loss_fraction):
            return "tail_loss_too_high"
        if remaining_by_sleeve.get(candidate.sleeve, Decimal("0")) <= 0:
            return "sleeve_exhausted"
        bucket_remaining = remaining_by_bucket.get(candidate.correlation_bucket, risk_state.portfolio_equity * risk_state.bucket_cap_fraction)
        if bucket_remaining <= 0:
            return "correlated_exposure_limit"
        if gross_remaining <= 0:
            return "portfolio_cap_exhausted"
        if score < self.config.minimum_score:
            return "score_too_low"
        return None

    def _score(self, candidate: TradeCandidate, *, adjustments: list[AllocationAdjustment]) -> Decimal:
        pnl_bias, tail_mult, uncertainty_adj = _adjustment_tuple(candidate, adjustments)
        adjusted_expected = candidate.expected_pnl_after_costs * pnl_bias
        adjusted_cvar = abs(candidate.cvar_95_loss) * tail_mult
        adjusted_uncertainty = candidate.uncertainty_penalty * uncertainty_adj
        denom = (
            adjusted_cvar
            * (Decimal("1") + adjusted_uncertainty)
            * (Decimal("1") + candidate.liquidity_penalty)
            * (Decimal("1") + (Decimal(candidate.horizon_days) / Decimal("30")))
            * (Decimal("1") + (Decimal("1") - candidate.regime_stability))
        )
        if denom <= 0:
            return Decimal("0")
        return adjusted_expected / denom


def default_sleeves() -> list[CapitalSleeve]:
    return [
        CapitalSleeve(name="base", capital_cap_fraction=Decimal("0.60"), max_trade_fraction=Decimal("0.25"), max_loss_budget_fraction=Decimal("0.015")),
        CapitalSleeve(name="opportunistic", capital_cap_fraction=Decimal("0.25"), max_trade_fraction=Decimal("0.15"), max_loss_budget_fraction=Decimal("0.010")),
        CapitalSleeve(name="attack", capital_cap_fraction=Decimal("0.15"), max_trade_fraction=Decimal("0.08"), max_loss_budget_fraction=Decimal("0.0075")),
    ]


def _adjustment_tuple(candidate: TradeCandidate, adjustments: list[AllocationAdjustment]) -> tuple[Decimal, Decimal, Decimal]:
    pnl_bias = Decimal("1")
    tail_mult = Decimal("1")
    uncertainty_adj = Decimal("1")
    for adjustment in adjustments:
        if (
            (adjustment.scope_type == "sleeve" and adjustment.scope_key == candidate.sleeve)
            or (adjustment.scope_type == "bucket" and adjustment.scope_key == candidate.correlation_bucket)
            or (adjustment.scope_type == "regime" and adjustment.scope_key == candidate.regime_name)
        ):
            pnl_bias *= adjustment.pnl_bias_multiplier
            tail_mult *= adjustment.tail_risk_multiplier
            uncertainty_adj *= adjustment.uncertainty_multiplier_adjustment
    return pnl_bias, tail_mult, uncertainty_adj
