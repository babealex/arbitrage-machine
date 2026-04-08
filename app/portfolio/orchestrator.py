from __future__ import annotations

import logging
from collections import Counter
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from decimal import Decimal
from typing import Callable, Iterable

from app.allocation.engine import AllocationEngine
from app.allocation.adapters import candidate_from_options_selection, candidate_from_trend_target
from app.allocation.coordinator import PendingAllocationAction
from app.allocation.models import AllocationDecision, AllocationRun, PortfolioRiskState, TradeCandidate
from app.allocation.outcomes import AllocationAdjustment
from app.core.money import to_decimal
from app.execution.models import ExecutionOutcome, SignalLegIntent
from app.execution.orders import build_order_intent
from app.execution.router import ExecutionRouter
from app.instruments.models import InstrumentRef
from app.market_data.snapshots import MarketStateInput
from app.models import Signal, SignalEdge, StrategyName
from app.options.scenario_engine import StructureScenarioSummary
from app.options.selection import StructureSelectionRecord
from app.persistence.allocation_repo import AllocationRepository
from app.persistence.contracts import PortfolioCycleSnapshotRecord
from app.persistence.kalshi_repo import KalshiRepository
from app.persistence.options_repo import OptionsRepository
from app.persistence.portfolio_cycle_repo import PortfolioCycleRepository
from app.persistence.trend_repo import TrendRepository
from app.strategies.trend.models import TrendTarget


@dataclass(frozen=True, slots=True)
class PortfolioOrchestratorConfig:
    cycle_interval_seconds: int = 60
    max_capital_per_strategy: dict[str, Decimal] = field(
        default_factory=lambda: {
            "kalshi": Decimal("0.30"),
            "trend": Decimal("0.60"),
            "options": Decimal("0.10"),
        }
    )
    minimum_liquidity_score: Decimal = Decimal("0.25")
    maximum_spread_bps: Decimal = Decimal("150")


@dataclass(frozen=True, slots=True)
class CandidateTrade:
    strategy_type: str
    instrument: str
    expected_return: Decimal
    expected_risk: Decimal
    liquidity_score: Decimal
    time_horizon: str
    metadata: dict[str, object]
    trade_candidate: TradeCandidate
    execute: Callable[[AllocationDecision], bool] | None = None
    is_live: bool = True


@dataclass(frozen=True, slots=True)
class PortfolioCycleResult:
    run: AllocationRun
    decisions: list[AllocationDecision]
    candidates: list[CandidateTrade]
    execution_results: dict[str, bool]


class PortfolioOrchestrator:
    def __init__(
        self,
        config,
        db,
        allocator,
        execution_router,
        *,
        kalshi_engine=None,
        trend_engine=None,
        options_engine=None,
        convexity_engine=None,
        logger: logging.Logger | None = None,
    ) -> None:
        self.config = config if isinstance(config, PortfolioOrchestratorConfig) else PortfolioOrchestratorConfig()
        self.db = db
        self.allocator = allocator if isinstance(allocator, AllocationEngine) else allocator
        self.execution_router = execution_router
        self.kalshi_engine = kalshi_engine
        self.trend_engine = trend_engine
        self.options_engine = options_engine
        self.convexity_engine = convexity_engine
        self.logger = logger or logging.getLogger("portfolio_orchestrator")
        self.allocation_repo = AllocationRepository(db)
        self.kalshi_repo = KalshiRepository(db)
        self.trend_repo = TrendRepository(db)
        self.options_repo = OptionsRepository(db)
        self.portfolio_cycle_repo = PortfolioCycleRepository(db)

    def run_cycle(self) -> PortfolioCycleResult:
        candidates = self._collect_candidates()
        risk_state = self._build_risk_state()
        adjustments = self.allocation_repo.load_latest_adjustments()
        run, decisions = self.allocator.run(
            candidates=[item.trade_candidate for item in candidates],
            risk_state=risk_state,
            adjustments=adjustments,
        )
        self.allocation_repo.persist_run(run)
        self.allocation_repo.persist_decisions(decisions)
        self.logger.info(
            "portfolio_cycle_candidates",
            extra={
                "event": {
                    "candidate_count": len(candidates),
                    "strategies": [item.strategy_type for item in candidates],
                }
            },
        )
        execution_results: dict[str, bool] = {}
        candidate_map = {item.trade_candidate.candidate_id: item for item in candidates}
        for decision in decisions:
            candidate = candidate_map[decision.candidate_id]
            self.logger.info(
                "allocation_decision",
                extra={
                    "event": {
                        "candidate_id": decision.candidate_id,
                        "strategy_type": candidate.strategy_type,
                        "approved": decision.approved,
                        "reason": decision.rejection_reason,
                        "target_notional": format(decision.target_notional, "f"),
                    }
                },
            )
            if not decision.approved:
                execution_results[decision.candidate_id] = False
                continue
            if candidate.execute is None:
                self.allocation_repo.update_decision_execution(
                    run_id=decision.run_id,
                    candidate_id=decision.candidate_id,
                    execution_price=None,
                    slippage_estimate=None,
                    fill_status="paper_only" if not candidate.is_live else "not_executed",
                )
                execution_results[decision.candidate_id] = False
                continue
            if not self._execution_allowed(candidate):
                self.allocation_repo.update_decision_execution(
                    run_id=decision.run_id,
                    candidate_id=decision.candidate_id,
                    execution_price=None,
                    slippage_estimate=None,
                    fill_status="rejected_by_orchestrator",
                )
                execution_results[decision.candidate_id] = False
                continue
            result = False if candidate.execute is None else candidate.execute(decision)
            execution_results[decision.candidate_id] = result
            self._sync_execution_status(decision)
        persisted_decisions = self.allocation_repo.load_decisions(run.run_id)
        self.portfolio_cycle_repo.persist_snapshot(
            PortfolioCycleSnapshotRecord(
                run_id=run.run_id,
                observed_at=run.observed_at.isoformat(),
                candidate_count=run.candidate_count,
                approved_count=run.approved_count,
                rejected_count=run.rejected_count,
                allocated_capital_decimal=format(run.allocated_capital, "f"),
                expected_portfolio_ev_decimal=format(run.expected_portfolio_ev, "f"),
                strategy_counts_json=dict(Counter(item.strategy_type for item in candidates)),
                rejection_counts_json=dict(Counter(decision.rejection_reason or "approved" for decision in persisted_decisions)),
                readiness_json={},
            )
        )
        return PortfolioCycleResult(run=run, decisions=persisted_decisions, candidates=candidates, execution_results=execution_results)

    def _collect_candidates(self) -> list[CandidateTrade]:
        now = datetime.now(timezone.utc)
        items: list[CandidateTrade] = []
        if self.kalshi_engine is not None:
            for raw in self.kalshi_engine.get_candidates():
                items.append(self._normalize_kalshi_candidate(raw))
        if self.trend_engine is not None:
            for raw in self.trend_engine.get_targets():
                items.append(self._normalize_trend_candidate(raw, now))
        if self.options_engine is not None:
            for raw in self.options_engine.get_candidates():
                items.append(self._normalize_options_candidate(raw))
        if self.convexity_engine is not None:
            for raw in self.convexity_engine.get_candidates():
                items.append(self._normalize_convexity_candidate(raw))
        return items

    def _normalize_kalshi_candidate(self, raw) -> CandidateTrade:
        if isinstance(raw, CandidateTrade):
            return raw
        if isinstance(raw, Signal):
            trade_candidate = TradeCandidate(
                candidate_id=f"{raw.strategy.value}:{raw.ticker}:{raw.ts.isoformat()}",
                strategy_family=raw.strategy.value,
                source_kind="kalshi_signal",
                instrument_key=raw.ticker,
                sleeve="opportunistic",
                regime_name=raw.source,
                observed_at=raw.ts,
                expected_pnl_after_costs=Decimal(raw.quantity) * Decimal("100") * raw.edge.edge_after_fees,
                cvar_95_loss=Decimal(max(40, raw.quantity * 10)),
                uncertainty_penalty=Decimal("0.25"),
                liquidity_penalty=Decimal("0.05"),
                horizon_days=14,
                regime_stability=Decimal("0.75"),
                notional_reference=Decimal(raw.quantity) * Decimal("100"),
                max_loss_estimate=Decimal(max(40, raw.quantity * 10)),
                correlation_bucket=raw.ticker,
                state_ok=True,
            )
            return CandidateTrade(
                strategy_type="kalshi",
                instrument=raw.ticker,
                expected_return=raw.edge.edge_after_fees,
                expected_risk=trade_candidate.cvar_95_loss / max(Decimal("1"), trade_candidate.notional_reference),
                liquidity_score=Decimal("1") - trade_candidate.liquidity_penalty,
                time_horizon=f"{trade_candidate.horizon_days}d",
                metadata={"source": raw.source, "spread_bps": "0"},
                trade_candidate=trade_candidate,
                execute=lambda decision, signal=raw: self.execution_router.execute_signal(
                    signal,
                    extra_source_metadata={
                        "allocation_run_id": decision.run_id,
                        "allocation_candidate_id": decision.candidate_id,
                    },
                ),
                is_live=True,
            )
        return raw

    def _normalize_trend_candidate(self, raw, observed_at: datetime) -> CandidateTrade:
        if isinstance(raw, CandidateTrade):
            return raw
        if isinstance(raw, TrendTarget):
            portfolio_equity = self._current_portfolio_equity()
            trade_candidate = candidate_from_trend_target(raw, observed_at=observed_at, portfolio_equity=portfolio_equity)
            latest_price = self._latest_trend_price(raw.symbol)
            signal = Signal(
                strategy=StrategyName.MOMENTUM,
                ticker=raw.symbol,
                action="rebalance_buy" if raw.target_weight >= 0 else "rebalance_sell",
                quantity=max(1, int(abs(trade_candidate.notional_reference) / max(Decimal("1"), latest_price))),
                edge=SignalEdge.from_values(
                    edge_before_fees=raw.momentum_return,
                    edge_after_fees=raw.momentum_return - Decimal("0.0012"),
                    fee_estimate=Decimal("0.0012"),
                ),
                source="portfolio_orchestrator",
                confidence=float(min(Decimal("1"), abs(raw.momentum_return) / max(raw.realized_volatility, Decimal("0.0001")))),
                priority=1,
                legs=[
                    SignalLegIntent(
                        instrument=InstrumentRef(symbol=raw.symbol, venue=self._trend_venue()),
                        side="buy" if raw.target_weight >= 0 else "sell",
                        order_type="market",
                        limit_price=None,
                        time_in_force="DAY",
                    )
                ],
                ts=observed_at,
            )
            intent = build_order_intent(
                instrument=InstrumentRef(symbol=raw.symbol, venue=self._trend_venue()),
                side=signal.legs[0].side,
                quantity=signal.quantity,
                order_type="market",
                strategy_name=signal.strategy.value,
                signal_ref=f"{signal.strategy.value}:{signal.ticker}:{signal.ts.isoformat()}",
                seed=f"{raw.symbol}:{observed_at.isoformat()}",
                created_at=observed_at,
            )
            return CandidateTrade(
                strategy_type="trend",
                instrument=raw.symbol,
                expected_return=raw.momentum_return,
                expected_risk=trade_candidate.cvar_95_loss / max(Decimal("1"), trade_candidate.notional_reference),
                liquidity_score=Decimal("1") - trade_candidate.liquidity_penalty,
                time_horizon=f"{trade_candidate.horizon_days}d",
                metadata={"source": "trend_target", "spread_bps": "12", "latest_price": format(latest_price, "f")},
                trade_candidate=trade_candidate,
                execute=lambda decision, intent=intent, price=latest_price, signal=signal: self._execute_trend_intent(decision, intent, price, signal),
                is_live=True,
            )
        return raw

    def _normalize_options_candidate(self, raw) -> CandidateTrade:
        if isinstance(raw, CandidateTrade):
            return raw
        if isinstance(raw, tuple) and len(raw) == 2 and isinstance(raw[0], StructureSelectionRecord) and isinstance(raw[1], StructureScenarioSummary):
            selection, summary = raw
            trade_candidate = candidate_from_options_selection(selection, summary)
            expected = summary.expected_pnl
            notional = max(summary.expected_entry_cost, Decimal("1"))
            return CandidateTrade(
                strategy_type="options",
                instrument=selection.underlying,
                expected_return=expected / notional,
                expected_risk=abs(summary.cvar_95_loss) / notional,
                liquidity_score=max(Decimal("0"), Decimal("1") - trade_candidate.liquidity_penalty),
                time_horizon=f"{selection.horizon_days}d",
                metadata={"source": selection.source, "structure_name": selection.selected_structure_name, "spread_bps": format(selection.slippage_bps, 'f')},
                trade_candidate=trade_candidate,
                execute=None,
                is_live=False,
            )
        return raw

    def _normalize_convexity_candidate(self, raw) -> CandidateTrade:
        if isinstance(raw, CandidateTrade):
            return raw
        if isinstance(raw, PendingAllocationAction):
            trade_candidate = raw.candidate
            expected_return = Decimal("0")
            if trade_candidate.notional_reference > 0:
                expected_return = trade_candidate.expected_pnl_after_costs / trade_candidate.notional_reference
            expected_risk = Decimal("0")
            if trade_candidate.notional_reference > 0:
                expected_risk = abs(trade_candidate.cvar_95_loss) / trade_candidate.notional_reference
            return CandidateTrade(
                strategy_type="convexity",
                instrument=trade_candidate.instrument_key,
                expected_return=expected_return,
                expected_risk=expected_risk,
                liquidity_score=max(Decimal("0"), Decimal("1") - trade_candidate.liquidity_penalty),
                time_horizon=f"{trade_candidate.horizon_days}d",
                metadata={"source": trade_candidate.source_kind, "spread_bps": "0", "paper_only": True},
                trade_candidate=trade_candidate,
                execute=raw.execute,
                is_live=False,
            )
        return raw

    def _build_risk_state(self) -> PortfolioRiskState:
        trend_snapshots = self.trend_repo.load_equity_snapshots()
        latest_equity = trend_snapshots[-1].equity if trend_snapshots else Decimal("100000")
        latest_prev = trend_snapshots[-2].equity if len(trend_snapshots) >= 2 else latest_equity
        latest_positions = self.trend_repo.load_positions()
        gross_trend = sum((abs(position.market_value) for position in latest_positions), Decimal("0"))
        return PortfolioRiskState(
            observed_at=datetime.now(timezone.utc),
            portfolio_equity=latest_equity,
            daily_realized_pnl=latest_equity - latest_prev,
            current_gross_notional=gross_trend,
            gross_notional_cap=latest_equity,
            allocated_notional_by_sleeve=self._allocated_by_sleeve(),
            allocated_notional_by_bucket=self._allocated_by_bucket(),
        )

    def _current_portfolio_equity(self) -> Decimal:
        trend_snapshots = self.trend_repo.load_equity_snapshots()
        return trend_snapshots[-1].equity if trend_snapshots else Decimal("100000")

    def _allocated_by_sleeve(self) -> dict[str, Decimal]:
        latest_run = self.allocation_repo.load_latest_run()
        if latest_run is None:
            return {}
        totals: dict[str, Decimal] = {}
        for decision in self.allocation_repo.load_decisions(latest_run.run_id):
            if not decision.approved:
                continue
            totals[decision.sleeve] = totals.get(decision.sleeve, Decimal("0")) + decision.target_notional
        return totals

    def _allocated_by_bucket(self) -> dict[str, Decimal]:
        latest_run = self.allocation_repo.load_latest_run()
        if latest_run is None:
            return {}
        totals: dict[str, Decimal] = {}
        for decision in self.allocation_repo.load_decisions(latest_run.run_id):
            if not decision.approved:
                continue
            totals[decision.correlation_bucket] = totals.get(decision.correlation_bucket, Decimal("0")) + decision.target_notional
        return totals

    def _latest_trend_price(self, symbol: str) -> Decimal:
        bars = self.trend_repo.load_recent_bars(symbol, 1)
        if not bars:
            return Decimal("100")
        return to_decimal(bars[-1].close)

    def _execute_trend_intent(self, decision: AllocationDecision, intent, price: Decimal, signal: Signal) -> bool:
        success, outcomes = self.execution_router.execute_order_intents(
            [intent],
            source_metadata={
                "signal": signal.strategy.value,
                "allocation_run_id": decision.run_id,
                "allocation_candidate_id": decision.candidate_id,
            },
            market_prices={signal.ticker: price},
            market_state_inputs=[
                MarketStateInput(
                    observed_at=signal.ts,
                    instrument=InstrumentRef(symbol=signal.ticker, venue=intent.instrument.venue),
                    source="portfolio_orchestrator",
                    reference_kind="close",
                    reference_price=price,
                    provenance="portfolio_orchestrator",
                    order_id=intent.order_id,
                )
            ],
        )
        return success and bool(outcomes)

    def _trend_venue(self) -> str:
        live_capable = bool(getattr(getattr(self.execution_router, "equity_broker", None), "is_live_capable", lambda: False)())
        if getattr(self.execution_router, "live_trading", False) and live_capable:
            return "live_equity"
        return "paper_equity"

    def _execution_allowed(self, candidate: CandidateTrade) -> bool:
        if candidate.expected_return <= 0:
            return False
        if candidate.liquidity_score < self.config.minimum_liquidity_score:
            return False
        spread_bps = to_decimal(candidate.metadata.get("spread_bps", "0"))
        if spread_bps > self.config.maximum_spread_bps:
            return False
        return True

    def _sync_execution_status(self, decision: AllocationDecision) -> None:
        outcomes = [
            outcome
            for outcome in self.kalshi_repo.load_execution_outcomes()
            if str(outcome.metadata.get("allocation_run_id", "")) == decision.run_id
            and str(outcome.metadata.get("allocation_candidate_id", "")) == decision.candidate_id
        ]
        if not outcomes:
            self.allocation_repo.update_decision_execution(
                run_id=decision.run_id,
                candidate_id=decision.candidate_id,
                execution_price=None,
                slippage_estimate=None,
                fill_status="not_executed",
            )
            return
        latest = max(outcomes, key=lambda item: item.created_at)
        slippage = None
        reference = latest.metadata.get("reference_price_cents")
        if latest.price_cents is not None and reference is not None:
            slippage = format((to_decimal(latest.price_cents) - to_decimal(reference)) / Decimal("100"), "f")
        self.allocation_repo.update_decision_execution(
            run_id=decision.run_id,
            candidate_id=decision.candidate_id,
            execution_price=None if latest.price_cents is None else format(to_decimal(latest.price_cents) / Decimal("100"), "f"),
            slippage_estimate=slippage,
            fill_status=latest.status,
        )
