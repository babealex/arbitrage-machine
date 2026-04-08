from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

from app.allocation.coordinator import PendingAllocationAction
from app.allocation.engine import AllocationEngine
from app.db import Database
from app.execution.models import ExecutionOutcome, SignalLegIntent
from app.instruments.models import InstrumentRef
from app.models import Signal, SignalEdge, StrategyName
from app.options.scenario_engine import StructureScenarioSummary
from app.options.selection import StructureSelectionRecord
from app.persistence.allocation_repo import AllocationRepository
from app.persistence.kalshi_repo import KalshiRepository
from app.persistence.market_state_repo import MarketStateRepository
from app.persistence.portfolio_cycle_repo import PortfolioCycleRepository
from app.persistence.reporting_repo import ReportingRepository
from app.persistence.trend_repo import TrendRepository
from app.portfolio.orchestrator import PortfolioOrchestrator, PortfolioOrchestratorConfig
from app.portfolio.replay import PortfolioReplayEngine
from app.reporting import PaperTradingReporter
from app.strategies.convexity.lifecycle import ConvexityCycleResult, ConvexityRunSummary, export_allocator_candidates
from app.strategies.convexity.models import ConvexTradeCandidate, ConvexityOpportunity
from app.strategies.trend.models import DailyBar, TrendTarget


class FakeKalshiEngine:
    def get_candidates(self):
        return [
            Signal(
                strategy=StrategyName.CROSS_MARKET,
                ticker="FED-1",
                action="buy_yes",
                quantity=2,
                edge=SignalEdge.from_values(edge_before_fees="0.04", edge_after_fees="0.03", fee_estimate="0.01"),
                source="cross_market_probability_map",
                confidence=0.8,
                priority=1,
                legs=[SignalLegIntent(instrument=InstrumentRef(symbol="FED-1", venue="kalshi", contract_side="yes"), side="buy", order_type="limit", limit_price=Decimal("0.40"), time_in_force="IOC")],
                ts=datetime(2026, 3, 22, 12, 0, tzinfo=timezone.utc),
            )
        ]


class FakeTrendEngine:
    def get_targets(self):
        return [
            TrendTarget(
                symbol="SPY",
                momentum_return=Decimal("0.03"),
                realized_volatility=Decimal("0.05"),
                target_weight=Decimal("0.10"),
            )
        ]


class FakeOptionsEngine:
    def get_candidates(self):
        observed_at = datetime(2026, 3, 22, 12, 0, tzinfo=timezone.utc)
        selection = StructureSelectionRecord(
            underlying="SPY",
            observed_at=observed_at,
            source="options_test",
            regime_name="event_term_structure",
            due_at_1d=observed_at + timedelta(days=1),
            due_at_3d=observed_at + timedelta(days=3),
            due_at_expiry=datetime(2026, 4, 17, 12, 0, tzinfo=timezone.utc),
            candidate_count=3,
            selected_structure_name="call_calendar_500",
            objective_value=Decimal("25"),
            horizon_days=7,
            path_count=200,
            seed=1,
            annual_drift=Decimal("0"),
            annual_volatility=Decimal("0.10"),
            event_day=1,
            event_jump_mean=Decimal("0"),
            event_jump_stddev=Decimal("0.02"),
            iv_mean_reversion=Decimal("0.15"),
            iv_regime_level=Decimal("0.20"),
            iv_volatility=Decimal("0.10"),
            event_crush=Decimal("0.06"),
            exit_spread_fraction=Decimal("0.10"),
            slippage_bps=Decimal("10"),
            stop_out_loss=None,
            tail_penalty=Decimal("0.50"),
            uncertainty_penalty=Decimal("1.00"),
        )
        summary = StructureScenarioSummary(
            underlying="SPY",
            observed_at=observed_at,
            source="options_test",
            structure_name="call_calendar_500",
            horizon_days=7,
            path_count=200,
            seed=1,
            expected_pnl=Decimal("25"),
            probability_of_profit=Decimal("0.62"),
            cvar_95_loss=Decimal("-60"),
            expected_max_drawdown=Decimal("40"),
            stop_out_probability=Decimal("0.15"),
            predicted_daily_vol=Decimal("12"),
            expected_entry_cost=Decimal("200"),
            stress_expected_pnl=Decimal("15"),
            model_uncertainty_penalty=Decimal("10"),
        )
        return [(selection, summary)]


class FakeConvexityEngine:
    def get_candidates(self):
        observed_at = datetime(2026, 3, 22, 12, 0, tzinfo=timezone.utc)
        candidate = ConvexTradeCandidate(
            symbol="QQQ",
            setup_type="momentum",
            structure_type="long_call",
            expiry=date(2026, 4, 17),
            legs=[{"symbol": "QQQ_20260417C480", "expiry": date(2026, 4, 17), "strike": Decimal("480"), "option_type": "call", "quantity": 1, "premium": Decimal("1.10")}],
            debit=Decimal("110"),
            max_loss=Decimal("110"),
            max_profit_estimate=Decimal("880"),
            reward_to_risk=Decimal("8"),
            expected_value=Decimal("40"),
            expected_value_after_costs=Decimal("25"),
            convex_score=Decimal("11"),
            breakout_sensitivity=Decimal("0.7"),
            liquidity_score=Decimal("0.85"),
            confidence_score=Decimal("0.8"),
            metadata={"candidate_id": "convexity-1"},
        )
        cycle = ConvexityCycleResult(
            run_id="convexity-run",
            opportunities=[
                ConvexityOpportunity(
                    symbol="QQQ",
                    setup_type="momentum",
                    event_time=None,
                    spot_price=Decimal("450"),
                    expected_move_pct=Decimal("0.05"),
                    implied_move_pct=Decimal("0.03"),
                    realized_vol_pct=Decimal("0.04"),
                    implied_vol_pct=Decimal("0.03"),
                    volume_spike_ratio=Decimal("2"),
                    breakout_score=Decimal("0.7"),
                    metadata={"direction": "bullish"},
                )
            ],
            candidates=[candidate],
            accepted_candidates=[candidate],
            selected_candidates=[candidate],
            order_intents=[],
            summary=ConvexityRunSummary(
                run_id="convexity-run",
                timestamp=observed_at.isoformat(),
                scanned_opportunities=1,
                candidate_trades=1,
                accepted_trades=1,
                rejected_for_liquidity=0,
                rejected_for_negative_ev=0,
                rejected_for_risk_budget=0,
                rejected_other=0,
                open_convexity_risk=Decimal("0"),
                avg_reward_to_risk=Decimal("8"),
                avg_expected_value_after_costs=Decimal("25"),
                metadata_json={"paper_execution_only": True},
            ),
            rejection_reasons={},
        )
        return export_allocator_candidates(cycle)


class EmptyEngine:
    def get_candidates(self):
        return []

    def get_targets(self):
        return []


class DummyExecutionRouter:
    def __init__(self, repo: KalshiRepository) -> None:
        self.repo = repo
        self.signal_calls: list[str] = []
        self.order_calls: list[str] = []

    def execute_signal(self, signal: Signal, *, extra_source_metadata=None) -> bool:
        self.signal_calls.append(signal.ticker)
        self.repo.persist_execution_outcome(
            ExecutionOutcome(
                client_order_id=f"kalshi:{signal.ticker}",
                instrument=InstrumentRef(symbol=signal.ticker, venue="kalshi", contract_side="yes"),
                side="buy",
                action="limit",
                status="simulated",
                requested_quantity=signal.quantity,
                filled_quantity=signal.quantity,
                price_cents=40,
                time_in_force="IOC",
                is_flatten=False,
                created_at=signal.ts,
                metadata=dict(extra_source_metadata or {}, reference_price_cents=40),
            )
        )
        return True

    def execute_order_intents(self, intents, *, source_metadata=None, market_prices=None, market_state_inputs=None, flatten_on_failure=False):
        intent = intents[0]
        self.order_calls.append(intent.instrument.symbol)
        self.repo.persist_execution_outcome(
            ExecutionOutcome.from_paper_equity_fill(
                client_order_id=intent.order_id,
                symbol=intent.instrument.symbol,
                side=intent.side,
                requested_quantity=int(intent.quantity),
                filled_quantity=int(intent.quantity),
                price=market_prices[intent.instrument.symbol],
                status="filled",
                created_at=intent.created_at,
                metadata=dict(source_metadata or {}, reference_price_cents="50000"),
            )
        )
        return True, self.repo.load_execution_outcomes()[-1:]


def test_orchestrator_aggregates_allocates_routes_and_persists(tmp_path: Path) -> None:
    db = Database(tmp_path / "orchestrator.db")
    trend_repo = TrendRepository(db)
    trend_repo.persist_bars(
        [
            DailyBar(symbol="SPY", bar_date=date(2026, 3, 21), close=Decimal("500")),
        ]
    )
    repo = KalshiRepository(db)
    router = DummyExecutionRouter(repo)
    orchestrator = PortfolioOrchestrator(
        PortfolioOrchestratorConfig(),
        db,
        AllocationEngine(),
        router,
        kalshi_engine=FakeKalshiEngine(),
        trend_engine=FakeTrendEngine(),
        options_engine=FakeOptionsEngine(),
        convexity_engine=FakeConvexityEngine(),
    )

    result = orchestrator.run_cycle()

    assert len(result.candidates) == 4
    assert result.run.candidate_count == 4
    assert len(result.decisions) == 4
    assert "FED-1" in router.signal_calls
    assert "SPY" in router.order_calls
    loaded_run = AllocationRepository(db).load_run(result.run.run_id)
    assert loaded_run is not None
    loaded_decisions = AllocationRepository(db).load_decisions(result.run.run_id)
    assert len(loaded_decisions) == 4
    assert any(decision.fill_status in {"simulated", "filled", "not_executed", "paper_only"} for decision in loaded_decisions)
    assert any(decision.strategy_family == "convexity" for decision in loaded_decisions)
    snapshot = PortfolioCycleRepository(db).load_snapshot(result.run.run_id)
    assert snapshot is not None
    assert snapshot.candidate_count == 4
    assert snapshot.strategy_counts_json["kalshi"] == 1
    assert snapshot.strategy_counts_json["convexity"] == 1


def test_orchestrator_no_trade_cycle_is_valid(tmp_path: Path) -> None:
    db = Database(tmp_path / "orchestrator_empty.db")
    router = DummyExecutionRouter(KalshiRepository(db))
    orchestrator = PortfolioOrchestrator(
        PortfolioOrchestratorConfig(),
        db,
        AllocationEngine(),
        router,
        kalshi_engine=EmptyEngine(),
        trend_engine=EmptyEngine(),
        options_engine=EmptyEngine(),
    )

    result = orchestrator.run_cycle()

    assert result.run.candidate_count == 0
    assert result.decisions == []
    assert result.execution_results == {}
    assert router.signal_calls == []
    assert router.order_calls == []


def test_orchestrator_run_is_replayable(tmp_path: Path) -> None:
    db = Database(tmp_path / "orchestrator_replay.db")
    trend_repo = TrendRepository(db)
    trend_repo.persist_bars([DailyBar(symbol="SPY", bar_date=date(2026, 3, 21), close=Decimal("500"))])
    repo = KalshiRepository(db)
    router = DummyExecutionRouter(repo)
    orchestrator = PortfolioOrchestrator(
        PortfolioOrchestratorConfig(),
        db,
        AllocationEngine(),
        router,
        kalshi_engine=FakeKalshiEngine(),
        trend_engine=FakeTrendEngine(),
        options_engine=FakeOptionsEngine(),
    )

    result = orchestrator.run_cycle()
    replay = PortfolioReplayEngine(
        allocation_repo=AllocationRepository(db),
        kalshi_repo=repo,
        market_state_repo=MarketStateRepository(db),
        portfolio_cycle_repo=PortfolioCycleRepository(db),
    ).replay_run(result.run.run_id)

    assert replay.run.run_id == result.run.run_id
    assert replay.cycle_snapshot is not None
    assert len(replay.decisions) == len(result.decisions)
    assert replay.replay.snapshot.equity >= replay.replay.snapshot.cash
    assert replay.execution_outcomes


def test_report_includes_deployment_readiness_payload(tmp_path: Path) -> None:
    db = Database(tmp_path / "orchestrator_report.db")
    report = PaperTradingReporter(ReportingRepository(db), tmp_path / "report.json").write(
        {
            "convexity_runtime_diagnostics": {
                "enabled": True,
                "status": "no_trade",
                "no_future_catalysts": True,
                "candidate_trades": 0,
                "selected_trades": 0,
            },
            "deployment_readiness": {
                "orchestrator_enabled": True,
                "live_trading": True,
                "live_enabled_sleeves": ["kalshi", "trend"],
                "live_capable_sleeves": ["kalshi"],
                "degraded_sleeves": ["trend_paper_only", "options_paper_only"],
                "unsupported_live_sleeves": ["trend"],
                "kalshi_auth_configured": True,
                "kill_switch_active": False,
                "kill_switch_reasons": [],
                "latest_cycle_run_id": "run-1",
                "latest_replayable_run_id": "run-1",
                "runtime_can_trade": False,
                "halt_reasons": ["unsupported_live_sleeve:trend"],
            }
        }
    )

    assert report["deployment_readiness"] is not None
    assert report["deployment_readiness"]["unsupported_live_sleeves"] == ["trend"]
    assert report["convexity_runtime_diagnostics"]["no_future_catalysts"] is True
