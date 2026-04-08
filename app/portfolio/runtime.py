from __future__ import annotations

import time
from datetime import date, datetime, timezone
from decimal import Decimal

from app.alerts.notifier import Notifier
from app.allocation.engine import AllocationEngine
from app.allocation.feedback import AllocationFeedbackEngine
from app.allocation.models import CapitalSleeve
from app.brokers.ibkr_trend_adapter import IbkrTrendBrokerAdapter, evaluate_trend_live_readiness
from app.persistence.allocation_repo import AllocationRepository
from app.common.runtime_health import assert_runtime_headroom, run_startup_checks, write_runtime_status
from app.config import Settings
from app.db import Database
from app.execution.router import ExecutionRouter
from app.execution.equity_broker import ManualLiveEquityBroker, UnsupportedLiveEquityBroker
from app.external_data.fed_futures import ExternalDataResult, FedFuturesProvider
from app.external_data.probability_mapper import map_market_probability
from app.kalshi.rest_client import KalshiRestClient
from app.logging_setup import setup_logging
from app.market_data.service import MarketDataService
from app.models import Signal
from app.portfolio.deployment import build_portfolio_deployment_readiness
from app.portfolio.orchestrator import PortfolioOrchestrator, PortfolioOrchestratorConfig
from app.portfolio.state import PortfolioState
from app.persistence.kalshi_repo import KalshiRepository
from app.persistence.market_state_repo import MarketStateRepository
from app.persistence.event_repo import EventRepository
from app.persistence.options_repo import OptionsRepository
from app.persistence.portfolio_cycle_repo import PortfolioCycleRepository
from app.persistence.reporting_repo import ReportingRepository
from app.persistence.risk_repo import RiskRepository
from app.persistence.trend_broker_repo import TrendBrokerRepository
from app.persistence.trend_repo import TrendRepository
from app.reporting import PaperTradingReporter
from app.risk.manager import RiskManager
from app.runtime.health import assess_layer2_trading_readiness, assess_portfolio_runtime_readiness
from app.strategies.convexity.lifecycle import export_allocator_candidates, run_convexity_cycle
from app.strategies.convexity.models import ConvexityAccountState, OptionContractQuote
from app.strategies.cross_market import CrossMarketStrategy
from app.strategies.monotonicity import MonotonicityStrategy
from app.strategies.partition import PartitionStrategy
from app.strategies.trend.portfolio import TrendPaperEngine


class KalshiCandidateEngine:
    def __init__(
        self,
        *,
        settings: Settings,
        repo: KalshiRepository,
        logger,
        market_data: MarketDataService,
        external_provider: FedFuturesProvider,
        monotonicity: MonotonicityStrategy,
        partition: PartitionStrategy,
        cross_market: CrossMarketStrategy,
    ) -> None:
        self.settings = settings
        self.repo = repo
        self.logger = logger
        self.market_data = market_data
        self.external_provider = external_provider
        self.monotonicity = monotonicity
        self.partition = partition
        self.cross_market = cross_market
        self.last_loop_stats: dict[str, object] = {}

    def get_candidates(self) -> list[Signal]:
        raw_markets = self.market_data.refresh_markets()
        filter_result = self.market_data.filter_markets(raw_markets)
        markets = self.market_data.hydrate_structural_quotes(filter_result.included)
        ladders = self.market_data.build_ladders(markets)
        if self.settings.app_mode == "safe":
            external = ExternalDataResult(
                distributions={},
                stale=True,
                errors=[],
                fetched_at=datetime.now(timezone.utc),
                source="skipped_safe_mode",
            )
        else:
            external = self.external_provider.fetch()
        readiness = assess_layer2_trading_readiness(
            market_data_last_update=self.market_data.last_update,
            max_market_data_staleness_seconds=self.settings.max_market_data_staleness_seconds,
            external_stale=external.stale,
            external_errors=external.errors,
            app_mode=self.settings.app_mode,
            reconciliation_success=True,
            reconciliation_mismatches=[],
            now=datetime.now(timezone.utc),
        )
        if not readiness.can_trade:
            self.last_loop_stats = {"trading_readiness": False, "trading_halt_reasons": readiness.halt_reasons, "markets_loaded": len(markets)}
            return []
        signals: list[Signal] = []
        equity_reference = Decimal("100000")
        for ladder in ladders.values():
            ladder_signals, evaluations = self.monotonicity.evaluate(ladder, float(equity_reference))
            for evaluation in evaluations:
                self.repo.persist_evaluation(evaluation)
            signals.extend(ladder_signals)
        if self.settings.enable_partition_strategy:
            by_event_all: dict[str, list] = {}
            for market in filter_result.included:
                by_event_all.setdefault(market.event_ticker, []).append(market)
            by_event_tradeable: dict[str, list] = {}
            for market in markets:
                by_event_tradeable.setdefault(market.event_ticker, []).append(market)
            for event_ticker, all_group in by_event_all.items():
                tradeable_group = by_event_tradeable.get(event_ticker, [])
                if len(all_group) < 2:
                    continue
                group_signals, evaluations = self.partition.evaluate(tradeable_group, float(equity_reference), all_group)
                for evaluation in evaluations:
                    self.repo.persist_evaluation(evaluation)
                signals.extend(group_signals)
        if self.settings.app_mode in {"balanced", "aggressive"} and not external.stale:
            for market in markets:
                mapped_probability, mapping_metadata = map_market_probability(market, external.distributions)
                try:
                    orderbook = self.market_data.refresh_orderbook(market.ticker)
                except Exception as exc:
                    self.logger.error("portfolio_orderbook_refresh_failed", extra={"event": {"ticker": market.ticker, "error": str(exc)}})
                    continue
                cross_signals, evaluations = self.cross_market.evaluate(market, mapped_probability, orderbook, float(equity_reference))
                for evaluation in evaluations:
                    evaluation.metadata.update(mapping_metadata)
                    self.repo.persist_evaluation(evaluation)
                signals.extend(cross_signals)
        self.last_loop_stats = {
            "trading_readiness": True,
            "trading_halt_reasons": [],
            "markets_loaded": len(markets),
            "markets_seen": len(raw_markets),
            "external_data_source_used": external.source,
        }
        return signals


class TrendCandidateEngine:
    def __init__(self, settings: Settings, trend_engine: TrendPaperEngine) -> None:
        self.settings = settings
        self.trend_engine = trend_engine
        self.live_capable = trend_engine.live_capable

    def get_targets(self):
        if self.settings.live_trading and not self.live_capable:
            return []
        readiness, _bars, targets = self.trend_engine.get_targets(date.today())
        return targets if readiness.can_trade else []


class OptionsCandidateEngine:
    def __init__(self, settings: Settings, repo: OptionsRepository) -> None:
        self.settings = settings
        self.repo = repo
        self.live_capable = False

    def get_candidates(self):
        if not self.settings.enable_options_sleeve:
            return []
        if self.settings.live_trading:
            return []
        candidates = []
        for selection in self.repo.load_all_structure_selections():
            summary = self.repo.load_structure_scenario_summary(
                underlying=selection.underlying,
                observed_at=selection.observed_at,
                structure_name=selection.selected_structure_name,
            )
            if summary is None:
                continue
            candidates.append((selection, summary))
        return candidates


class ConvexityCatalystProvider:
    def __init__(self, settings: Settings, event_repo: EventRepository) -> None:
        self.settings = settings
        self.event_repo = event_repo

    def build_inputs(self, underlyings: list[str]) -> tuple[dict[str, list[dict]], dict[str, dict], dict[str, object]]:
        calendars: dict[str, list[dict]] = {symbol: [] for symbol in underlyings}
        news_state: dict[str, dict] = {symbol: {"direction": "bullish"} for symbol in underlyings}
        configured_macro_rows = self._configured_events(self.settings.convexity_macro_event_rows, default_setup_type="macro")
        configured_earnings_rows = self._configured_events(self.settings.convexity_earnings_event_rows, default_setup_type="earnings")
        for row in configured_macro_rows:
            if row["symbol"] in calendars:
                calendars[row["symbol"]].append(row)
        for row in configured_earnings_rows:
            if row["symbol"] in calendars:
                calendars[row["symbol"]].append(row)
        recent_trade_candidates = self.event_repo.load_recent_trade_candidates(lookback_hours=72)
        for row in recent_trade_candidates:
            symbol = str(row["symbol"])
            if symbol not in news_state:
                continue
            confidence = Decimal(str(row.get("confidence", 0)))
            current_confidence = Decimal(str(news_state[symbol].get("confidence", "0")))
            if confidence < current_confidence:
                continue
            news_state[symbol] = {
                "direction": "bullish" if str(row.get("side", "buy")).lower() == "buy" else "bearish",
                "confidence": format(confidence, "f"),
                "source": "event_repo",
                "event_type": str(row.get("event_type", "unknown")),
            }
        return calendars, news_state, {
            "input_underlyings": len(underlyings),
            "configured_macro_events": len(configured_macro_rows),
            "configured_earnings_events": len(configured_earnings_rows),
            "recent_event_bias_rows": len(recent_trade_candidates),
            "calendar_symbols_with_events": sum(1 for rows in calendars.values() if rows),
            "news_symbols_with_bias": sum(1 for item in news_state.values() if item.get("source") == "event_repo"),
        }

    def _configured_events(self, rows: list[str], *, default_setup_type: str) -> list[dict]:
        parsed: list[dict] = []
        for row in rows:
            parts = [item.strip() for item in row.split("|")]
            if len(parts) < 3:
                continue
            try:
                event_time = datetime.fromisoformat(parts[2])
            except ValueError:
                continue
            parsed.append(
                {
                    "symbol": parts[0],
                    "setup_type": parts[1] or default_setup_type,
                    "event_time": event_time,
                    "direction": parts[3] if len(parts) >= 4 and parts[3] else "neutral",
                    "event_history_median_move": Decimal(parts[4]) if len(parts) >= 5 and parts[4] else Decimal("0.02"),
                }
            )
        return parsed


class ConvexityCandidateEngine:
    def __init__(self, settings: Settings, db: Database, repo: OptionsRepository, trend_repo: TrendRepository, event_repo: EventRepository) -> None:
        self.settings = settings
        self.db = db
        self.repo = repo
        self.trend_repo = trend_repo
        self.catalyst_provider = ConvexityCatalystProvider(settings, event_repo)
        self.live_capable = False
        self.last_cycle = None
        self.last_diagnostics: dict[str, object] = {"enabled": settings.enable_convexity_sleeve and settings.convexity_enabled, "status": "idle"}

    def get_candidates(self):
        if not self.settings.enable_convexity_sleeve or not self.settings.convexity_enabled:
            self.last_diagnostics = {"enabled": False, "status": "disabled"}
            return []
        market_state: dict[str, dict] = {}
        option_chains: dict[str, list[OptionContractQuote]] = {}
        calendars: dict[str, list[dict]] = {}
        news_state: dict[str, dict] = {}
        for underlying in self.repo.load_latest_chain_underlyings():
            snapshot = self.repo.load_latest_chain_snapshot(underlying=underlying)
            if snapshot is None:
                continue
            state_vector = self.repo.load_state_vector(underlying=underlying, observed_at=snapshot.observed_at)
            quotes = [
                OptionContractQuote(
                    symbol=quote.contract.contract_key(),
                    expiry=quote.contract.expiration_date,
                    strike=quote.contract.strike,
                    option_type=quote.contract.option_type,
                    bid=Decimal("0") if quote.bid is None else quote.bid,
                    ask=Decimal("0") if quote.ask is None else quote.ask,
                    mid=((quote.bid or Decimal("0")) + (quote.ask or Decimal("0"))) / Decimal("2") if quote.bid is not None and quote.ask is not None else (quote.last or Decimal("0")),
                    iv=quote.implied_volatility,
                    delta=quote.delta,
                    gamma=quote.gamma,
                    vega=quote.vega,
                    theta=quote.theta,
                    open_interest=quote.open_interest,
                    volume=quote.volume,
                )
                for quote in snapshot.quotes
            ]
            if not quotes:
                continue
            realized_vol = snapshot.realized_vol_20d or (None if state_vector is None else state_vector.realized_vol_20d) or Decimal("0")
            implied_vol = (None if state_vector is None else state_vector.atm_iv_near) or _average_option_iv(quotes)
            market_state[underlying] = {
                "spot_price": snapshot.spot_price,
                "option_chain": quotes,
                "realized_vol_pct": realized_vol,
                "implied_vol_pct": implied_vol,
                "breakout_score": Decimal("0"),
                "volume_spike_ratio": Decimal("1"),
                "as_of": snapshot.observed_at,
            }
            option_chains[underlying] = quotes
            calendars[underlying] = []
            news_state[underlying] = {"direction": "bullish"}
        provided_calendars, provided_news, input_summary = self.catalyst_provider.build_inputs(list(market_state))
        for symbol, rows in provided_calendars.items():
            calendars.setdefault(symbol, []).extend(rows)
        for symbol, item in provided_news.items():
            news_state[symbol] = item
        if not market_state:
            self.last_diagnostics = {
                "enabled": True,
                "status": "no_option_chains",
                "symbols_with_option_chains": 0,
                **input_summary,
            }
            return []
        account_state = ConvexityAccountState(
            nav=self._portfolio_equity(),
            available_cash=self._portfolio_equity(),
        )
        cycle = run_convexity_cycle(
            self.db,
            self.settings,
            market_state,
            calendars,
            news_state,
            option_chains,
            account_state,
            run_metadata=input_summary,
        )
        self.last_cycle = cycle
        self.last_diagnostics = {
            "enabled": True,
            "status": "ready" if cycle.selected_candidates else "no_trade",
            "symbols_with_option_chains": len(market_state),
            "configured_or_recent_catalysts": int(input_summary.get("configured_macro_events", 0)) + int(input_summary.get("configured_earnings_events", 0)) + int(input_summary.get("recent_event_bias_rows", 0)),
            "no_future_catalysts": (
                int(input_summary.get("configured_macro_events", 0)) == 0
                and int(input_summary.get("configured_earnings_events", 0)) == 0
                and int(input_summary.get("recent_event_bias_rows", 0)) == 0
            ),
            "no_scanned_opportunities": cycle.summary.scanned_opportunities == 0,
            "candidate_trades": cycle.summary.candidate_trades,
            "accepted_trades": cycle.summary.accepted_trades,
            "selected_trades": len(cycle.selected_candidates),
            "rejected_for_liquidity": cycle.summary.rejected_for_liquidity,
            "rejected_for_negative_ev": cycle.summary.rejected_for_negative_ev,
            "rejected_for_risk_budget": cycle.summary.rejected_for_risk_budget,
            "rejected_other": cycle.summary.rejected_other,
            **input_summary,
        }
        return export_allocator_candidates(cycle)

    def _portfolio_equity(self) -> Decimal:
        snapshots = self.trend_repo.load_equity_snapshots()
        if not snapshots:
            return Decimal("100000")
        return snapshots[-1].equity


def run_portfolio_orchestrator(settings: Settings) -> None:
    logger = setup_logging(settings)
    startup_check = run_startup_checks(settings, "portfolio_orchestrator")
    for warning in startup_check.warnings:
        logger.warning("startup_validation_warning", extra={"event": {"warning": warning}})
    if startup_check.errors:
        write_runtime_status(
            settings,
            "portfolio_orchestrator",
            "startup_failed",
            message=",".join(startup_check.errors),
            details=startup_check.details,
            startup_check=startup_check,
        )
        raise RuntimeError(f"portfolio_orchestrator_startup_failed:{','.join(startup_check.errors)}")
    db = Database(settings.db_path)
    repo = KalshiRepository(db)
    trend_repo = TrendRepository(db)
    options_repo = OptionsRepository(db)
    event_repo = EventRepository(db)
    reporting_repo = ReportingRepository(db)
    market_state_repo = MarketStateRepository(db)
    risk_repo = RiskRepository(db)
    allocation_repo = AllocationRepository(db)
    portfolio_cycle_repo = PortfolioCycleRepository(db)
    trend_broker_repo = TrendBrokerRepository(db)
    notifier = Notifier(settings)
    client = KalshiRestClient(settings)
    market_data = MarketDataService(client, settings)
    portfolio = PortfolioState(client)
    risk = RiskManager(
        max_daily_loss_pct=settings.max_daily_loss_pct,
        max_total_exposure_pct=settings.max_total_exposure_pct,
        max_position_per_trade_pct=settings.max_position_per_trade_pct,
        max_failed_trades_per_day=settings.max_failed_trades_per_day,
        logger=logger,
        event_recorder=risk_repo.persist_transition,
    )
    equity_broker = UnsupportedLiveEquityBroker()
    trend_broker_adapter = None
    if settings.live_trading and settings.trend_live_broker_mode == "manual":
        equity_broker = ManualLiveEquityBroker()
    elif settings.live_trading and settings.trend_live_broker_mode == "ibkr":
        trend_broker_adapter = IbkrTrendBrokerAdapter(
            config=settings,
            logger=logger,
            trend_repo=trend_repo,
            signal_repo=repo,
            broker_repo=trend_broker_repo,
        )
        equity_broker = trend_broker_adapter
    execution_router = ExecutionRouter(
        client,
        repo,
        portfolio,
        risk,
        logger,
        settings.live_trading,
        market_state_repo=market_state_repo,
        equity_broker=equity_broker,
    )
    feedback_engine = AllocationFeedbackEngine(
        allocation_repo=allocation_repo,
        kalshi_repo=repo,
        market_state_repo=market_state_repo,
        options_repo=options_repo,
    )
    trend_engine = TrendPaperEngine(
        signal_repo=repo,
        trend_repo=trend_repo,
        risk=risk,
        execution_router=execution_router,
        universe=settings.trend_symbols,
        weight_mode=settings.trend_weight_mode,
        allocation_adjustments_loader=lambda: AllocationRepository(db).load_latest_adjustments(),
    )
    orchestrator = PortfolioOrchestrator(
        PortfolioOrchestratorConfig(
            cycle_interval_seconds=settings.portfolio_cycle_interval_seconds,
            max_capital_per_strategy={key: Decimal(str(value)) for key, value in settings.max_capital_per_strategy.items()},
        ),
        db,
        AllocationEngine(
            sleeves=[
                CapitalSleeve(name="base", capital_cap_fraction=Decimal(str(settings.max_capital_trend)), max_trade_fraction=Decimal("0.25"), max_loss_budget_fraction=Decimal("0.015")),
                CapitalSleeve(name="opportunistic", capital_cap_fraction=Decimal(str(settings.max_capital_kalshi)), max_trade_fraction=Decimal("0.15"), max_loss_budget_fraction=Decimal("0.010")),
                CapitalSleeve(name="attack", capital_cap_fraction=Decimal(str(settings.max_capital_options)), max_trade_fraction=Decimal("0.08"), max_loss_budget_fraction=Decimal("0.0075")),
            ]
        ),
        execution_router,
        kalshi_engine=KalshiCandidateEngine(
            settings=settings,
            repo=repo,
            logger=logger,
            market_data=market_data,
            external_provider=FedFuturesProvider(settings),
            monotonicity=MonotonicityStrategy(
                settings.min_edge_bps,
                settings.slippage_buffer_bps,
                settings.max_position_per_trade_pct,
                settings.monotonicity_min_top_level_depth,
                settings.monotonicity_min_size_multiple,
                settings.monotonicity_ladder_cooldown_seconds,
            ),
            partition=PartitionStrategy(
                settings.min_edge_bps,
                settings.slippage_buffer_bps,
                settings.max_position_per_trade_pct,
                settings.min_depth,
                settings.partition_min_coverage,
            ),
            cross_market=CrossMarketStrategy(
                min_edge=settings.min_edge,
                slippage_buffer=settings.slippage_buffer_bps / 10_000.0,
                min_depth=settings.min_depth,
                min_time_to_expiry_seconds=settings.min_time_to_expiry_seconds,
                max_position_pct=settings.max_position_per_trade_pct,
                directional_allowed=settings.app_mode == "aggressive",
            ),
        ),
        trend_engine=TrendCandidateEngine(settings, trend_engine),
        options_engine=None if not settings.enable_options_sleeve else OptionsCandidateEngine(settings, options_repo),
        convexity_engine=None if not settings.enable_convexity_sleeve else ConvexityCandidateEngine(settings, db, options_repo, trend_repo, event_repo),
        logger=logger,
    )
    reporter = PaperTradingReporter(reporting_repo, settings.report_path)
    write_runtime_status(settings, "portfolio_orchestrator", "starting", details=startup_check.details, startup_check=startup_check)

    while True:
        try:
            disk_headroom = assert_runtime_headroom(settings)
            portfolio.reconcile(repo)
            repo.persist_snapshot(portfolio.snapshot)
            latest_cycle_snapshot = portfolio_cycle_repo.load_latest_snapshot()
            latest_allocation_run = allocation_repo.load_latest_run()
            trend_broker_readiness = evaluate_trend_live_readiness(trend_broker_adapter)
            trend_live_capable = trend_broker_readiness.is_ready
            if orchestrator.trend_engine is not None:
                orchestrator.trend_engine.live_capable = trend_live_capable
            portfolio_readiness = assess_portfolio_runtime_readiness(
                live_trading=settings.live_trading,
                live_enabled_sleeves=settings.live_enabled_sleeves,
                risk_killed=risk.state.killed,
                risk_reasons=risk.state.reasons,
                kalshi_context=getattr(orchestrator.kalshi_engine, "last_loop_stats", {}),
                kalshi_live_capable=settings.kalshi_mode == "authenticated",
                trend_live_capable=trend_live_capable,
                options_live_capable=getattr(orchestrator.options_engine, "live_capable", False),
            )
            deployment_readiness = build_portfolio_deployment_readiness(
                orchestrator_enabled=settings.enable_portfolio_orchestrator,
                live_trading=settings.live_trading,
                live_enabled_sleeves=settings.live_enabled_sleeves,
                kalshi_auth_configured=settings.kalshi_mode == "authenticated",
                portfolio_readiness=portfolio_readiness,
                latest_cycle_run_id=None if latest_cycle_snapshot is None else latest_cycle_snapshot.run_id,
                latest_replayable_run_id=None if latest_allocation_run is None else latest_allocation_run.run_id,
            )
            if settings.live_trading and not portfolio_readiness.can_trade:
                for reason in portfolio_readiness.halt_reasons:
                    risk.kill(reason, portfolio_readiness.details)
                write_runtime_status(
                    settings,
                    "portfolio_orchestrator",
                    "halted",
                    message=",".join(portfolio_readiness.halt_reasons),
                    details={
                        "portfolio_runtime_readiness": portfolio_readiness.details,
                        "deployment_readiness": deployment_readiness.as_dict(),
                    },
                    update_heartbeat=True,
                )
                time.sleep(settings.portfolio_cycle_interval_seconds)
                continue
            result = orchestrator.run_cycle()
            snapshot = portfolio_cycle_repo.load_snapshot(result.run.run_id)
            if snapshot is not None:
                snapshot.readiness_json.update(
                    {
                        "disk_headroom": bool(disk_headroom),
                        "kalshi_candidate_context": getattr(orchestrator.kalshi_engine, "last_loop_stats", {}),
                        "trend_target_count": 0 if orchestrator.trend_engine is None else len(orchestrator.trend_engine.get_targets()),
                        "trend_cycle_diagnostics": {} if orchestrator.trend_engine is None else orchestrator.trend_engine.trend_engine.last_diagnostics,
                        "options_candidate_count": len(orchestrator.options_engine.get_candidates()) if orchestrator.options_engine is not None else 0,
                        "convexity_candidate_count": 0 if getattr(orchestrator, "convexity_engine", None) is None or orchestrator.convexity_engine.last_cycle is None else len(orchestrator.convexity_engine.last_cycle.selected_candidates),
                        "convexity_runtime_diagnostics": {} if getattr(orchestrator, "convexity_engine", None) is None else orchestrator.convexity_engine.last_diagnostics,
                        "portfolio_runtime_readiness": portfolio_readiness.details,
                        "deployment_readiness": deployment_readiness.as_dict(),
                        "trend_broker_live_status": {
                            "broker": trend_broker_readiness.broker_name,
                            "configured": settings.trend_live_broker_mode == "ibkr",
                            "readiness": trend_broker_readiness.is_ready,
                            "reasons": trend_broker_readiness.reasons,
                            "last_auth_ok_at": None if trend_broker_readiness.last_successful_auth_at is None else trend_broker_readiness.last_successful_auth_at.isoformat(),
                            "last_reconciliation_ok_at": None if trend_broker_readiness.last_reconciliation_check_at is None else trend_broker_readiness.last_reconciliation_check_at.isoformat(),
                            "last_order_reject_at": trend_broker_repo.latest_reject_timestamp(),
                            "reject_count": trend_broker_repo.count_rejects(),
                            "reconnect_count": trend_broker_repo.count_reconnect_events(),
                            "reconciliation_mismatch_count": trend_broker_repo.count_reconciliation_mismatches(),
                            "execution_mode": "live_equity" if trend_live_capable else "paper_equity",
                        },
                    }
                )
                portfolio_cycle_repo.persist_snapshot(snapshot)
            feedback_result = feedback_engine.refresh_run(result.run.run_id)
            deployment_readiness = build_portfolio_deployment_readiness(
                orchestrator_enabled=settings.enable_portfolio_orchestrator,
                live_trading=settings.live_trading,
                live_enabled_sleeves=settings.live_enabled_sleeves,
                kalshi_auth_configured=settings.kalshi_mode == "authenticated",
                portfolio_readiness=portfolio_readiness,
                latest_cycle_run_id=result.run.run_id,
                latest_replayable_run_id=result.run.run_id,
            )
            report = reporter.write(
                {
                    "portfolio_cycle_run_id": result.run.run_id,
                    "portfolio_candidate_count": result.run.candidate_count,
                    "portfolio_approved_count": result.run.approved_count,
                    "portfolio_rejected_count": result.run.rejected_count,
                    "portfolio_feedback_outcomes": 0 if feedback_result is None else len(feedback_result.outcomes),
                    "trend_cycle_diagnostics": {} if orchestrator.trend_engine is None else orchestrator.trend_engine.trend_engine.last_diagnostics,
                    "portfolio_runtime_readiness": {
                        "can_trade": portfolio_readiness.can_trade,
                        "halt_reasons": portfolio_readiness.halt_reasons,
                        "details": portfolio_readiness.details,
                    },
                    "convexity_runtime_diagnostics": {} if getattr(orchestrator, "convexity_engine", None) is None else orchestrator.convexity_engine.last_diagnostics,
                    "deployment_readiness": deployment_readiness.as_dict(),
                    "trend_broker_live_status": {
                        "broker": trend_broker_readiness.broker_name,
                        "configured": settings.trend_live_broker_mode == "ibkr",
                        "readiness": trend_broker_readiness.is_ready,
                        "reasons": trend_broker_readiness.reasons,
                        "last_auth_ok_at": None if trend_broker_readiness.last_successful_auth_at is None else trend_broker_readiness.last_successful_auth_at.isoformat(),
                        "last_reconciliation_ok_at": None if trend_broker_readiness.last_reconciliation_check_at is None else trend_broker_readiness.last_reconciliation_check_at.isoformat(),
                        "last_order_reject_at": trend_broker_repo.latest_reject_timestamp(),
                        "reject_count": trend_broker_repo.count_rejects(),
                        "reconnect_count": trend_broker_repo.count_reconnect_events(),
                        "reconciliation_mismatch_count": trend_broker_repo.count_reconciliation_mismatches(),
                        "execution_mode": "live_equity" if trend_live_capable else "paper_equity",
                    },
                }
            )
            write_runtime_status(
                settings,
                "portfolio_orchestrator",
                "running",
                details={
                    "disk_headroom": disk_headroom,
                    "allocation_run_id": result.run.run_id,
                    "candidate_count": result.run.candidate_count,
                    "approved_count": result.run.approved_count,
                    "feedback_outcomes": 0 if feedback_result is None else len(feedback_result.outcomes),
                    "report_allocation_summary": report.get("allocation_summary", {}),
                    "convexity_runtime_diagnostics": report.get("convexity_runtime_diagnostics"),
                    "deployment_readiness": deployment_readiness.as_dict(),
                    "trend_broker_live_status": report.get("trend_broker_live_status"),
                },
                update_heartbeat=True,
            )
        except Exception as exc:
            logger.error("portfolio_cycle_error", extra={"event": {"error": str(exc)}})
            write_runtime_status(
                settings,
                "portfolio_orchestrator",
                "error",
                message=str(exc),
                details={"runtime": "portfolio_orchestrator"},
            )
            try:
                notifier.send("Portfolio Orchestrator crash", str(exc))
            except Exception:
                logger.error("notifier_error", extra={"event": {"error": "failed_to_send_crash_notification"}})
        time.sleep(settings.portfolio_cycle_interval_seconds)


def _average_option_iv(quotes: list[OptionContractQuote]) -> Decimal | None:
    ivs = [quote.iv for quote in quotes if quote.iv is not None]
    if not ivs:
        return None
    return sum(ivs, Decimal("0")) / Decimal(len(ivs))
