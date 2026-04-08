from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import date
from decimal import Decimal
from typing import Callable

from app.allocation.adapters import candidate_from_trend_target
from app.allocation.coordinator import AllocationCoordinator, PendingAllocationAction
from app.allocation.feedback import AllocationFeedbackEngine
from app.allocation.outcomes import AllocationAdjustment
from app.allocation.models import PortfolioRiskState
from app.core.money import quantize_cents, to_decimal
from app.execution.models import ExecutionOutcome, SignalLegIntent
from app.execution.orders import build_order_intent
from app.execution.router import ExecutionRouter
from app.instruments.models import InstrumentRef
from app.market_data.snapshots import MarketStateInput
from app.models import Signal, SignalEdge, StrategyName
from app.persistence.kalshi_repo import KalshiRepository
from app.persistence.trend_repo import TrendRepository
from app.risk.manager import RiskManager
from app.strategies.trend.models import DailyBar, TrendEquitySnapshot, TrendPosition, TrendReadiness, TrendTarget
from app.strategies.trend.risk_targeting import build_target_weight_plan, realized_volatility
from app.strategies.trend.signal import momentum_return


@dataclass(frozen=True, slots=True)
class TrendRunResult:
    readiness: TrendReadiness
    signals: list[Signal]
    targets: list[TrendTarget]
    outcomes: list[ExecutionOutcome]
    snapshot: TrendEquitySnapshot


def assess_trend_readiness(
    *,
    bars_by_symbol: dict[str, list[DailyBar]],
    as_of: date,
    min_history: int,
    max_staleness_days: int,
) -> TrendReadiness:
    halt_reasons: list[str] = []
    details: dict[str, object] = {}
    for symbol, bars in bars_by_symbol.items():
        if len(bars) < min_history:
            halt_reasons.append(f"insufficient_bars:{symbol}")
            details[symbol] = {"bars": len(bars)}
            continue
        latest = bars[-1].bar_date
        if (as_of - latest).days > max_staleness_days:
            halt_reasons.append(f"stale_bars:{symbol}")
            details[symbol] = {"latest_bar_date": latest.isoformat()}
    return TrendReadiness(can_trade=not halt_reasons, halt_reasons=halt_reasons, details=details)


class TrendPaperEngine:
    def __init__(
        self,
        *,
        signal_repo: KalshiRepository,
        trend_repo: TrendRepository,
        risk: RiskManager,
        execution_router: ExecutionRouter,
        allocation_coordinator: AllocationCoordinator | None = None,
        allocation_adjustments_loader: Callable[[], list[AllocationAdjustment]] | None = None,
        allocation_feedback_engine: AllocationFeedbackEngine | None = None,
        universe: list[str],
        lookback_bars: int = 20,
        vol_lookback_bars: int = 20,
        target_volatility: Decimal | str | int | float = Decimal("0.10"),
        max_weight: Decimal | str | int | float = Decimal("0.35"),
        max_gross_exposure: Decimal | str | int | float = Decimal("1.0"),
        min_rebalance_fraction: Decimal | str | int | float = Decimal("0.10"),
        weight_mode: str = "current",
        slippage_bps: Decimal | str | int | float = Decimal("12"),
        starting_capital: Decimal | str | int | float = Decimal("100000"),
        max_drawdown: Decimal | str | int | float = Decimal("0.15"),
        max_bar_staleness_days: int = 3,
    ) -> None:
        self.signal_repo = signal_repo
        self.trend_repo = trend_repo
        self.risk = risk
        self.execution_router = execution_router
        self.allocation_coordinator = allocation_coordinator
        self.allocation_adjustments_loader = allocation_adjustments_loader
        self.allocation_feedback_engine = allocation_feedback_engine
        self.universe = universe
        self.lookback_bars = lookback_bars
        self.vol_lookback_bars = vol_lookback_bars
        self.target_volatility = to_decimal(target_volatility)
        self.max_weight = to_decimal(max_weight)
        self.max_gross_exposure = to_decimal(max_gross_exposure)
        self.min_rebalance_fraction = to_decimal(min_rebalance_fraction)
        self.weight_mode = str(weight_mode or "current").strip().lower()
        self.slippage_bps = to_decimal(slippage_bps)
        self.starting_capital = to_decimal(starting_capital)
        self.max_drawdown = to_decimal(max_drawdown)
        self.max_bar_staleness_days = max_bar_staleness_days
        self.live_capable = bool(getattr(self.execution_router.equity_broker, "is_live_capable", lambda: False)())
        self.last_diagnostics: dict[str, object] = {}

    def run(self, as_of: date) -> TrendRunResult:
        readiness, bars_by_symbol, targets = self.get_targets(as_of)
        existing_positions = {position.symbol: position for position in self.trend_repo.load_positions()}
        last_snapshot = self.trend_repo.load_equity_snapshots()
        cash = last_snapshot[-1].cash if last_snapshot else self.starting_capital
        equity_reference = last_snapshot[-1].equity if last_snapshot else self.starting_capital
        if not readiness.can_trade:
            for reason in readiness.halt_reasons:
                self.risk.kill(reason, readiness.details)
            diagnostics = self._build_run_diagnostics(
                readiness=readiness,
                targets=[],
                equity_reference=equity_reference,
                target_notional_by_symbol={},
                effective_exposure=Decimal("0"),
                no_trade_reasons=readiness.halt_reasons,
            )
            snapshot = self._snapshot(as_of, cash, existing_positions, trade_count=0, diagnostics=diagnostics)
            self.trend_repo.persist_equity_snapshot(snapshot)
            self.last_diagnostics = diagnostics
            return TrendRunResult(readiness, [], [], [], snapshot)

        signals: list[Signal] = []
        outcomes: list[ExecutionOutcome] = []
        trade_count = 0
        positions = dict(existing_positions)
        pending_allocations: list[PendingAllocationAction] = []
        target_notional_by_symbol: dict[str, str] = {}
        no_trade_reasons: list[str] = []
        for target in targets:
            latest_close = bars_by_symbol[target.symbol][-1].close
            desired_notional = equity_reference * target.target_weight
            target_notional_by_symbol[target.symbol] = format(desired_notional, "f")
            target_quantity = int(desired_notional / latest_close) if latest_close > 0 else 0
            current_quantity = positions.get(target.symbol, TrendPosition(target.symbol, 0, 0, 0, Decimal("0"))).quantity
            delta = target_quantity - current_quantity
            if delta == 0:
                no_trade_reasons.append(f"no_rebalance_needed:{target.symbol}")
                continue
            rebalance_fraction = Decimal(abs(delta)) / Decimal(max(1, abs(target_quantity), abs(current_quantity)))
            if rebalance_fraction < self.min_rebalance_fraction:
                no_trade_reasons.append(f"rebalance_below_threshold:{target.symbol}")
                continue
            side = "buy" if delta > 0 else "sell"
            signal = self._build_signal(target, quantity=abs(delta), side=side, close=latest_close, as_of=as_of)
            self.signal_repo.persist_signal(signal)
            signals.append(signal)
            intent = self._build_order_intent(signal, target)
            candidate = candidate_from_trend_target(target, observed_at=signal.ts, portfolio_equity=equity_reference)

            def _execute(decision, *, target=target, signal=signal, intent=intent, latest_close=latest_close, current_quantity=current_quantity, side=side):
                nonlocal cash, trade_count, positions, outcomes
                success, order_outcomes = self.execution_router.execute_order_intents(
                    [intent],
                    source_metadata={
                        "sleeve_name": "trend",
                        "signal": signal.strategy.value,
                        "target_weight": format(target.target_weight, "f"),
                        "allocation_run_id": decision.run_id,
                        "allocation_candidate_id": decision.candidate_id,
                    },
                    market_prices={target.symbol: latest_close},
                    market_state_inputs=[
                        MarketStateInput(
                            observed_at=signal.ts,
                            instrument=InstrumentRef(symbol=target.symbol, venue="paper_equity"),
                            source="trend_daily_bar",
                            reference_kind="close",
                            reference_price=latest_close,
                            provenance="trend_engine",
                            order_id=intent.order_id,
                        )
                    ],
                )
                if not success:
                    return False
                outcome = order_outcomes[0]
                outcomes.extend(order_outcomes)
                cash = self._apply_fill_cash(cash, outcome)
                filled = outcome.filled_quantity if side == "buy" else -outcome.filled_quantity
                new_quantity = current_quantity + filled
                if new_quantity == 0:
                    positions.pop(target.symbol, None)
                else:
                    positions[target.symbol] = TrendPosition(
                        symbol=target.symbol,
                        quantity=new_quantity,
                        average_price_cents=outcome.price_cents or int((quantize_cents(latest_close) * Decimal("100")).to_integral_value()),
                        market_price_cents=int((quantize_cents(latest_close) * Decimal("100")).to_integral_value()),
                        market_value=to_decimal(new_quantity) * latest_close,
                    )
                trade_count += 1
                return True

            pending_allocations.append(PendingAllocationAction(candidate=candidate, execute=_execute))

        if self.allocation_coordinator is not None and pending_allocations:
            risk_state = PortfolioRiskState(
                observed_at=signals[0].ts if signals else self.trend_repo.load_equity_snapshots()[-1].created_at,
                portfolio_equity=equity_reference,
                daily_realized_pnl=last_snapshot[-1].equity - last_snapshot[-2].equity if len(last_snapshot) >= 2 else Decimal("0"),
                current_gross_notional=sum((abs(position.market_value) for position in positions.values()), Decimal("0")),
                gross_notional_cap=equity_reference * self.max_gross_exposure,
                allocated_notional_by_sleeve={},
                allocated_notional_by_bucket={},
            )
            adjustments = self.allocation_adjustments_loader() if self.allocation_adjustments_loader is not None else []
            self.allocation_coordinator.run(
                pending=pending_allocations,
                risk_state=risk_state,
                adjustments=adjustments,
            )
            if self.allocation_feedback_engine is not None:
                self.allocation_feedback_engine.refresh_latest_run()
        else:
            for item in pending_allocations:
                item.execute(
                    type("Decision", (), {"run_id": "alloc:disabled", "candidate_id": item.candidate.candidate_id})
                )

        effective_exposure = sum((abs(position.market_value) for position in positions.values()), Decimal("0"))
        diagnostics = self._build_run_diagnostics(
            readiness=readiness,
            targets=targets,
            equity_reference=equity_reference,
            target_notional_by_symbol=target_notional_by_symbol,
            effective_exposure=effective_exposure,
            no_trade_reasons=no_trade_reasons,
        )
        preview_snapshot = self._snapshot(as_of, cash, positions, trade_count, diagnostics=diagnostics)
        snapshot = preview_snapshot
        if snapshot.drawdown >= self.max_drawdown:
            self.risk.kill(
                "trend_drawdown_limit",
                {"drawdown": format(snapshot.drawdown, "f"), "max_drawdown": format(self.max_drawdown, "f")},
            )
            diagnostics["kill_switch_state"] = {
                "active": True,
                "reasons": list(self.risk.state.reasons),
            }
            snapshot = self._snapshot(as_of, cash, positions, trade_count, diagnostics=diagnostics)
        self.trend_repo.replace_positions(list(positions.values()))
        self.trend_repo.persist_equity_snapshot(snapshot)
        self.last_diagnostics = diagnostics
        return TrendRunResult(readiness, signals, targets, outcomes, snapshot)

    def get_targets(self, as_of: date) -> tuple[TrendReadiness, dict[str, list[DailyBar]], list[TrendTarget]]:
        bars_by_symbol = {
            symbol: self.trend_repo.load_recent_bars(
                symbol,
                max(self.lookback_bars, self.vol_lookback_bars) + 1,
            )
            for symbol in self.universe
        }
        readiness = assess_trend_readiness(
            bars_by_symbol=bars_by_symbol,
            as_of=as_of,
            min_history=max(self.lookback_bars, self.vol_lookback_bars) + 1,
            max_staleness_days=self.max_bar_staleness_days,
        )
        if self.risk.state.killed and "risk_killed" not in readiness.halt_reasons:
            readiness = replace(
                readiness,
                can_trade=False,
                halt_reasons=[*readiness.halt_reasons, "risk_killed"],
                details={**readiness.details, "risk_reasons": list(self.risk.state.reasons)},
            )
        if not readiness.can_trade:
            return readiness, bars_by_symbol, []
        return readiness, bars_by_symbol, self._build_targets(bars_by_symbol)

    def _build_targets(self, bars_by_symbol: dict[str, list[DailyBar]]) -> list[TrendTarget]:
        symbol_rows: list[dict[str, Decimal | str]] = []
        for symbol, bars in bars_by_symbol.items():
            momentum = momentum_return(bars, self.lookback_bars)
            realized_vol = realized_volatility(bars, self.vol_lookback_bars)
            if momentum is None or realized_vol is None:
                continue
            symbol_rows.append(
                {
                    "symbol": symbol,
                    "momentum": momentum,
                    "realized_vol": realized_vol,
                }
            )
        plan = build_target_weight_plan(
            symbol_rows=symbol_rows,
            target_vol=self.target_volatility,
            max_weight=self.max_weight,
            max_gross_exposure=self.max_gross_exposure,
            weight_mode=self.weight_mode,
        )
        final_weights = plan["final_weights"]
        return [
            TrendTarget(
                symbol=str(row["symbol"]),
                momentum_return=to_decimal(row["momentum"]),
                realized_volatility=to_decimal(row["realized_vol"]),
                target_weight=to_decimal(final_weights.get(str(row["symbol"]), Decimal("0"))),
                raw_weight=to_decimal(plan["raw_weights"].get(str(row["symbol"]), Decimal("0"))),
                scaling_factor=to_decimal(plan["scaling_factor"]),
                cap_binding=self._cap_binding(
                    symbol=str(row["symbol"]),
                    momentum=to_decimal(row["momentum"]),
                    raw_weight=to_decimal(plan["raw_weights"].get(str(row["symbol"]), Decimal("0"))),
                    target_weight=to_decimal(final_weights.get(str(row["symbol"]), Decimal("0"))),
                    capped_symbols=set(plan["capped_symbols"]),
                    scaling_factor=to_decimal(plan["scaling_factor"]),
                ),
            )
            for row in symbol_rows
        ]

    def _build_signal(self, target: TrendTarget, *, quantity: int, side: str, close: Decimal, as_of: date) -> Signal:
        fee_buffer = self.slippage_bps / Decimal("10000")
        edge_after_costs = target.momentum_return - fee_buffer
        confidence = float(min(Decimal("1"), abs(target.momentum_return) / max(target.realized_volatility, Decimal("0.0001"))))
        return Signal(
            strategy=StrategyName.MOMENTUM,
            ticker=target.symbol,
            action=f"rebalance_{side}",
            quantity=quantity,
            edge=SignalEdge.from_values(
                edge_before_fees=target.momentum_return,
                edge_after_fees=edge_after_costs,
                fee_estimate=fee_buffer,
                execution_buffer=fee_buffer,
            ),
            source="daily_trend",
            confidence=confidence,
            priority=1,
            legs=[
                SignalLegIntent(
                    instrument=InstrumentRef(symbol=target.symbol, venue=self._trend_venue()),
                    side=side,
                    order_type="market",
                    limit_price=None,
                    time_in_force="DAY",
                )
            ],
        )

    def _build_order_intent(self, signal: Signal, target: TrendTarget):
        return build_order_intent(
            instrument=InstrumentRef(symbol=signal.ticker, venue=self._trend_venue()),
            side=signal.legs[0].side,
            quantity=signal.quantity,
            order_type="market",
            strategy_name=signal.strategy.value,
            signal_ref=f"{signal.strategy.value}:{signal.ticker}:{signal.ts.isoformat()}",
            seed=f"{signal.ticker}:{signal.action}:{signal.ts.isoformat()}",
            limit_price=None,
            time_in_force="DAY",
            created_at=signal.ts,
        )

    def _trend_venue(self) -> str:
        if self.execution_router.live_trading and self.live_capable:
            return "live_equity"
        return "paper_equity"

    @staticmethod
    def _apply_fill_cash(cash: Decimal, outcome: ExecutionOutcome) -> Decimal:
        if outcome.price_cents is None:
            return cash
        fill_value = Decimal(outcome.filled_quantity) * (Decimal(outcome.price_cents) / Decimal("100"))
        if outcome.side == "buy":
            return cash - fill_value
        return cash + fill_value

    def _snapshot(
        self,
        snapshot_date: date,
        cash: Decimal,
        positions: dict[str, TrendPosition],
        trade_count: int,
        diagnostics: dict[str, object],
    ) -> TrendEquitySnapshot:
        gross_exposure = sum((abs(position.market_value) for position in positions.values()), Decimal("0"))
        net_exposure = sum((position.market_value for position in positions.values()), Decimal("0"))
        equity = cash + net_exposure
        previous_snapshots = self.trend_repo.load_equity_snapshots()
        peak_equity = max([snapshot.equity for snapshot in previous_snapshots] + [equity])
        drawdown = Decimal("0") if peak_equity <= 0 else (peak_equity - equity) / peak_equity
        return TrendEquitySnapshot(
            snapshot_date=snapshot_date,
            equity=equity,
            cash=cash,
            gross_exposure=gross_exposure,
            net_exposure=net_exposure,
            drawdown=drawdown,
            trade_count=trade_count,
            positions={symbol: position.quantity for symbol, position in positions.items()},
            diagnostics=diagnostics,
            kill_switch_active=self.risk.state.killed,
        )

    def _cap_binding(
        self,
        *,
        symbol: str,
        momentum: Decimal,
        raw_weight: Decimal,
        target_weight: Decimal,
        capped_symbols: set[str],
        scaling_factor: Decimal,
    ) -> str:
        if momentum <= 0:
            return "negative_or_zero_signal"
        if raw_weight <= 0:
            return "zero_weight"
        if symbol in capped_symbols:
            return "per_asset_cap"
        if scaling_factor < Decimal("1") and target_weight < raw_weight:
            return "gross_exposure_cap"
        return "none"

    def _build_run_diagnostics(
        self,
        *,
        readiness: TrendReadiness,
        targets: list[TrendTarget],
        equity_reference: Decimal,
        target_notional_by_symbol: dict[str, str],
        effective_exposure: Decimal,
        no_trade_reasons: list[str],
    ) -> dict[str, object]:
        return {
            "readiness": {
                "can_trade": readiness.can_trade,
                "halt_reasons": list(readiness.halt_reasons),
                "details": dict(readiness.details),
            },
            "equity_reference": format(equity_reference, "f"),
            "target_volatility": format(self.target_volatility, "f"),
            "max_weight": format(self.max_weight, "f"),
            "max_gross_exposure": format(self.max_gross_exposure, "f"),
            "effective_exposure": format(effective_exposure, "f"),
            "kill_switch_state": {
                "active": self.risk.state.killed,
                "reasons": list(self.risk.state.reasons),
            },
            "no_trade_reasons": list(no_trade_reasons),
            "targets": [
                {
                    "symbol": target.symbol,
                    "raw_signal": format(target.momentum_return, "f"),
                    "realized_vol": format(target.realized_volatility, "f"),
                    "raw_weight": format(target.raw_weight, "f"),
                    "target_weight": format(target.target_weight, "f"),
                    "scaling_factor": format(target.scaling_factor, "f"),
                    "cap_binding": target.cap_binding,
                    "target_notional": target_notional_by_symbol.get(target.symbol, "0"),
                }
                for target in targets
            ],
        }
