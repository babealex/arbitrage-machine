from __future__ import annotations

from decimal import Decimal

from app.core.money import to_decimal
from app.models import Signal
from app.options.scenario_engine import StructureScenarioSummary
from app.options.selection import StructureSelectionRecord
from app.strategies.trend.models import TrendTarget

from .models import TradeCandidate


def candidate_from_kalshi_signal(
    signal: Signal,
    *,
    cvar_95_loss: Decimal | str | int | float,
    uncertainty_penalty: Decimal | str | int | float,
    liquidity_penalty: Decimal | str | int | float,
    horizon_days: int,
    regime_stability: Decimal | str | int | float = Decimal("0.75"),
    correlation_bucket: str | None = None,
) -> TradeCandidate:
    notional_reference = Decimal(signal.quantity) * Decimal("100")
    return TradeCandidate(
        candidate_id=f"{signal.strategy.value}:{signal.ticker}:{signal.ts.isoformat()}",
        strategy_family=signal.strategy.value,
        source_kind="kalshi_signal",
        instrument_key=signal.ticker,
        sleeve="opportunistic",
        regime_name=signal.source,
        observed_at=signal.ts,
        expected_pnl_after_costs=notional_reference * signal.edge.edge_after_fees,
        cvar_95_loss=to_decimal(cvar_95_loss),
        uncertainty_penalty=to_decimal(uncertainty_penalty),
        liquidity_penalty=to_decimal(liquidity_penalty),
        horizon_days=horizon_days,
        regime_stability=to_decimal(regime_stability),
        notional_reference=notional_reference,
        max_loss_estimate=abs(to_decimal(cvar_95_loss)),
        correlation_bucket=correlation_bucket or signal.ticker,
        state_ok=True,
    )


def candidate_from_trend_target(
    target: TrendTarget,
    *,
    observed_at,
    portfolio_equity: Decimal | str | int | float,
    regime_name: str = "trend_balanced",
    liquidity_penalty: Decimal | str | int | float = Decimal("0.05"),
    uncertainty_penalty: Decimal | str | int | float = Decimal("0.25"),
    regime_stability: Decimal | str | int | float = Decimal("0.80"),
    horizon_days: int = 20,
) -> TradeCandidate:
    equity = to_decimal(portfolio_equity)
    notional_reference = abs(equity * target.target_weight)
    expected_pnl = notional_reference * target.momentum_return
    cvar = notional_reference * target.realized_volatility * Decimal("1.65")
    return TradeCandidate(
        candidate_id=f"trend:{target.symbol}:{observed_at.isoformat()}",
        strategy_family="trend",
        source_kind="trend_target",
        instrument_key=target.symbol,
        sleeve="base",
        regime_name=regime_name,
        observed_at=observed_at,
        expected_pnl_after_costs=expected_pnl,
        cvar_95_loss=abs(cvar),
        uncertainty_penalty=to_decimal(uncertainty_penalty),
        liquidity_penalty=to_decimal(liquidity_penalty),
        horizon_days=horizon_days,
        regime_stability=to_decimal(regime_stability),
        notional_reference=notional_reference,
        max_loss_estimate=abs(cvar),
        correlation_bucket=target.symbol,
        state_ok=True,
    )


def candidate_from_options_selection(
    selection: StructureSelectionRecord,
    summary: StructureScenarioSummary,
) -> TradeCandidate:
    expected = summary.expected_pnl
    cvar = abs(summary.cvar_95_loss)
    uncertainty = Decimal("0")
    if summary.model_uncertainty_penalty is not None and expected != 0:
        uncertainty = abs(summary.model_uncertainty_penalty / expected)
    liquidity_penalty = selection.exit_spread_fraction + (selection.slippage_bps / Decimal("10000"))
    notional_reference = max(summary.expected_entry_cost, Decimal("1"))
    return TradeCandidate(
        candidate_id=f"options:{selection.underlying}:{selection.observed_at.isoformat()}:{selection.selected_structure_name}",
        strategy_family="options",
        source_kind="options_selection",
        instrument_key=selection.selected_structure_name,
        sleeve="attack",
        regime_name=selection.regime_name,
        observed_at=selection.observed_at,
        expected_pnl_after_costs=expected,
        cvar_95_loss=cvar,
        uncertainty_penalty=uncertainty,
        liquidity_penalty=liquidity_penalty,
        horizon_days=selection.horizon_days,
        regime_stability=Decimal("0.60") if selection.regime_name == "event_term_structure" else Decimal("0.75"),
        notional_reference=notional_reference,
        max_loss_estimate=max(cvar, Decimal("1")),
        correlation_bucket=selection.underlying,
        state_ok=True,
    )
