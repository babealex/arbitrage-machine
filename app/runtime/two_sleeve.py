from __future__ import annotations

import argparse
import json
import time
from collections import defaultdict
from dataclasses import replace
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal

from app.alerts.notifier import Notifier
from app.common.runtime_health import assert_runtime_headroom, run_startup_checks, write_runtime_status
from app.config import Settings
from app.core.money import to_decimal
from app.db import Database
from app.execution.router import ExecutionRouter
from app.external_data.fed_futures import ExternalDataResult, FedFuturesProvider
from app.external_data.probability_mapper import map_market_probability
from app.kalshi.rest_client import KalshiRestClient
from app.logging_setup import setup_logging
from app.market_data.service import MarketDataService
from app.market_data.snapshots import market_state_inputs_from_kalshi_market
from app.models import PortfolioSnapshot
from app.persistence.kalshi_repo import KalshiRepository
from app.persistence.market_state_repo import MarketStateRepository
from app.persistence.sleeve_repo import SleeveRepository
from app.persistence.trend_repo import TrendRepository
from app.portfolio.state import PortfolioState
from app.reporting_execution import build_execution_report
from app.replay.providers import YahooHistoricalBarProvider
from app.replay_execution import ExecutionReplayEngine
from app.risk.manager import RiskManager
from app.runtime.dashboard import start_control_panel
from app.runtime.health import assess_layer2_trading_readiness
from app.sleeves import SLEEVE_REGISTRY, enabled_sleeves, sleeve_name_for_strategy
from app.strategy_doctrine import doctrine_summary
from app.strategies.cross_market import CrossMarketStrategy
from app.strategies.monotonicity import MonotonicityStrategy
from app.strategies.partition import PartitionStrategy
from app.strategies.trend.models import DailyBar
from app.strategies.trend.portfolio import TrendPaperEngine
from app.strategies.trend.risk_targeting import build_target_weight_plan, realized_volatility, target_weight_from_signal
from app.strategies.trend.signal import momentum_return


def _sleeve_capital(settings: Settings) -> dict[str, Decimal]:
    total = Decimal(str(settings.total_paper_capital))
    weights = settings.two_sleeve_capital_weights
    return {
        "trend": total * Decimal(str(weights["trend"])),
        "kalshi_event": total * Decimal(str(weights["kalshi_event"])),
    }


def _mean(values: list[Decimal]) -> Decimal | None:
    if not values:
        return None
    return sum(values, Decimal("0")) / Decimal(len(values))


def _median(values: list[Decimal]) -> Decimal | None:
    if not values:
        return None
    ordered = sorted(values)
    midpoint = len(ordered) // 2
    if len(ordered) % 2:
        return ordered[midpoint]
    return (ordered[midpoint - 1] + ordered[midpoint]) / Decimal("2")


def _bump_reason(reason_counts: dict[str, int], reason: str, count: int = 1) -> None:
    normalized = str(reason or "").strip()
    if not normalized:
        return
    reason_counts[normalized] = int(reason_counts.get(normalized, 0)) + count


def _normalize_no_trade_reason(reason: str, sleeve_name: str) -> str:
    normalized = str(reason or "").strip().lower()
    if sleeve_name == "trend":
        if normalized.startswith("stale_bars"):
            return "stale_data"
        if normalized.startswith("insufficient_bars"):
            return "insufficient_history"
        if "rebalance" in normalized:
            return "no_rebalance_needed"
        if "risk" in normalized:
            return "risk_gate"
        if "missing_market_price" in normalized or "connection" in normalized or "timeout" in normalized:
            return "broker_or_data_failure"
        return normalized or "disabled"
    if sleeve_name == "kalshi_event":
        if normalized.startswith("insufficient_signal_time_depth") or "depth" in normalized:
            return "insufficient_depth"
        if "edge" in normalized:
            return "edge_below_threshold"
        if "coverage" in normalized or "missing" in normalized or "partition" in normalized:
            return "no_valid_ladders"
        if "stale" in normalized or "data" in normalized or "auth" in normalized:
            return "data_failure"
        return normalized or "disabled"
    return normalized or "unknown"


def _overall_run_mode(sleeves: list[dict[str, object]]) -> str:
    modes = {str(item.get("run_mode", "data_unavailable")) for item in sleeves}
    if "live_paper" in modes:
        return "live_paper"
    if "replay_paper" in modes:
        return "replay_paper"
    return "data_unavailable"


def _top_reason_pairs(reason_counts: dict[str, int], limit: int = 3) -> list[dict[str, object]]:
    ordered = sorted(((reason, count) for reason, count in reason_counts.items() if count > 0), key=lambda item: (-item[1], item[0]))
    return [{"reason": reason, "count": count} for reason, count in ordered[:limit]]


def _kalshi_family_from_ticker(ticker: str) -> str:
    normalized = str(ticker or "").strip()
    if not normalized:
        return "unknown"
    return normalized.split("-", 1)[0]


def _decimal_l2(values: list[Decimal]) -> Decimal:
    if not values:
        return Decimal("0")
    return sum((value * value for value in values), Decimal("0")).sqrt()


def _serialize_decimal_map(values: dict[str, Decimal]) -> dict[str, str]:
    return {key: format(value, "f") for key, value in sorted(values.items())}


def _equity_from_metric(metric: dict[str, object]) -> Decimal | None:
    capital_assigned = metric.get("capital_assigned_decimal")
    realized = metric.get("realized_pnl_decimal")
    unrealized = metric.get("unrealized_pnl_decimal")
    if capital_assigned is None:
        return None
    return to_decimal(capital_assigned) + to_decimal(realized or "0") + to_decimal(unrealized or "0")


def _compound_returns(interval_returns: list[Decimal]) -> Decimal | None:
    if not interval_returns:
        return None
    compounded = Decimal("1")
    for interval_return in interval_returns:
        compounded *= Decimal("1") + interval_return
    return compounded - Decimal("1")


def _weight_return(
    *,
    previous_closes: dict[str, Decimal],
    current_closes: dict[str, Decimal],
    weights: dict[str, Decimal],
) -> Decimal | None:
    if not weights:
        return None
    available = []
    for symbol, weight in weights.items():
        previous_close = previous_closes.get(symbol)
        current_close = current_closes.get(symbol)
        if previous_close is None or current_close is None or previous_close <= 0:
            continue
        available.append((symbol, weight, previous_close, current_close))
    if not available:
        return None
    total_weight = sum((weight for _, weight, _, _ in available), Decimal("0"))
    if total_weight <= 0:
        return None
    interval_return = Decimal("0")
    for _, weight, previous_close, current_close in available:
        normalized_weight = weight / total_weight
        interval_return += normalized_weight * ((current_close / previous_close) - Decimal("1"))
    return interval_return


def _build_trend_benchmark_summary(metrics: list[dict]) -> dict[str, object] | None:
    if len(metrics) < 2:
        return None
    strategy_interval_returns: list[Decimal] = []
    spy_qqq_interval_returns: list[Decimal] = []
    universe_interval_returns: list[Decimal] = []
    intervals_outperforming_spy_qqq = 0
    intervals_outperforming_universe = 0
    benchmark_intervals = 0
    universe_intervals = 0
    for previous_metric, current_metric in zip(metrics, metrics[1:]):
        previous_details = json.loads(previous_metric["details"]) if previous_metric.get("details") else {}
        current_details = json.loads(current_metric["details"]) if current_metric.get("details") else {}
        previous_cycle = previous_details.get("trend_cycle_diagnostics") or {}
        current_cycle = current_details.get("trend_cycle_diagnostics") or {}
        previous_closes_raw = previous_cycle.get("latest_closes") or {}
        current_closes_raw = current_cycle.get("latest_closes") or {}
        previous_closes = {str(symbol): to_decimal(value) for symbol, value in previous_closes_raw.items()}
        current_closes = {str(symbol): to_decimal(value) for symbol, value in current_closes_raw.items()}
        previous_equity = _equity_from_metric(previous_metric)
        current_equity = _equity_from_metric(current_metric)
        if previous_equity is None or current_equity is None or previous_equity <= 0:
            continue
        strategy_return = (current_equity / previous_equity) - Decimal("1")
        strategy_interval_returns.append(strategy_return)
        spy_qqq_return = _weight_return(
            previous_closes=previous_closes,
            current_closes=current_closes,
            weights={"SPY": Decimal("0.5"), "QQQ": Decimal("0.5")},
        )
        if spy_qqq_return is not None:
            benchmark_intervals += 1
            spy_qqq_interval_returns.append(spy_qqq_return)
            if strategy_return > spy_qqq_return:
                intervals_outperforming_spy_qqq += 1
        common_symbols = sorted(set(previous_closes) & set(current_closes))
        if common_symbols:
            equal_weight = Decimal("1") / Decimal(len(common_symbols))
            universe_return = _weight_return(
                previous_closes=previous_closes,
                current_closes=current_closes,
                weights={symbol: equal_weight for symbol in common_symbols},
            )
            if universe_return is not None:
                universe_intervals += 1
                universe_interval_returns.append(universe_return)
                if strategy_return > universe_return:
                    intervals_outperforming_universe += 1
    if not strategy_interval_returns:
        return {
            "interpretation": "insufficient_data",
            "interpretation_reason": "trend_metrics_do_not_have_enough_consecutive_equity_snapshots",
            "intervals_compared": 0,
            "spy_qqq_equal_weight": None,
            "trend_universe_equal_weight": None,
        }
    strategy_cumulative = _compound_returns(strategy_interval_returns)
    spy_qqq_cumulative = _compound_returns(spy_qqq_interval_returns)
    universe_cumulative = _compound_returns(universe_interval_returns)
    neutral_band = Decimal("0.0005")
    interpretation = "insufficient_data"
    interpretation_reason = "benchmark_closes_missing_for_passive_comparison"
    if spy_qqq_cumulative is not None and strategy_cumulative is not None:
        excess = strategy_cumulative - spy_qqq_cumulative
        if abs(excess) <= neutral_band:
            interpretation = "matching_simple_spy_qqq_in_sample"
            interpretation_reason = "trend_equity_and_equal_weight_spy_qqq_are_effectively_flat_over_available_intervals"
        elif excess > Decimal("0"):
            interpretation = "outperforming_simple_spy_qqq_in_sample"
            interpretation_reason = "trend_equity_compounded_above_equal_weight_spy_qqq_over_available_intervals"
        else:
            interpretation = "lagging_simple_spy_qqq_in_sample"
            interpretation_reason = "trend_equity_compounded_below_equal_weight_spy_qqq_over_available_intervals"
    elif universe_cumulative is not None and strategy_cumulative is not None:
        excess = strategy_cumulative - universe_cumulative
        if abs(excess) <= neutral_band:
            interpretation = "matching_equal_weight_universe_in_sample"
            interpretation_reason = "trend_equity_and_equal_weight_trend_universe_are_effectively_flat_over_available_intervals"
        elif excess > Decimal("0"):
            interpretation = "outperforming_equal_weight_universe_in_sample"
            interpretation_reason = "trend_equity_compounded_above_equal_weight_trend_universe_over_available_intervals"
        else:
            interpretation = "lagging_equal_weight_universe_in_sample"
            interpretation_reason = "trend_equity_compounded_below_equal_weight_trend_universe_over_available_intervals"
    return {
        "intervals_compared": len(strategy_interval_returns),
        "strategy_cumulative_return": None if strategy_cumulative is None else format(strategy_cumulative, "f"),
        "strategy_avg_interval_return": format(_mean(strategy_interval_returns), "f"),
        "spy_qqq_equal_weight": None
        if spy_qqq_cumulative is None
        else {
            "intervals_compared": benchmark_intervals,
            "cumulative_return": format(spy_qqq_cumulative, "f"),
            "avg_interval_return": format(_mean(spy_qqq_interval_returns), "f"),
            "strategy_excess_return": format(strategy_cumulative - spy_qqq_cumulative, "f"),
            "interval_outperformance_rate": format(
                Decimal(intervals_outperforming_spy_qqq) / Decimal(max(benchmark_intervals, 1)),
                "f",
            ),
        },
        "trend_universe_equal_weight": None
        if universe_cumulative is None
        else {
            "intervals_compared": universe_intervals,
            "cumulative_return": format(universe_cumulative, "f"),
            "avg_interval_return": format(_mean(universe_interval_returns), "f"),
            "strategy_excess_return": format(strategy_cumulative - universe_cumulative, "f"),
            "interval_outperformance_rate": format(
                Decimal(intervals_outperforming_universe) / Decimal(max(universe_intervals, 1)),
                "f",
            ),
        },
        "interpretation": interpretation,
        "interpretation_reason": interpretation_reason,
    }


def _load_latest_trend_closes(repo: TrendRepository, symbols: set[str]) -> tuple[dict[str, Decimal], dict[str, str]]:
    latest_closes: dict[str, Decimal] = {}
    latest_dates: dict[str, str] = {}
    for symbol in sorted(symbols):
        bars = repo.load_recent_bars(symbol, 1)
        if not bars:
            continue
        latest_closes[symbol] = bars[-1].close
        latest_dates[symbol] = bars[-1].bar_date.isoformat()
    return latest_closes, latest_dates


def _build_trend_cycle_diagnostics(
    *,
    trend_repo: TrendRepository,
    trend_result,
    trend_engine: TrendPaperEngine,
    min_rebalance_fraction: Decimal,
) -> dict[str, object]:
    symbols = {target.symbol for target in trend_result.targets} | set(trend_result.snapshot.positions.keys())
    latest_closes, latest_dates = _load_latest_trend_closes(trend_repo, symbols)
    equity = max(trend_result.snapshot.equity, Decimal("1"))
    target_weights = {target.symbol: target.target_weight for target in trend_result.targets}
    current_weights: dict[str, Decimal] = {}
    for symbol, quantity in trend_result.snapshot.positions.items():
        close = latest_closes.get(symbol)
        if close is None:
            continue
        current_weights[symbol] = (Decimal(quantity) * close) / equity
    union_symbols = set(target_weights) | set(current_weights)
    weight_deltas = {symbol: target_weights.get(symbol, Decimal("0")) - current_weights.get(symbol, Decimal("0")) for symbol in union_symbols}
    distance_l1 = sum((abs(delta) for delta in weight_deltas.values()), Decimal("0"))
    distance_l2 = _decimal_l2([abs(delta) for delta in weight_deltas.values()])
    threshold_suppressed = Decimal("0")
    max_suppressed_rebalance_fraction = Decimal("0")
    suppressed_distances: list[Decimal] = []
    for symbol, delta in weight_deltas.items():
        target_weight = abs(target_weights.get(symbol, Decimal("0")))
        current_weight = abs(current_weights.get(symbol, Decimal("0")))
        scale = max(target_weight, current_weight, Decimal("0.000001"))
        rebalance_fraction = abs(delta) / scale
        if abs(delta) > 0 and rebalance_fraction < min_rebalance_fraction:
            threshold_suppressed += abs(delta)
            suppressed_distances.append(min_rebalance_fraction - rebalance_fraction)
            if rebalance_fraction > max_suppressed_rebalance_fraction:
                max_suppressed_rebalance_fraction = rebalance_fraction
    momentum_values = [target.momentum_return for target in trend_result.targets]
    realized_vol_values = [target.realized_volatility for target in trend_result.targets]
    mean_momentum = _mean(momentum_values)
    cross_sectional_dispersion = None
    if mean_momentum is not None and len(momentum_values) > 1:
        cross_sectional_dispersion = _mean([abs(value - mean_momentum) for value in momentum_values])
    construction_rows: list[dict[str, object]] = []
    symbol_rows: list[dict[str, Decimal | str]] = []
    momentum_by_symbol: dict[str, Decimal | None] = {}
    vol_by_symbol: dict[str, Decimal | None] = {}
    for symbol in trend_engine.universe:
        bars = trend_repo.load_recent_bars(symbol, max(trend_engine.lookback_bars, trend_engine.vol_lookback_bars) + 1)
        momentum = momentum_return(bars, trend_engine.lookback_bars)
        realized_vol = realized_volatility(bars, trend_engine.vol_lookback_bars)
        if momentum is not None and realized_vol is not None:
            symbol_rows.append({"symbol": symbol, "momentum": momentum, "realized_vol": realized_vol})
        momentum_by_symbol[symbol] = momentum
        vol_by_symbol[symbol] = realized_vol
    plan = build_target_weight_plan(
        symbol_rows=symbol_rows,
        target_vol=trend_engine.target_volatility,
        max_weight=trend_engine.max_weight,
        max_gross_exposure=trend_engine.max_gross_exposure,
        weight_mode=trend_engine.weight_mode,
    )
    raw_weights = plan["raw_weights"]
    positive_symbols = sorted(symbol for symbol, value in momentum_by_symbol.items() if value is not None and value > 0)
    capped_symbols = list(plan["capped_symbols"])
    gross_raw_target_weight = to_decimal(plan["gross_raw_weight"])
    scaling_factor = to_decimal(plan["scaling_factor"])
    gross_scaled_target_weight = to_decimal(plan["gross_final_weight"])
    for symbol in trend_engine.universe:
        momentum = momentum_by_symbol.get(symbol)
        realized_vol = vol_by_symbol.get(symbol)
        raw_weight = to_decimal(raw_weights.get(symbol, Decimal("0")))
        capped = symbol in capped_symbols
        construction_rows.append(
            {
                "symbol": symbol,
                "momentum_return": None if momentum is None else format(momentum, "f"),
                "realized_volatility": None if realized_vol is None else format(realized_vol, "f"),
                "raw_target_weight_before_scale": format(raw_weight, "f"),
                "final_target_weight": format(target_weights.get(symbol, Decimal("0")), "f"),
                "capped_at_max_weight": capped,
            }
        )
    top_weight_share = Decimal("0")
    effective_symbol_count = Decimal("0")
    if gross_scaled_target_weight > 0:
        squared_sum = sum((weight * weight for weight in target_weights.values()), Decimal("0"))
        top_weight_share = max(target_weights.values())
        if squared_sum > 0:
            effective_symbol_count = (gross_scaled_target_weight * gross_scaled_target_weight) / squared_sum
    top_target_symbol = None
    if target_weights:
        top_target_symbol = max(target_weights.items(), key=lambda item: (abs(item[1]), item[0]))[0]
    # These are weight-based approximations derived from the persisted target/current state.
    # They are intended for attribution, not precise execution sizing.
    return {
        "target_symbols": sorted(target_weights),
        "target_weights": _serialize_decimal_map(target_weights),
        "current_weights": _serialize_decimal_map(current_weights),
        "top_target_symbol": top_target_symbol,
        "latest_closes": _serialize_decimal_map(latest_closes),
        "latest_bar_dates": latest_dates,
        "portfolio_distance_to_target_l1": format(distance_l1, "f"),
        "portfolio_distance_to_target_l2": format(distance_l2, "f"),
        "unconstrained_rebalance_notional_ratio": format(distance_l1, "f"),
        "threshold_suppressed_notional_ratio": format(threshold_suppressed, "f"),
        "constraint_suppressed_notional_ratio": "0",
        "near_target": distance_l1 <= min_rebalance_fraction,
        "near_miss_rebalance": bool(
            threshold_suppressed > 0 and max_suppressed_rebalance_fraction >= (min_rebalance_fraction * Decimal("0.8"))
        ),
        "avg_signal_distance_from_trigger": None if not suppressed_distances else format(_mean(suppressed_distances), "f"),
        "cross_sectional_dispersion": None if cross_sectional_dispersion is None else format(cross_sectional_dispersion, "f"),
        "avg_realized_volatility_proxy": None if not realized_vol_values else format(_mean(realized_vol_values), "f"),
        "construction": {
            "rows": construction_rows,
            "positive_momentum_symbols": positive_symbols,
            "positive_momentum_count": len(positive_symbols),
            "capped_target_symbols": capped_symbols,
            "capped_target_count": len(capped_symbols),
            "gross_raw_target_weight": format(gross_raw_target_weight, "f"),
            "gross_scaled_target_weight": format(gross_scaled_target_weight, "f"),
            "scaling_factor": format(scaling_factor, "f"),
            "top_weight_share": format(top_weight_share, "f"),
            "effective_symbol_count": format(effective_symbol_count, "f"),
        },
    }


def _build_trend_sparsity_summary(metrics: list[dict]) -> dict[str, object] | None:
    if len(metrics) < 2:
        return {
            "interpretation": "insufficient_data",
            "interpretation_reason": "need_at_least_two_cycles_for_sparsity_attribution",
        }
    target_sets: list[set[str]] = []
    target_weight_maps: list[dict[str, Decimal]] = []
    top_symbols: list[str | None] = []
    latest_bar_signatures: list[tuple[tuple[str, str], ...]] = []
    distance_l1_values: list[Decimal] = []
    distance_l2_values: list[Decimal] = []
    unconstrained_values: list[Decimal] = []
    threshold_values: list[Decimal] = []
    constraint_values: list[Decimal] = []
    cross_dispersion_values: list[Decimal] = []
    vol_values: list[Decimal] = []
    signal_trigger_distances: list[Decimal] = []
    near_target_cycles = 0
    threshold_suppression_cycles = 0
    constraint_suppression_cycles = 0
    near_miss_cycles = 0
    cycles_low_dispersion = 0
    for metric in metrics:
        details = json.loads(metric["details"]) if metric.get("details") else {}
        cycle = details.get("trend_cycle_diagnostics") or {}
        target_weights = {str(symbol): to_decimal(weight) for symbol, weight in (cycle.get("target_weights") or {}).items()}
        target_weight_maps.append(target_weights)
        target_sets.append(set(target_weights))
        top_symbols.append(cycle.get("top_target_symbol"))
        latest_dates = cycle.get("latest_bar_dates") or {}
        latest_bar_signatures.append(tuple(sorted((str(symbol), str(value)) for symbol, value in latest_dates.items())))
        if cycle.get("portfolio_distance_to_target_l1") is not None:
            distance_l1_values.append(to_decimal(cycle["portfolio_distance_to_target_l1"]))
        if cycle.get("portfolio_distance_to_target_l2") is not None:
            distance_l2_values.append(to_decimal(cycle["portfolio_distance_to_target_l2"]))
        if cycle.get("unconstrained_rebalance_notional_ratio") is not None:
            unconstrained_values.append(to_decimal(cycle["unconstrained_rebalance_notional_ratio"]))
        threshold_ratio = to_decimal(cycle.get("threshold_suppressed_notional_ratio") or "0")
        threshold_values.append(threshold_ratio)
        if threshold_ratio > 0:
            threshold_suppression_cycles += 1
        constraint_ratio = to_decimal(cycle.get("constraint_suppressed_notional_ratio") or "0")
        constraint_values.append(constraint_ratio)
        reasons = json.loads(metric["no_trade_reasons"]) if metric.get("no_trade_reasons") else {}
        if constraint_ratio > 0 or any(reason in {"risk_gate", "broker_or_data_failure", "stale_data"} for reason in reasons):
            constraint_suppression_cycles += 1
        if cycle.get("near_target"):
            near_target_cycles += 1
        if cycle.get("near_miss_rebalance"):
            near_miss_cycles += 1
        if cycle.get("cross_sectional_dispersion") is not None:
            dispersion = to_decimal(cycle["cross_sectional_dispersion"])
            cross_dispersion_values.append(dispersion)
            if dispersion <= Decimal("0.01"):
                cycles_low_dispersion += 1
        if cycle.get("avg_realized_volatility_proxy") is not None:
            vol_values.append(to_decimal(cycle["avg_realized_volatility_proxy"]))
        if cycle.get("avg_signal_distance_from_trigger") is not None:
            signal_trigger_distances.append(to_decimal(cycle["avg_signal_distance_from_trigger"]))
    comparisons = len(metrics) - 1
    target_change_l1_values: list[Decimal] = []
    target_change_l2_values: list[Decimal] = []
    target_turnover_values: list[Decimal] = []
    stable_core_values: list[Decimal] = []
    identical_target_sets = 0
    dominant_symbol_same_count = 0
    identical_bar_dates = 0
    for index in range(1, len(metrics)):
        prev_weights = target_weight_maps[index - 1]
        curr_weights = target_weight_maps[index]
        union_symbols = set(prev_weights) | set(curr_weights)
        diffs = [abs(curr_weights.get(symbol, Decimal("0")) - prev_weights.get(symbol, Decimal("0"))) for symbol in union_symbols]
        target_change_l1_values.append(sum(diffs, Decimal("0")))
        target_change_l2_values.append(_decimal_l2(diffs))
        prev_set = target_sets[index - 1]
        curr_set = target_sets[index]
        union_count = len(prev_set | curr_set)
        intersection_count = len(prev_set & curr_set)
        target_turnover_values.append(Decimal("0") if union_count == 0 else Decimal(union_count - intersection_count) / Decimal(union_count))
        stable_core_values.append(Decimal("1") if not prev_set and not curr_set else Decimal(intersection_count) / Decimal(max(1, min(len(prev_set), len(curr_set)))))
        if prev_set == curr_set:
            identical_target_sets += 1
        if top_symbols[index - 1] is not None and top_symbols[index - 1] == top_symbols[index]:
            dominant_symbol_same_count += 1
        if latest_bar_signatures[index - 1] == latest_bar_signatures[index]:
            identical_bar_dates += 1
    avg_target_count = _mean([Decimal(len(item)) for item in target_sets])
    avg_distance_l1 = _mean(distance_l1_values)
    avg_unconstrained = _mean(unconstrained_values)
    avg_threshold = _mean(threshold_values)
    avg_constraint = _mean(constraint_values)
    avg_target_change_l1 = _mean(target_change_l1_values)
    avg_target_change_l2 = _mean(target_change_l2_values)
    avg_turnover = _mean(target_turnover_values)
    avg_stable_core = _mean(stable_core_values)
    dominant_symbol_persistence = None if comparisons <= 0 else Decimal(dominant_symbol_same_count) / Decimal(comparisons)
    replay_cycles = sum(1 for metric in metrics if str(metric.get("run_mode")) == "replay_paper")
    replay_rate = Decimal(replay_cycles) / Decimal(len(metrics))
    identical_target_set_rate = Decimal(identical_target_sets) / Decimal(comparisons) if comparisons > 0 else None
    identical_bar_date_rate = Decimal(identical_bar_dates) / Decimal(comparisons) if comparisons > 0 else None
    material_input_advancement_count = comparisons - identical_bar_dates
    material_input_advancement_rate = Decimal(material_input_advancement_count) / Decimal(comparisons) if comparisons > 0 else None
    near_target_rate = Decimal(near_target_cycles) / Decimal(len(metrics))
    threshold_suppression_rate = Decimal(threshold_suppression_cycles) / Decimal(len(metrics))
    constraint_suppression_rate = Decimal(constraint_suppression_cycles) / Decimal(len(metrics))
    near_miss_rate = Decimal(near_miss_cycles) / Decimal(len(metrics))
    low_dispersion_rate = Decimal(cycles_low_dispersion) / Decimal(len(metrics)) if cross_dispersion_values else None
    interpretation = "mixed_sparse"
    interpretation_reason = "multiple_sparse_forces_present_without_single_dominant_driver"
    if avg_target_change_l1 is None or avg_distance_l1 is None or avg_unconstrained is None:
        interpretation = "insufficient_data"
        interpretation_reason = "missing_cycle_level_target_or_distance_diagnostics"
    elif (
        replay_rate >= Decimal("0.8")
        and identical_bar_date_rate is not None
        and identical_bar_date_rate >= Decimal("0.8")
        and avg_target_change_l1 <= Decimal("0.0001")
        and (avg_turnover is None or avg_turnover <= Decimal("0.05"))
    ):
        interpretation = "bootstrap_too_smooth"
        interpretation_reason = "replay_cycles_have_unchanged_bar_dates_and_near_zero_target_drift"
    elif near_target_rate >= Decimal("0.7") and avg_distance_l1 <= Decimal("0.10") and avg_unconstrained <= Decimal("0.10"):
        interpretation = "already_at_target"
        interpretation_reason = "portfolio_distance_to_target_stays_low_and_required_turnover_is_small"
    elif threshold_suppression_rate >= Decimal("0.4") and avg_threshold is not None and avg_threshold >= Decimal("0.03") and near_miss_rate >= Decimal("0.3"):
        interpretation = "threshold_too_wide"
        interpretation_reason = "target_drift_exists_but_subthreshold_rebalance_notional_repeats_near_trigger"
    elif constraint_suppression_rate >= Decimal("0.4") and avg_constraint is not None and avg_constraint >= Decimal("0.03"):
        interpretation = "constraint_suppressed"
        interpretation_reason = "material_rebalance_notional_is_blocked_by_runtime_or_risk_constraints"
    elif (
        avg_turnover is not None
        and avg_turnover <= Decimal("0.05")
        and avg_stable_core is not None
        and avg_stable_core >= Decimal("0.95")
        and dominant_symbol_persistence is not None
        and dominant_symbol_persistence >= Decimal("0.8")
    ):
        interpretation = "universe_too_stable"
        interpretation_reason = "target_membership_and_top_symbol_change_too_little_across_cycles"
    return {
        "avg_target_count": None if avg_target_count is None else format(avg_target_count, "f"),
        "avg_target_weight_change_l1": None if avg_target_change_l1 is None else format(avg_target_change_l1, "f"),
        "avg_target_weight_change_l2": None if avg_target_change_l2 is None else format(avg_target_change_l2, "f"),
        "avg_target_name_turnover_rate": None if avg_turnover is None else format(avg_turnover, "f"),
        "stable_target_core_rate": None if avg_stable_core is None else format(avg_stable_core, "f"),
        "cycles_with_identical_target_set": identical_target_sets,
        "dominant_symbol_persistence": None if dominant_symbol_persistence is None else format(dominant_symbol_persistence, "f"),
        "avg_portfolio_distance_to_target_l1": None if avg_distance_l1 is None else format(avg_distance_l1, "f"),
        "avg_portfolio_distance_to_target_l2": None if not distance_l2_values else format(_mean(distance_l2_values), "f"),
        "cycles_already_near_target": near_target_cycles,
        "near_target_rate": format(near_target_rate, "f"),
        "avg_unconstrained_rebalance_notional_ratio": None if avg_unconstrained is None else format(avg_unconstrained, "f"),
        "avg_threshold_suppressed_notional_ratio": None if avg_threshold is None else format(avg_threshold, "f"),
        "avg_constraint_suppressed_notional_ratio": None if avg_constraint is None else format(avg_constraint, "f"),
        "cycles_with_threshold_suppression": threshold_suppression_cycles,
        "cycles_with_constraint_suppression": constraint_suppression_cycles,
        "cycles_with_near_miss_rebalance": near_miss_cycles,
        "near_miss_rebalance_rate": format(near_miss_rate, "f"),
        "avg_cross_sectional_dispersion": None if not cross_dispersion_values else format(_mean(cross_dispersion_values), "f"),
        "avg_signal_distance_from_trigger": None if not signal_trigger_distances else format(_mean(signal_trigger_distances), "f"),
        "avg_realized_volatility_proxy": None if not vol_values else format(_mean(vol_values), "f"),
        "cycles_low_dispersion": cycles_low_dispersion,
        "cycles_with_identical_latest_bar_dates": identical_bar_dates,
        "identical_latest_bar_date_rate": None if identical_bar_date_rate is None else format(identical_bar_date_rate, "f"),
        "cycles_with_material_input_advancement": material_input_advancement_count,
        "material_input_advancement_rate": None if material_input_advancement_rate is None else format(material_input_advancement_rate, "f"),
        "interpretation": interpretation,
        "interpretation_reason": interpretation_reason,
    }


def _build_trend_construction_summary(metrics: list[dict]) -> dict[str, object] | None:
    if not metrics:
        return None
    positive_counts: list[Decimal] = []
    capped_counts: list[Decimal] = []
    gross_raw_weights: list[Decimal] = []
    gross_scaled_weights: list[Decimal] = []
    scaling_factors: list[Decimal] = []
    top_weight_shares: list[Decimal] = []
    effective_symbol_counts: list[Decimal] = []
    positive_core_sets: list[set[str]] = []
    max_weight_binding_cycles = 0
    scale_applied_cycles = 0
    concentrated_cycles = 0
    for metric in metrics:
        details = json.loads(metric["details"]) if metric.get("details") else {}
        cycle = details.get("trend_cycle_diagnostics") or {}
        construction = cycle.get("construction") or {}
        positive_symbols = {str(symbol) for symbol in construction.get("positive_momentum_symbols", [])}
        capped_symbols = {str(symbol) for symbol in construction.get("capped_target_symbols", [])}
        positive_core_sets.append(positive_symbols)
        positive_counts.append(Decimal(int(construction.get("positive_momentum_count", len(positive_symbols)) or 0)))
        capped_counts.append(Decimal(int(construction.get("capped_target_count", len(capped_symbols)) or 0)))
        gross_raw = to_decimal(construction.get("gross_raw_target_weight") or "0")
        gross_scaled = to_decimal(construction.get("gross_scaled_target_weight") or "0")
        scaling = to_decimal(construction.get("scaling_factor") or "0")
        top_share = to_decimal(construction.get("top_weight_share") or "0")
        effective_names = to_decimal(construction.get("effective_symbol_count") or "0")
        gross_raw_weights.append(gross_raw)
        gross_scaled_weights.append(gross_scaled)
        scaling_factors.append(scaling)
        top_weight_shares.append(top_share)
        effective_symbol_counts.append(effective_names)
        if capped_symbols:
            max_weight_binding_cycles += 1
        if gross_raw > 0 and scaling < Decimal("0.999"):
            scale_applied_cycles += 1
        if top_share >= Decimal("0.5") or effective_names <= Decimal("2.1"):
            concentrated_cycles += 1
    stable_positive_core_pairs = 0
    for index in range(1, len(positive_core_sets)):
        if positive_core_sets[index] == positive_core_sets[index - 1]:
            stable_positive_core_pairs += 1
    comparisons = max(0, len(positive_core_sets) - 1)
    stable_positive_core_rate = None if comparisons == 0 else Decimal(stable_positive_core_pairs) / Decimal(comparisons)
    avg_positive_count = _mean(positive_counts)
    avg_capped_count = _mean(capped_counts)
    avg_scaling_factor = _mean(scaling_factors)
    avg_top_weight_share = _mean(top_weight_shares)
    avg_effective_symbol_count = _mean(effective_symbol_counts)
    interpretation = "mixed_construction"
    interpretation_reason = "construction_constraints_are_present_but_not_singular"
    if avg_positive_count is None:
        interpretation = "insufficient_data"
        interpretation_reason = "missing_construction_diagnostics"
    elif (
        avg_positive_count <= Decimal("2.2")
        and stable_positive_core_rate is not None
        and stable_positive_core_rate >= Decimal("0.8")
        and avg_capped_count is not None
        and avg_capped_count >= Decimal("2")
    ):
        interpretation = "narrow_positive_universe_with_weight_saturation"
        interpretation_reason = "same_small_positive_symbol_set_repeatedly_hits_max_weight"
    elif avg_capped_count is not None and avg_capped_count >= Decimal("2"):
        interpretation = "weight_saturation"
        interpretation_reason = "multiple_symbols_repeatedly_bind_at_max_weight"
    elif stable_positive_core_rate is not None and stable_positive_core_rate >= Decimal("0.8") and avg_positive_count <= Decimal("2.2"):
        interpretation = "narrow_positive_universe"
        interpretation_reason = "few_positive_momentum_symbols_repeat_across_cycles"
    return {
        "avg_positive_momentum_count": None if avg_positive_count is None else format(avg_positive_count, "f"),
        "avg_capped_target_count": None if avg_capped_count is None else format(avg_capped_count, "f"),
        "avg_gross_raw_target_weight": None if not gross_raw_weights else format(_mean(gross_raw_weights), "f"),
        "avg_gross_scaled_target_weight": None if not gross_scaled_weights else format(_mean(gross_scaled_weights), "f"),
        "avg_scaling_factor": None if avg_scaling_factor is None else format(avg_scaling_factor, "f"),
        "avg_top_weight_share": None if avg_top_weight_share is None else format(avg_top_weight_share, "f"),
        "avg_effective_symbol_count": None if avg_effective_symbol_count is None else format(avg_effective_symbol_count, "f"),
        "cycles_with_max_weight_binding": max_weight_binding_cycles,
        "cycles_with_scale_applied": scale_applied_cycles,
        "cycles_with_high_concentration": concentrated_cycles,
        "stable_positive_core_rate": None if stable_positive_core_rate is None else format(stable_positive_core_rate, "f"),
        "interpretation": interpretation,
        "interpretation_reason": interpretation_reason,
    }


def _build_trend_validation_summary(metrics: list[dict]) -> dict[str, object] | None:
    if not metrics:
        return None
    trade_counts: list[int] = []
    signal_counts: list[int] = []
    position_counts: list[int] = []
    gross_ratios: list[Decimal] = []
    cash_ratios: list[Decimal] = []
    target_change_l1_values: list[Decimal] = []
    portfolio_distance_l1_values: list[Decimal] = []
    effective_name_counts: list[Decimal] = []
    top_weight_shares: list[Decimal] = []
    capped_counts: list[Decimal] = []
    realized_edges: list[Decimal] = []
    expected_realized_gaps: list[Decimal] = []
    for metric in metrics:
        details = json.loads(metric["details"]) if metric.get("details") else {}
        trade_count = int(metric.get("trade_count") or 0)
        signal_count = int(details.get("signals_generated", 0) or 0)
        position_count = int(details.get("position_count", 0) or 0)
        capital = to_decimal(metric.get("capital_assigned_decimal") or "0")
        gross = to_decimal(details.get("gross_exposure") or "0")
        cash = to_decimal(details.get("cash") or "0")
        cycle = details.get("trend_cycle_diagnostics") or {}
        construction = cycle.get("construction") or {}
        trade_counts.append(trade_count)
        signal_counts.append(signal_count)
        position_counts.append(position_count)
        if capital > 0:
            gross_ratios.append(gross / capital)
            cash_ratios.append(cash / capital)
        if cycle.get("target_weights") is not None and cycle.get("current_weights") is not None:
            target_change_l1_values.append(to_decimal(cycle.get("portfolio_distance_to_target_l1") or "0"))
            portfolio_distance_l1_values.append(to_decimal(cycle.get("portfolio_distance_to_target_l1") or "0"))
        effective_name_counts.append(to_decimal(construction.get("effective_symbol_count") or "0"))
        top_weight_shares.append(to_decimal(construction.get("top_weight_share") or "0"))
        capped_counts.append(to_decimal(construction.get("capped_target_count") or "0"))
        if metric.get("avg_realized_edge_decimal") is not None:
            realized_edges.append(to_decimal(metric["avg_realized_edge_decimal"]))
        if metric.get("expected_realized_gap_decimal") is not None:
            expected_realized_gaps.append(to_decimal(metric["expected_realized_gap_decimal"]))
    cycles = Decimal(len(metrics))
    cycles_with_trades = sum(1 for count in trade_counts if count > 0)
    cycles_with_signals = sum(1 for count in signal_counts if count > 0)
    trade_cycle_rate = Decimal(cycles_with_trades) / cycles if cycles > 0 else Decimal("0")
    signal_cycle_rate = Decimal(cycles_with_signals) / cycles if cycles > 0 else Decimal("0")
    avg_trade_count_per_trade_cycle = None
    active_trade_counts = [Decimal(count) for count in trade_counts if count > 0]
    if active_trade_counts:
        avg_trade_count_per_trade_cycle = _mean(active_trade_counts)
    avg_target_change_l1 = _mean(target_change_l1_values)
    avg_distance_l1 = _mean(portfolio_distance_l1_values)
    avg_effective_names = _mean(effective_name_counts)
    avg_top_weight_share = _mean(top_weight_shares)
    avg_capped_count = _mean(capped_counts)
    avg_gross_ratio = _mean(gross_ratios)
    avg_cash_ratio = _mean(cash_ratios)
    avg_position_count = _mean([Decimal(count) for count in position_counts])
    avg_realized_edge = _mean(realized_edges)
    avg_gap = _mean(expected_realized_gaps)
    interpretation = "mixed_validation"
    interpretation_reason = "trend_runtime_has_partial_allocator_expression_but_no_single_clear_validation_story"
    if avg_effective_names is None:
        interpretation = "insufficient_data"
        interpretation_reason = "missing_trend_construction_diagnostics"
    elif avg_capped_count is not None and avg_capped_count >= Decimal("2"):
        interpretation = "construction_still_saturated"
        interpretation_reason = "multiple_names_still_bind_at_cap_under_bounded_run"
    elif avg_effective_names >= Decimal("5") and avg_target_change_l1 is not None and avg_target_change_l1 < Decimal("0.01"):
        interpretation = "expressive_but_low_drift"
        interpretation_reason = "allocator_is_diversified_but_target_drift_per_cycle_remains_small"
    elif avg_effective_names >= Decimal("5") and signal_cycle_rate >= Decimal("0.2") and avg_target_change_l1 is not None and avg_target_change_l1 >= Decimal("0.01"):
        interpretation = "responsive_allocator_with_light_rebalance"
        interpretation_reason = "allocator_shows_diversification_and_nontrivial_target_drift_without_cap_saturation"
    return {
        "cycles_with_trades": cycles_with_trades,
        "trade_cycle_rate": format(trade_cycle_rate, "f"),
        "signal_cycle_rate": format(signal_cycle_rate, "f"),
        "avg_trade_count_per_trade_cycle": None if avg_trade_count_per_trade_cycle is None else format(avg_trade_count_per_trade_cycle, "f"),
        "avg_position_count": None if avg_position_count is None else format(avg_position_count, "f"),
        "avg_target_weight_change_l1": None if avg_target_change_l1 is None else format(avg_target_change_l1, "f"),
        "avg_portfolio_distance_to_target_l1": None if avg_distance_l1 is None else format(avg_distance_l1, "f"),
        "avg_effective_symbol_count": None if avg_effective_names is None else format(avg_effective_names, "f"),
        "avg_top_weight_share": None if avg_top_weight_share is None else format(avg_top_weight_share, "f"),
        "avg_capped_target_count": None if avg_capped_count is None else format(avg_capped_count, "f"),
        "avg_gross_exposure_ratio": None if avg_gross_ratio is None else format(avg_gross_ratio, "f"),
        "avg_cash_ratio": None if avg_cash_ratio is None else format(avg_cash_ratio, "f"),
        "avg_realized_edge_over_cycles": None if avg_realized_edge is None else format(avg_realized_edge, "f"),
        "avg_expected_realized_gap_over_cycles": None if avg_gap is None else format(avg_gap, "f"),
        "interpretation": interpretation,
        "interpretation_reason": interpretation_reason,
    }


def _build_trend_signal_summary(metrics: list[dict]) -> dict[str, object] | None:
    if not metrics:
        return None
    universe_counts: list[Decimal] = []
    positive_counts: list[Decimal] = []
    positive_rates: list[Decimal] = []
    top_positive_momenta: list[Decimal] = []
    avg_positive_momenta: list[Decimal] = []
    avg_negative_momenta: list[Decimal] = []
    cross_sectional_spreads: list[Decimal] = []
    top2_gaps: list[Decimal] = []
    strong_rank_separation_cycles = 0
    broad_positive_cycles = 0
    mixed_regime_cycles = 0
    for metric in metrics:
        details = json.loads(metric["details"]) if metric.get("details") else {}
        cycle = details.get("trend_cycle_diagnostics") or {}
        construction = cycle.get("construction") or {}
        rows = construction.get("rows") or []
        momentum_values: list[Decimal] = []
        positive_values: list[Decimal] = []
        negative_values: list[Decimal] = []
        for row in rows:
            momentum_raw = row.get("momentum_return")
            if momentum_raw is None:
                continue
            momentum = to_decimal(momentum_raw)
            momentum_values.append(momentum)
            if momentum > 0:
                positive_values.append(momentum)
            elif momentum < 0:
                negative_values.append(momentum)
        if not momentum_values:
            continue
        universe_count = Decimal(len(momentum_values))
        positive_count = Decimal(len(positive_values))
        positive_rate = positive_count / universe_count
        universe_counts.append(universe_count)
        positive_counts.append(positive_count)
        positive_rates.append(positive_rate)
        cross_sectional_spreads.append(max(momentum_values) - min(momentum_values))
        if positive_values:
            ordered_positive = sorted(positive_values, reverse=True)
            top_positive_momenta.append(ordered_positive[0])
            avg_positive_momenta.append(_mean(positive_values) or Decimal("0"))
            if len(ordered_positive) >= 2:
                gap = ordered_positive[0] - ordered_positive[1]
                top2_gaps.append(gap)
                if gap >= Decimal("0.02"):
                    strong_rank_separation_cycles += 1
        if negative_values:
            avg_negative_momenta.append(_mean(negative_values) or Decimal("0"))
        if positive_rate >= Decimal("0.7"):
            broad_positive_cycles += 1
        elif positive_rate >= Decimal("0.3"):
            mixed_regime_cycles += 1
    if not universe_counts:
        return {
            "interpretation": "insufficient_data",
            "interpretation_reason": "missing_cycle_level_momentum_rows",
        }
    total_cycles = Decimal(len(universe_counts))
    avg_positive_rate = _mean(positive_rates)
    avg_top_positive = _mean(top_positive_momenta)
    avg_positive_momentum = _mean(avg_positive_momenta)
    avg_negative_momentum = _mean(avg_negative_momenta)
    avg_spread = _mean(cross_sectional_spreads)
    avg_top2_gap = _mean(top2_gaps)
    strong_rank_rate = Decimal(strong_rank_separation_cycles) / total_cycles
    broad_positive_rate = Decimal(broad_positive_cycles) / total_cycles
    mixed_regime_rate = Decimal(mixed_regime_cycles) / total_cycles
    interpretation = "mixed_signal_profile"
    interpretation_reason = "signal_profile_has_some_dispersion_but_not_a_single_clear_regime"
    if avg_positive_rate is None or avg_spread is None:
        interpretation = "insufficient_data"
        interpretation_reason = "missing_aggregated_momentum_statistics"
    elif broad_positive_rate >= Decimal("0.7") and (avg_top2_gap is None or avg_top2_gap < Decimal("0.01")):
        interpretation = "broad_risk_on_low_separation"
        interpretation_reason = "many_assets_are_positive_but_top_rank_separation_is_small"
    elif avg_spread <= Decimal("0.05") and (avg_top_positive is None or avg_top_positive <= Decimal("0.05")):
        interpretation = "weak_signal_regime"
        interpretation_reason = "cross_sectional_momentum_spread_and_absolute_signal_strength_are_both_small"
    elif strong_rank_rate >= Decimal("0.4") and avg_spread >= Decimal("0.08"):
        interpretation = "meaningful_rank_dispersion"
        interpretation_reason = "cross_sectional_momentum_spread_is_large_enough_to_support_rotation"
    return {
        "avg_universe_count": format(_mean(universe_counts) or Decimal("0"), "f"),
        "avg_positive_symbol_count": format(_mean(positive_counts) or Decimal("0"), "f"),
        "avg_positive_symbol_rate": None if avg_positive_rate is None else format(avg_positive_rate, "f"),
        "avg_top_positive_momentum": None if avg_top_positive is None else format(avg_top_positive, "f"),
        "avg_positive_momentum": None if avg_positive_momentum is None else format(avg_positive_momentum, "f"),
        "avg_negative_momentum": None if avg_negative_momentum is None else format(avg_negative_momentum, "f"),
        "avg_cross_sectional_momentum_spread": None if avg_spread is None else format(avg_spread, "f"),
        "avg_top2_positive_gap": None if avg_top2_gap is None else format(avg_top2_gap, "f"),
        "strong_rank_separation_rate": format(strong_rank_rate, "f"),
        "broad_positive_regime_rate": format(broad_positive_rate, "f"),
        "mixed_regime_rate": format(mixed_regime_rate, "f"),
        "interpretation": interpretation,
        "interpretation_reason": interpretation_reason,
    }


def _build_bill_focus_summary(*, sleeve_repo: SleeveRepository, earliest_created_at: str | None) -> dict[str, object] | None:
    if earliest_created_at is None:
        return None
    rows = sleeve_repo.db.fetchall(
        """
        SELECT ticker, strategy, reason, edge_after_fees_decimal, quantity, metadata
        FROM strategy_evaluations
        WHERE created_at >= ? AND ticker LIKE 'KX3MTBILL%'
        ORDER BY id ASC
        """,
        (earliest_created_at,),
    )
    if not rows:
        return None
    candidate_rows = []
    for row in rows:
        metadata = json.loads(row["metadata"]) if row["metadata"] else {}
        edge_after_fees = to_decimal(row["edge_after_fees_decimal"] or 0)
        quantity = int(row["quantity"] or 0)
        if quantity <= 0:
            quantity = int(metadata.get("quantity", 0) or 0)
        if quantity <= 0:
            quantity = int(metadata.get("size_target", 0) or 0)
        lower_size = int(metadata.get("lower_yes_ask_size", 0) or 0)
        higher_size = int(metadata.get("higher_yes_bid_size", 0) or 0)
        executable_size = min(lower_size, higher_size) if lower_size and higher_size else max(lower_size, higher_size)
        size_gap = None
        if quantity > 0:
            size_gap = format(Decimal(executable_size) / Decimal(quantity), "f")
        candidate_rows.append(
            {
                "ticker": str(row["ticker"]),
                "strategy": str(row["strategy"]),
                "reason": str(row["reason"]),
                "edge_after_fees": format(edge_after_fees, "f"),
                "quantity": quantity,
                "lower_yes_ask_size": lower_size,
                "higher_yes_bid_size": higher_size,
                "executable_size": executable_size,
                "size_gap_ratio": size_gap,
                "min_size_multiple": metadata.get("min_size_multiple"),
                "average_signal_time_depth": metadata.get("average_signal_time_depth"),
                "insufficient_depth_fields": metadata.get("insufficient_depth_fields", []),
            }
        )
    top_candidate = max(candidate_rows, key=lambda item: (to_decimal(item["edge_after_fees"]), item["executable_size"]))
    repeated = [item for item in candidate_rows if item["ticker"] == top_candidate["ticker"] and item["reason"] == top_candidate["reason"]]
    avg_size_gap = _mean([to_decimal(item["size_gap_ratio"]) for item in repeated if item["size_gap_ratio"] is not None])
    return {
        "focus_family": "KX3MTBILL",
        "evaluations_seen": len(candidate_rows),
        "top_candidate": top_candidate,
        "top_candidate_appearances": len(repeated),
        "avg_size_gap_ratio_for_top_candidate": None if avg_size_gap is None else format(avg_size_gap, "f"),
        "interpretation": "likely_sizing_limited" if top_candidate["reason"] == "insufficient_size_multiple" and top_candidate["size_gap_ratio"] is not None and to_decimal(top_candidate["size_gap_ratio"]) < Decimal("0.1") else "mixed_constraints",
    }


def _build_trend_focus_summary(metrics: list[dict]) -> dict[str, object] | None:
    if not metrics:
        return None
    gross_ratios: list[Decimal] = []
    cash_ratios: list[Decimal] = []
    drawdowns: list[Decimal] = []
    cycles_with_signals = 0
    cycles_without_signals = 0
    cycles_with_targets = 0
    position_counts: list[int] = []
    target_counts: list[int] = []
    signal_counts: list[int] = []
    no_trade_counts: dict[str, int] = defaultdict(int)
    for metric in metrics:
        details = json.loads(metric["details"]) if metric.get("details") else {}
        capital = to_decimal(metric.get("capital_assigned_decimal") or "0")
        gross = to_decimal(details.get("gross_exposure") or "0")
        cash = to_decimal(details.get("cash") or "0")
        drawdown = to_decimal(details.get("drawdown") or "0")
        if capital > 0:
            gross_ratios.append(gross / capital)
            cash_ratios.append(cash / capital)
        drawdowns.append(drawdown)
        target_count = int(details.get("targets_generated", 0) or 0)
        signal_count = int(details.get("signals_generated", 0) or 0)
        target_counts.append(target_count)
        signal_counts.append(signal_count)
        if target_count > 0:
            cycles_with_targets += 1
        if signal_count > 0:
            cycles_with_signals += 1
        else:
            cycles_without_signals += 1
        position_counts.append(int(details.get("position_count", 0) or 0))
        reasons = json.loads(metric["no_trade_reasons"]) if metric.get("no_trade_reasons") else {}
        for reason, count in reasons.items():
            _bump_reason(no_trade_counts, str(reason), int(count))
    dominant_reason = _top_reason_pairs(no_trade_counts, limit=1)
    interpretation = "unclear"
    if dominant_reason:
        if dominant_reason[0]["reason"] == "no_rebalance_needed":
            interpretation = "already_positioned_not_retriggering"
        elif dominant_reason[0]["reason"] == "risk_gate":
            interpretation = "risk_constraints_dominating"
        elif dominant_reason[0]["reason"] == "insufficient_history":
            interpretation = "insufficient_history_or_sparse_universe"
    avg_gross_ratio = _mean(gross_ratios)
    avg_cash_ratio = _mean(cash_ratios)
    avg_targets_generated = _mean([Decimal(item) for item in target_counts]) if target_counts else None
    avg_signals_generated = _mean([Decimal(item) for item in signal_counts]) if signal_counts else None
    if interpretation == "unclear":
        if cycles_with_targets == 0:
            interpretation = "sparse_or_unqualified_universe"
        elif cycles_with_signals == 0 and avg_gross_ratio is not None and avg_gross_ratio < Decimal("0.35"):
            interpretation = "underdeployed_without_rebalance_triggers"
        elif cycles_with_signals <= max(1, len(metrics) // 5) and avg_gross_ratio is not None and avg_gross_ratio >= Decimal("0.5"):
            interpretation = "deployed_but_static"
        elif cycles_with_signals >= max(2, len(metrics) // 3):
            interpretation = "active_rebalancing"
    return {
        "cycles_with_signals": cycles_with_signals,
        "cycles_without_signals": cycles_without_signals,
        "signal_cycle_rate": format(Decimal(cycles_with_signals) / Decimal(len(metrics)), "f"),
        "target_cycle_rate": format(Decimal(cycles_with_targets) / Decimal(len(metrics)), "f"),
        "avg_targets_generated": None if avg_targets_generated is None else format(avg_targets_generated, "f"),
        "avg_signals_generated": None if avg_signals_generated is None else format(avg_signals_generated, "f"),
        "avg_gross_exposure_ratio": None if avg_gross_ratio is None else format(avg_gross_ratio, "f"),
        "avg_cash_ratio": None if avg_cash_ratio is None else format(avg_cash_ratio, "f"),
        "avg_position_count": None if not position_counts else format(Decimal(sum(position_counts)) / Decimal(len(position_counts)), "f"),
        "avg_drawdown": None if not drawdowns else format(_mean(drawdowns), "f"),
        "latest_position_count": position_counts[-1] if position_counts else 0,
        "dominant_no_trade_reason": dominant_reason[0] if dominant_reason else None,
        "interpretation": interpretation,
    }


def _summarize_kalshi_evaluations(evaluations: list) -> dict[str, object]:
    if not evaluations:
        return {
            "avg_expected_edge": None,
            "max_expected_edge": None,
            "median_expected_edge": None,
            "best_edge_seen": None,
            "top_candidates_nearest_to_trade": [],
        }
    edges = [to_decimal(item.edge_after_fees) for item in evaluations]
    missed = [item for item in evaluations if not item.generated]
    top_candidates = sorted(
        missed,
        key=lambda item: (to_decimal(item.edge_after_fees), to_decimal(item.edge_before_fees)),
        reverse=True,
    )[:3]
    return {
        "avg_expected_edge": None if not edges else format(_mean(edges), "f"),
        "max_expected_edge": None if not edges else format(max(edges), "f"),
        "median_expected_edge": None if not edges else format(_median(edges), "f"),
        "best_edge_seen": None if not edges else format(max(edges), "f"),
        "top_candidates_nearest_to_trade": [
            {
                "ticker": item.ticker,
                "strategy": item.strategy.value,
                "reason": item.reason,
                "edge_before_fees": format(to_decimal(item.edge_before_fees), "f"),
                "edge_after_fees": format(to_decimal(item.edge_after_fees), "f"),
                "quantity": item.quantity,
            }
            for item in top_candidates
        ],
    }


def _build_smooth_bootstrap_curves() -> dict[str, list[str]]:
    return {
        "SPY": [str(500 + step) for step in range(30)],
        "QQQ": [str(400 + (step // 2) + step) for step in range(30)],
        "IWM": [str(200 + (step * Decimal("0.7"))) for step in range(30)],
        "EFA": [str(75 + (step * Decimal("0.35"))) for step in range(30)],
        "EEM": [str(42 + (step * Decimal("0.20"))) for step in range(30)],
        "TLT": [str(110 - step) for step in range(30)],
        "IEF": [str(95 - (step * Decimal("0.25"))) for step in range(30)],
        "TIP": [str(108 + (step * Decimal("0.12"))) for step in range(30)],
        "LQD": [str(104 + (step * Decimal("0.18"))) for step in range(30)],
        "HYG": [str(78 + (step * Decimal("0.22"))) for step in range(30)],
        "GLD": [str(190 + (step * Decimal("0.28"))) for step in range(30)],
        "DBC": [str(24 + (step * Decimal("0.16"))) for step in range(30)],
    }


def _build_diverse_bootstrap_curves(total_points: int = 120) -> dict[str, list[str]]:
    curves: dict[str, list[str]] = {}
    spy = Decimal("470")
    qqq = Decimal("385")
    iwm = Decimal("205")
    efa = Decimal("76")
    eem = Decimal("41")
    tlt = Decimal("118")
    ief = Decimal("97")
    tip = Decimal("109")
    lqd = Decimal("105")
    hyg = Decimal("79")
    gld = Decimal("192")
    dbc = Decimal("23")
    spy_points: list[str] = []
    qqq_points: list[str] = []
    iwm_points: list[str] = []
    efa_points: list[str] = []
    eem_points: list[str] = []
    tlt_points: list[str] = []
    ief_points: list[str] = []
    tip_points: list[str] = []
    lqd_points: list[str] = []
    hyg_points: list[str] = []
    gld_points: list[str] = []
    dbc_points: list[str] = []
    for step in range(total_points):
        if step < 35:
            spy += Decimal("1.15")
            qqq += Decimal("1.35")
            iwm += Decimal("0.95")
            efa += Decimal("0.40")
            eem += Decimal("0.28")
            tlt -= Decimal("0.45")
            ief -= Decimal("0.18")
            tip += Decimal("0.08")
            lqd += Decimal("0.22")
            hyg += Decimal("0.30")
            gld += Decimal("0.18")
            dbc += Decimal("0.20")
        elif step < 70:
            spy += Decimal("0.55") if step % 2 == 0 else Decimal("-0.35")
            qqq += Decimal("0.85") if step % 3 != 0 else Decimal("-0.60")
            iwm += Decimal("0.35") if step % 2 == 0 else Decimal("-0.40")
            efa += Decimal("0.28") if step % 3 == 0 else Decimal("-0.12")
            eem += Decimal("0.22") if step % 4 in {0, 1} else Decimal("-0.18")
            tlt += Decimal("0.25") if step % 2 == 0 else Decimal("-0.10")
            ief += Decimal("0.12") if step % 2 == 0 else Decimal("-0.05")
            tip += Decimal("0.10") if step % 2 == 0 else Decimal("0.02")
            lqd += Decimal("0.12") if step % 3 != 0 else Decimal("-0.08")
            hyg += Decimal("0.18") if step % 4 != 0 else Decimal("-0.16")
            gld += Decimal("0.24") if step % 3 == 0 else Decimal("-0.06")
            dbc += Decimal("0.14") if step % 2 == 0 else Decimal("-0.10")
        else:
            spy += Decimal("-0.75") if step % 4 == 0 else Decimal("0.30")
            qqq += Decimal("-1.10") if step % 5 == 0 else Decimal("0.45")
            iwm += Decimal("-0.80") if step % 3 == 0 else Decimal("0.20")
            efa += Decimal("-0.18") if step % 4 == 0 else Decimal("0.16")
            eem += Decimal("-0.30") if step % 3 == 0 else Decimal("0.22")
            tlt += Decimal("0.55") if step % 3 == 0 else Decimal("-0.20")
            ief += Decimal("0.18") if step % 3 == 0 else Decimal("-0.06")
            tip += Decimal("0.09") if step % 2 == 0 else Decimal("-0.03")
            lqd += Decimal("0.08") if step % 4 != 0 else Decimal("-0.14")
            hyg += Decimal("-0.24") if step % 3 == 0 else Decimal("0.10")
            gld += Decimal("0.34") if step % 4 == 0 else Decimal("-0.05")
            dbc += Decimal("-0.20") if step % 5 == 0 else Decimal("0.12")
        spy_points.append(format(spy, "f"))
        qqq_points.append(format(qqq, "f"))
        iwm_points.append(format(iwm, "f"))
        efa_points.append(format(efa, "f"))
        eem_points.append(format(eem, "f"))
        tlt_points.append(format(tlt, "f"))
        ief_points.append(format(ief, "f"))
        tip_points.append(format(tip, "f"))
        lqd_points.append(format(lqd, "f"))
        hyg_points.append(format(hyg, "f"))
        gld_points.append(format(gld, "f"))
        dbc_points.append(format(dbc, "f"))
    curves["SPY"] = spy_points
    curves["QQQ"] = qqq_points
    curves["IWM"] = iwm_points
    curves["EFA"] = efa_points
    curves["EEM"] = eem_points
    curves["TLT"] = tlt_points
    curves["IEF"] = ief_points
    curves["TIP"] = tip_points
    curves["LQD"] = lqd_points
    curves["HYG"] = hyg_points
    curves["GLD"] = gld_points
    curves["DBC"] = dbc_points
    return curves


def _bootstrap_trend_replay_bars(settings: Settings, *, db: Database, as_of: date | None = None, variant: str | None = None) -> dict[str, object]:
    trend_repo = TrendRepository(db)
    variant_name = (variant or settings.trend_bootstrap_variant or "smooth").strip().lower()
    end_date = as_of or date.today()
    seeded: list[DailyBar] = []
    if variant_name == "diverse":
        source_curves = _build_diverse_bootstrap_curves()
        window = 30
        offset = max(0, min((end_date - date.today()).days, len(next(iter(source_curves.values()))) - window))
        synthetic_curves = {symbol: series[offset : offset + window] for symbol, series in source_curves.items()}
    else:
        synthetic_curves = _build_smooth_bootstrap_curves()
    start_date = end_date - timedelta(days=len(next(iter(synthetic_curves.values()))) - 1)
    for symbol in settings.trend_symbols:
        closes = synthetic_curves.get(symbol)
        if closes is None:
            closes = [str(100 + step) for step in range(len(next(iter(synthetic_curves.values()))))]
        for index, close in enumerate(closes):
            seeded.append(DailyBar(symbol=symbol, bar_date=start_date + timedelta(days=index), close=Decimal(close)))
    trend_repo.persist_bars(seeded)
    return {
        "mode": "replay_paper",
        "bootstrap_variant": variant_name,
        "symbols_seeded": sorted({bar.symbol for bar in seeded}),
        "bars_seeded": len(seeded),
        "start_date": start_date.isoformat(),
        "end_date": (start_date + timedelta(days=len(next(iter(synthetic_curves.values()))) - 1)).isoformat(),
    }


class TrendBarUpdater:
    def __init__(self, repo: TrendRepository, logger, settings: Settings) -> None:
        self.repo = repo
        self.logger = logger
        self.settings = settings
        self.provider = YahooHistoricalBarProvider(timeout_seconds=settings.trend_fetch_timeout_seconds)

    def refresh(self, symbols: list[str], *, as_of: datetime) -> dict[str, object]:
        start = as_of - timedelta(days=120)
        end = as_of + timedelta(days=1)
        errors: list[str] = []
        written = 0
        live_fetch_attempted = self.settings.trend_data_mode != "cached"
        live_fetch_succeeded = 0
        for symbol in symbols:
            if not live_fetch_attempted:
                continue
            try:
                result = None
                for attempt in range(self.settings.trend_fetch_retries + 1):
                    try:
                        result = self.provider.fetch(symbol=symbol, interval="1d", start=start, end=end)
                        break
                    except Exception:
                        if attempt >= self.settings.trend_fetch_retries:
                            raise
                        time.sleep(self.settings.trend_fetch_backoff_seconds)
                bars: list[DailyBar] = []
                seen: set[date] = set()
                for item in (result.bars if result is not None else []):
                    bar_date = item.timestamp_utc.date()
                    if bar_date in seen:
                        continue
                    seen.add(bar_date)
                    bars.append(DailyBar(symbol=symbol, bar_date=bar_date, close=Decimal(str(item.close))))
                if bars:
                    self.repo.persist_bars(bars)
                    live_fetch_succeeded += 1
                    written += len(bars)
            except Exception as exc:  # noqa: BLE001
                errors.append(f"{symbol}:{exc}")
                self.logger.error("trend_bar_refresh_failed", extra={"event": {"sleeve_name": "trend", "symbol": symbol, "error": str(exc)}})
        history = {}
        for symbol in symbols:
            bars = self.repo.load_recent_bars(symbol, 32)
            history[symbol] = {
                "bars": len(bars),
                "latest_bar_date": None if not bars else bars[-1].bar_date.isoformat(),
            }
        cached_history_available = all(item["bars"] > 0 for item in history.values())
        if live_fetch_succeeded > 0:
            run_mode = "live_paper"
        elif cached_history_available and self.settings.trend_data_mode in {"auto", "cached"}:
            run_mode = "replay_paper"
        else:
            run_mode = "data_unavailable"
        return {
            "symbols": len(symbols),
            "bars_written": written,
            "errors": errors,
            "run_mode": run_mode,
            "live_fetch_attempted": live_fetch_attempted,
            "live_fetch_succeeded": live_fetch_succeeded,
            "cached_history_available": cached_history_available,
            "history": history,
        }


class KalshiBoosterSleeve:
    def __init__(self, *, settings: Settings, repo: KalshiRepository, market_state_repo: MarketStateRepository, logger, client: KalshiRestClient, execution_router: ExecutionRouter, portfolio: PortfolioState) -> None:
        self.settings = settings
        self.repo = repo
        self.market_state_repo = market_state_repo
        self.logger = logger
        self.client = client
        self.execution_router = execution_router
        self.portfolio = portfolio
        self.market_data = MarketDataService(client, settings)
        self.external_provider = FedFuturesProvider(settings)
        self.monotonicity = MonotonicityStrategy(
            settings.min_edge_bps,
            settings.slippage_buffer_bps,
            settings.max_position_per_trade_pct,
            settings.monotonicity_min_top_level_depth,
            settings.monotonicity_min_size_multiple,
            settings.monotonicity_ladder_cooldown_seconds,
        )
        self.partition = PartitionStrategy(
            settings.min_edge_bps,
            settings.slippage_buffer_bps,
            settings.max_position_per_trade_pct,
            settings.min_depth,
            settings.partition_min_coverage,
        )
        self.cross_market = CrossMarketStrategy(
            min_edge=settings.min_edge,
            slippage_buffer=settings.slippage_buffer_bps / 10_000.0,
            min_depth=settings.min_depth,
            min_time_to_expiry_seconds=settings.min_time_to_expiry_seconds,
            max_position_pct=settings.max_position_per_trade_pct,
            directional_allowed=settings.app_mode == "aggressive",
        )

    def run_cycle(self, capital_assigned: Decimal) -> dict[str, object]:
        self.portfolio.snapshot = PortfolioSnapshot(equity=float(capital_assigned), cash=float(capital_assigned), daily_pnl=0.0, positions=self.portfolio.snapshot.positions)
        reconciliation = self.portfolio.reconcile(self.repo)
        raw_markets = self.market_data.refresh_markets()
        filter_result = self.market_data.filter_markets(raw_markets)
        markets = self.market_data.hydrate_structural_quotes(filter_result.included)
        for market in markets:
            self.market_state_repo.persist_many(market_state_inputs_from_kalshi_market(market, self.market_data.last_update))
        ladders = self.market_data.build_ladders(markets)
        if self.settings.app_mode == "safe":
            external = ExternalDataResult(distributions={}, stale=True, errors=[], fetched_at=datetime.now(timezone.utc), source="skipped_safe_mode")
        else:
            external = self.external_provider.fetch()
        readiness = assess_layer2_trading_readiness(
            market_data_last_update=self.market_data.last_update,
            max_market_data_staleness_seconds=self.settings.max_market_data_staleness_seconds,
            external_stale=external.stale,
            external_errors=external.errors,
            app_mode=self.settings.app_mode,
            reconciliation_success=reconciliation.success,
            reconciliation_mismatches=reconciliation.mismatches,
            now=datetime.now(timezone.utc),
        )
        stats = {
            "sleeve_name": "kalshi_event",
            "run_mode": "live_paper",
            "status": "running" if readiness.can_trade else "paused",
            "markets_seen": len(raw_markets),
            "markets_loaded": len(markets),
            "opportunities_seen": 0,
            "signals_generated": 0,
            "signals_attempted": 0,
            "no_trade_reasons": {},
            "trading_halt_reasons": readiness.halt_reasons,
            "data_error": self.client.last_error,
            "discovery_mode_used": self.market_data.last_discovery_stats.discovery_mode_used,
            "quote_failures": self.market_data.last_quote_stats.orderbook_failures,
            "edge_summary": {
                "avg_expected_edge": None,
                "max_expected_edge": None,
                "median_expected_edge": None,
                "best_edge_seen": None,
            },
            "top_candidates_nearest_to_trade": [],
        }
        cycle_evaluations = []
        if self.settings.kalshi_data_mode == "cached":
            stats["run_mode"] = "data_unavailable"
            stats["status"] = "disabled"
            stats["no_trade_reasons"] = {"disabled": 1, "cached_mode_not_supported": 1}
            return stats
        if self.client.last_error and not raw_markets:
            stats["run_mode"] = "data_unavailable"
            stats["status"] = "error"
            stats["no_trade_reasons"] = {"data_failure": 1}
            return stats
        if not raw_markets:
            stats["no_trade_reasons"] = {"no_open_markets": 1}
            return stats
        if not markets:
            stats["no_trade_reasons"] = {"no_valid_ladders": 1 if filter_result.included else 0, "insufficient_depth": 1 if filter_result.included else 0}
            return stats
        if not readiness.can_trade:
            reasons: dict[str, int] = {}
            for halt_reason in readiness.halt_reasons:
                _bump_reason(reasons, _normalize_no_trade_reason(halt_reason, "kalshi_event"))
            stats["no_trade_reasons"] = reasons
            return stats
        signals = []
        reason_counts: dict[str, int] = defaultdict(int)
        equity_reference = float(capital_assigned)
        for ladder in ladders.values():
            ladder_signals, evaluations = self.monotonicity.evaluate(ladder, equity_reference)
            cycle_evaluations.extend(evaluations)
            stats["opportunities_seen"] += len(evaluations)
            for evaluation in evaluations:
                self.repo.persist_evaluation(evaluation)
                if not evaluation.generated:
                    _bump_reason(reason_counts, _normalize_no_trade_reason(evaluation.reason, "kalshi_event"))
            signals.extend(ladder_signals)
        if self.settings.enable_partition_strategy:
            by_event_all: dict[str, list] = {}
            by_event_tradeable: dict[str, list] = {}
            for market in filter_result.included:
                by_event_all.setdefault(market.event_ticker, []).append(market)
            for market in markets:
                by_event_tradeable.setdefault(market.event_ticker, []).append(market)
            for event_ticker, all_group in by_event_all.items():
                if len(all_group) < 2:
                    continue
                group_signals, evaluations = self.partition.evaluate(by_event_tradeable.get(event_ticker, []), equity_reference, all_group)
                cycle_evaluations.extend(evaluations)
                stats["opportunities_seen"] += len(evaluations)
                for evaluation in evaluations:
                    self.repo.persist_evaluation(evaluation)
                    if not evaluation.generated:
                        _bump_reason(reason_counts, _normalize_no_trade_reason(evaluation.reason, "kalshi_event"))
                signals.extend(group_signals)
        if self.settings.app_mode in {"balanced", "aggressive"} and not external.stale:
            for market in markets:
                mapped_probability, mapping_metadata = map_market_probability(market, external.distributions)
                try:
                    orderbook = self.market_data.refresh_orderbook(market.ticker)
                except Exception:
                    _bump_reason(reason_counts, "data_failure")
                    continue
                cross_signals, evaluations = self.cross_market.evaluate(market, mapped_probability, orderbook, equity_reference)
                cycle_evaluations.extend(evaluations)
                stats["opportunities_seen"] += len(evaluations)
                for evaluation in evaluations:
                    evaluation.metadata.update(mapping_metadata)
                    self.repo.persist_evaluation(evaluation)
                    if not evaluation.generated:
                        _bump_reason(reason_counts, _normalize_no_trade_reason(evaluation.reason, "kalshi_event"))
                signals.extend(cross_signals)
        stats["signals_generated"] = len(signals)
        stats["signals_attempted"] = len(signals)
        if not signals and not reason_counts:
            _bump_reason(reason_counts, "edge_below_threshold")
        for signal in signals:
            self.execution_router.execute_signal(signal, extra_source_metadata={"sleeve_name": "kalshi_event"})
        stats["no_trade_reasons"] = dict(reason_counts)
        summary = _summarize_kalshi_evaluations(cycle_evaluations)
        stats["edge_summary"] = {
            "avg_expected_edge": summary["avg_expected_edge"],
            "max_expected_edge": summary["max_expected_edge"],
            "median_expected_edge": summary["median_expected_edge"],
            "best_edge_seen": summary["best_edge_seen"],
        }
        stats["top_candidates_nearest_to_trade"] = summary["top_candidates_nearest_to_trade"]
        return stats


def _dashboard_state(*, settings: Settings, sleeve_repo: SleeveRepository) -> dict[str, object]:
    statuses = sleeve_repo.load_runtime_statuses()
    doctrine = doctrine_summary()
    sleeves = []
    comparison = []
    for row in statuses:
        details = json.loads(row["details"]) if row.get("details") else {}
        no_trade_reasons = json.loads(row["no_trade_reasons"]) if row.get("no_trade_reasons") else {}
        metrics = sleeve_repo.load_recent_metrics(str(row["sleeve_name"]), limit=200)
        recent_trades = sleeve_repo.load_trade_ledger(sleeve_name=str(row["sleeve_name"]), limit=50)
        recent_errors = sleeve_repo.load_recent_errors(sleeve_name=str(row["sleeve_name"]), limit=25)
        latest = metrics[-1] if metrics else {}
        previous = metrics[-2] if len(metrics) >= 2 else latest
        current_total = to_decimal(latest.get("realized_pnl_decimal") or "0") + to_decimal(latest.get("unrealized_pnl_decimal") or "0")
        previous_total = to_decimal(previous.get("realized_pnl_decimal") or "0") + to_decimal(previous.get("unrealized_pnl_decimal") or "0")
        sleeves.append(
            {
                "sleeve_name": row["sleeve_name"],
                "role": doctrine["sleeves"].get(str(row["sleeve_name"]), {}).get("role"),
                "mandate": doctrine["sleeves"].get(str(row["sleeve_name"]), {}).get("mandate"),
                "scale_profile": doctrine["sleeves"].get(str(row["sleeve_name"]), {}).get("scale_profile"),
                "promotion_gate": doctrine["sleeves"].get(str(row["sleeve_name"]), {}).get("promotion_gate"),
                "maturity": row["maturity"],
                "run_mode": row["run_mode"],
                "status": row["status"],
                "capital_assigned": row["capital_assigned_decimal"],
                "daily_pnl": format(current_total - previous_total, "f"),
                "cumulative_pnl": format(current_total, "f"),
                "opportunities_seen": int(row["opportunities_seen"]),
                "trades_attempted": int(row["trades_attempted"]),
                "trades_filled": int(row["trades_filled"]),
                "open_positions": int(row["open_positions"]),
                "trade_count": int(row["trade_count"]),
                "error_count": int(row["error_count"]),
                "realized_pnl": row["realized_pnl_decimal"],
                "unrealized_pnl": row["unrealized_pnl_decimal"],
                "fees": row["fees_decimal"],
                "avg_slippage_bps": row["avg_slippage_bps_decimal"],
                "fill_rate": row["fill_rate_decimal"],
                "avg_expected_edge": row["avg_expected_edge_decimal"],
                "avg_realized_edge": row["avg_realized_edge_decimal"],
                "expected_realized_gap": row["expected_realized_gap_decimal"],
                "no_trade_reasons": no_trade_reasons,
                "top_no_trade_reasons": _top_reason_pairs(no_trade_reasons),
                "edge_summary": details.get("edge_summary", {}),
                "top_candidates_nearest_to_trade": details.get("top_candidates_nearest_to_trade", []),
                "blocker_notes": details.get("blocker_notes", []),
                "recent_trades": recent_trades,
                "recent_errors": recent_errors,
                "pnl_curve": metrics[-50:],
            }
        )
        comparison.append(
            {
                "sleeve_name": row["sleeve_name"],
                "role": doctrine["sleeves"].get(str(row["sleeve_name"]), {}).get("role"),
                "run_mode": row["run_mode"],
                "capital_assigned": row["capital_assigned_decimal"],
                "cumulative_pnl": format(current_total, "f"),
                "opportunities_seen": int(row["opportunities_seen"]),
                "trades_attempted": int(row["trades_attempted"]),
                "trades_filled": int(row["trades_filled"]),
                "win_rate_proxy": row["fill_rate_decimal"],
                "avg_expected_edge": row["avg_expected_edge_decimal"],
                "avg_realized_edge": row["avg_realized_edge_decimal"],
                "expected_realized_gap": row["expected_realized_gap_decimal"],
                "fees": row["fees_decimal"],
                "avg_slippage_bps": row["avg_slippage_bps_decimal"],
                "fill_rate": row["fill_rate_decimal"],
                "drawdown_proxy": _drawdown(metrics),
                "capital_efficiency_proxy": _capital_efficiency(current_total, row["capital_assigned_decimal"]),
                "top_no_trade_reasons": _top_reason_pairs(no_trade_reasons),
                "edge_summary": details.get("edge_summary", {}),
                "top_candidates_nearest_to_trade": details.get("top_candidates_nearest_to_trade", []),
                "error_count": int(row["error_count"]),
            }
        )
    run_mode = _overall_run_mode(sleeves)
    ledger = sleeve_repo.load_trade_ledger(limit=500)
    ledger_explanation = None
    if not ledger:
        blockers = [f"{sleeve['sleeve_name']}:{'/'.join(item['reason'] for item in sleeve['top_no_trade_reasons']) or sleeve['status']}" for sleeve in sleeves]
        ledger_explanation = "No trade ledger rows yet. " + (" | ".join(blockers) if blockers else "No sleeves have produced attempted trades.")
    return {
        "runtime_name": "two_sleeve_paper",
        "run_mode": run_mode,
        "updated_at_utc": datetime.now(timezone.utc).isoformat(),
        "master_thesis": doctrine["master_thesis"],
        "benchmark_question": "What can this machine do that a simple SPY/QQQ allocator cannot?",
        "operating_hierarchy": doctrine["operating_hierarchy"],
        "sleeves": sleeves,
        "comparison": comparison,
        "ledger": ledger,
        "ledger_explanation": ledger_explanation,
        "errors": sleeve_repo.load_recent_errors(limit=100),
        "disabled_modules": [name for name, definition in SLEEVE_REGISTRY.items() if not definition.enabled_by_default],
        "control_panel_url": f"http://{settings.control_panel_host}:{settings.control_panel_port}/",
    }


def _drawdown(metrics: list[dict]) -> str:
    peak = Decimal("0")
    worst = Decimal("0")
    for row in metrics:
        total = to_decimal(row.get("realized_pnl_decimal") or "0") + to_decimal(row.get("unrealized_pnl_decimal") or "0")
        peak = max(peak, total)
        worst = max(worst, peak - total)
    return format(worst, "f")


def _capital_efficiency(total: Decimal, capital_assigned: str | None) -> str:
    capital = to_decimal(capital_assigned or "0")
    if capital <= 0:
        return "0"
    return format(total / capital, "f")


def _write_measurement_report(
    *,
    settings: Settings,
    state: dict[str, object],
    started_at: datetime,
    ended_at: datetime,
    cycles_completed: int,
) -> dict[str, object]:
    doctrine = doctrine_summary()
    report = {
        "runtime_name": "two_sleeve_paper",
        "run_mode": state.get("run_mode", "data_unavailable"),
        "runtime_started_at_utc": started_at.isoformat(),
        "runtime_ended_at_utc": ended_at.isoformat(),
        "cycles_completed": cycles_completed,
        "master_thesis": doctrine["master_thesis"],
        "benchmark_question": "What can this machine do that a simple SPY/QQQ allocator cannot?",
        "operating_hierarchy": doctrine["operating_hierarchy"],
        "sleeves": state.get("sleeves", []),
        "comparison": state.get("comparison", []),
        "ledger_rows": len(state.get("ledger", [])),
        "ledger_explanation": state.get("ledger_explanation"),
        "errors": state.get("errors", []),
    }
    settings.first_measurement_report_path.write_text(json.dumps(report, indent=2), encoding="utf-8")
    return report


def _write_strategy_transparency_pack(*, settings: Settings, state: dict[str, object]) -> dict[str, object]:
    doctrine = doctrine_summary()
    pack = {
        "runtime_name": "two_sleeve_paper",
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "master_thesis": doctrine["master_thesis"],
        "benchmark_question": "What can this machine do that a simple SPY/QQQ allocator cannot?",
        "what_the_system_does": (
            "Runs a paper-first two-sleeve trading system: a cross-asset trend core and a Kalshi/event booster, "
            "with explicit measurement, structured logs, and operator-visible assumptions."
        ),
        "commercial_wedge": "paper_trading_plus_alerts_plus_verification",
        "trust_surfaces": [
            "strategy_transparency_pack",
            "first_measurement_report",
            "multi_cycle_research_report",
            "control_panel_state",
            "runtime_status",
        ],
        "assumptions": {
            "app_mode": settings.app_mode,
            "live_trading": settings.live_trading,
            "total_paper_capital": settings.total_paper_capital,
            "trend_capital_weight_default": settings.trend_capital_weight_default,
            "kalshi_capital_weight_default": settings.kalshi_capital_weight_default,
            "trend_data_mode": settings.trend_data_mode,
            "trend_weight_mode": settings.trend_weight_mode,
            "trend_symbols": settings.trend_symbols,
            "kalshi_data_mode": settings.kalshi_data_mode,
            "slippage_buffer_bps": settings.slippage_buffer_bps,
            "min_edge_bps": settings.min_edge_bps,
            "min_edge": settings.min_edge,
            "min_depth": settings.min_depth,
        },
        "operator_disclosures": {
            "human_oversight_required": True,
            "live_order_submission_default": False,
            "data_tier_constraints_must_be_understood": True,
            "paper_validation_is_not_live_execution_proof": True,
        },
        "sleeves": [
            {
                "sleeve_name": sleeve.get("sleeve_name"),
                "role": sleeve.get("role"),
                "maturity": sleeve.get("maturity"),
                "run_mode": sleeve.get("run_mode"),
                "status": sleeve.get("status"),
                "mandate": sleeve.get("mandate"),
                "scale_profile": sleeve.get("scale_profile"),
                "promotion_gate": sleeve.get("promotion_gate"),
            }
            for sleeve in state.get("sleeves", [])
        ],
        "disabled_modules": state.get("disabled_modules", []),
        "artifact_paths": {
            "paper_report_path": str(settings.report_path),
            "first_measurement_report_path": str(settings.first_measurement_report_path),
            "multi_cycle_research_report_path": str(settings.multi_cycle_research_report_path),
            "dashboard_state_path": str(settings.dashboard_state_path),
            "runtime_status_path": str(settings.runtime_status_path),
        },
    }
    settings.strategy_transparency_pack_path.write_text(json.dumps(pack, indent=2), encoding="utf-8")
    return pack


def _build_multi_cycle_research_summary(*, sleeve_repo: SleeveRepository, cycles_completed: int) -> dict[str, object]:
    sleeves_summary = []
    for row in sleeve_repo.load_runtime_statuses():
        sleeve_name = str(row["sleeve_name"])
        metrics = sleeve_repo.load_recent_metrics(sleeve_name, limit=max(cycles_completed, 1))
        if not metrics:
            continue
        opportunity_values = [int(item["opportunities_seen"]) for item in metrics]
        avg_expected_values = [to_decimal(item["avg_expected_edge_decimal"]) for item in metrics if item.get("avg_expected_edge_decimal") is not None]
        avg_realized_values = [to_decimal(item["avg_realized_edge_decimal"]) for item in metrics if item.get("avg_realized_edge_decimal") is not None]
        gap_values = [to_decimal(item["expected_realized_gap_decimal"]) for item in metrics if item.get("expected_realized_gap_decimal") is not None]
        rejection_counts: dict[str, int] = defaultdict(int)
        candidate_rollup: dict[tuple[str, str, str], dict[str, object]] = {}
        family_rollup: dict[str, dict[str, object]] = {}
        best_edge_seen: Decimal | None = None
        earliest_created_at = str(metrics[0]["created_at"]) if metrics else None
        for metric in metrics:
            reasons = json.loads(metric["no_trade_reasons"]) if metric.get("no_trade_reasons") else {}
            for reason, count in reasons.items():
                _bump_reason(rejection_counts, str(reason), int(count))
            details = json.loads(metric["details"]) if metric.get("details") else {}
            edge_summary = details.get("edge_summary", {})
            if edge_summary.get("best_edge_seen") is not None:
                edge_value = to_decimal(edge_summary["best_edge_seen"])
                best_edge_seen = edge_value if best_edge_seen is None else max(best_edge_seen, edge_value)
            for candidate in details.get("top_candidates_nearest_to_trade", []):
                family = _kalshi_family_from_ticker(str(candidate.get("ticker", "")))
                key = (str(candidate.get("ticker")), str(candidate.get("strategy")), str(candidate.get("reason")))
                edge_after_fees = to_decimal(candidate.get("edge_after_fees", 0))
                existing = candidate_rollup.get(key)
                if existing is None:
                    candidate_rollup[key] = {
                        "ticker": key[0],
                        "strategy": key[1],
                        "reason": key[2],
                        "appearances": 1,
                        "best_edge_after_fees": edge_after_fees,
                        "last_quantity": int(candidate.get("quantity", 0)),
                    }
                else:
                    existing["appearances"] = int(existing["appearances"]) + 1
                    existing["best_edge_after_fees"] = max(to_decimal(existing["best_edge_after_fees"]), edge_after_fees)
                    existing["last_quantity"] = int(candidate.get("quantity", 0))
        dominant_reason = _top_reason_pairs(rejection_counts, limit=1)
        top_candidates = sorted(
            candidate_rollup.values(),
            key=lambda item: (-int(item["appearances"]), -to_decimal(item["best_edge_after_fees"]), str(item["ticker"])),
        )[:3]
        if sleeve_name == "kalshi_event" and earliest_created_at is not None:
            evaluation_rows = sleeve_repo.db.fetchall(
                """
                SELECT strategy, ticker, reason, edge_after_fees_decimal
                FROM strategy_evaluations
                WHERE created_at >= ?
                ORDER BY id ASC
                """,
                (earliest_created_at,),
            )
            for eval_row in evaluation_rows:
                strategy_name = str(eval_row["strategy"] or "")
                if sleeve_name_for_strategy(strategy_name) != "kalshi_event":
                    continue
                family = _kalshi_family_from_ticker(str(eval_row["ticker"] or ""))
                edge_after_fees = to_decimal(eval_row["edge_after_fees_decimal"] or 0)
                family_entry = family_rollup.setdefault(
                    family,
                    {
                        "family": family,
                        "evaluation_count": 0,
                        "best_edge_seen": None,
                        "top_candidate_ticker": None,
                        "reasons": defaultdict(int),
                    },
                )
                family_entry["evaluation_count"] = int(family_entry["evaluation_count"]) + 1
                if family_entry["best_edge_seen"] is None or edge_after_fees > to_decimal(family_entry["best_edge_seen"]):
                    family_entry["best_edge_seen"] = edge_after_fees
                    family_entry["top_candidate_ticker"] = str(eval_row["ticker"] or "")
                _bump_reason(family_entry["reasons"], str(eval_row["reason"] or "unknown"))
        family_summary = sorted(
            (
                {
                    "family": item["family"],
                    "evaluation_count": item.get("evaluation_count", 0),
                    "best_edge_seen": None if item["best_edge_seen"] is None else format(to_decimal(item["best_edge_seen"]), "f"),
                    "top_candidate_ticker": item["top_candidate_ticker"],
                    "dominant_reason": _top_reason_pairs(dict(item["reasons"]), limit=1)[0] if item["reasons"] else None,
                    "reason_counts": dict(item["reasons"]),
                }
                for item in family_rollup.values()
            ),
            key=lambda item: (-int(item["evaluation_count"]), -(to_decimal(item["best_edge_seen"]) if item["best_edge_seen"] is not None else Decimal("0")), item["family"]),
        )
        sleeves_summary.append(
            {
                "sleeve_name": sleeve_name,
                "cycles_observed": len(metrics),
                "avg_opportunities_seen": format(Decimal(sum(opportunity_values)) / Decimal(len(opportunity_values)), "f"),
                "avg_expected_edge_over_cycles": None if not avg_expected_values else format(_mean(avg_expected_values), "f"),
                "avg_realized_edge_over_cycles": None if not avg_realized_values else format(_mean(avg_realized_values), "f"),
                "avg_expected_realized_gap_over_cycles": None if not gap_values else format(_mean(gap_values), "f"),
                "best_edge_seen_over_cycles": None if best_edge_seen is None else format(best_edge_seen, "f"),
                "rejection_reason_counts": dict(rejection_counts),
                "dominant_rejection_reason": dominant_reason[0] if dominant_reason else None,
                "family_rollup": family_summary[:5],
                "bill_focus": _build_bill_focus_summary(sleeve_repo=sleeve_repo, earliest_created_at=earliest_created_at) if sleeve_name == "kalshi_event" else None,
                "trend_focus": _build_trend_focus_summary(metrics) if sleeve_name == "trend" else None,
                "trend_sparsity": _build_trend_sparsity_summary(metrics) if sleeve_name == "trend" else None,
                "trend_construction": _build_trend_construction_summary(metrics) if sleeve_name == "trend" else None,
                "trend_validation": _build_trend_validation_summary(metrics) if sleeve_name == "trend" else None,
                "trend_signal": _build_trend_signal_summary(metrics) if sleeve_name == "trend" else None,
                "trend_benchmark": _build_trend_benchmark_summary(metrics) if sleeve_name == "trend" else None,
                "top_repeated_near_trades": [
                    {
                        "ticker": item["ticker"],
                        "strategy": item["strategy"],
                        "reason": item["reason"],
                        "appearances": item["appearances"],
                        "best_edge_after_fees": format(to_decimal(item["best_edge_after_fees"]), "f"),
                        "last_quantity": item["last_quantity"],
                    }
                    for item in top_candidates
                ],
            }
        )
    return {"sleeves": sleeves_summary}


def _write_multi_cycle_research_report(*, settings: Settings, sleeve_repo: SleeveRepository, started_at: datetime, ended_at: datetime, cycles_completed: int) -> dict[str, object]:
    doctrine = doctrine_summary()
    report = {
        "runtime_name": "two_sleeve_paper",
        "runtime_started_at_utc": started_at.isoformat(),
        "runtime_ended_at_utc": ended_at.isoformat(),
        "cycles_completed": cycles_completed,
        "master_thesis": doctrine["master_thesis"],
        "benchmark_question": "What can this machine do that a simple SPY/QQQ allocator cannot?",
        "operating_hierarchy": doctrine["operating_hierarchy"],
    }
    report.update(_build_multi_cycle_research_summary(sleeve_repo=sleeve_repo, cycles_completed=cycles_completed))
    settings.multi_cycle_research_report_path.write_text(json.dumps(report, indent=2), encoding="utf-8")
    return report


def _extract_trend_replay_diversity_metrics(report: dict[str, object]) -> dict[str, object] | None:
    for sleeve in report.get("sleeves", []):
        if sleeve.get("sleeve_name") != "trend":
            continue
        trend_focus = sleeve.get("trend_focus") or {}
        trend_sparsity = sleeve.get("trend_sparsity") or {}
        trend_construction = sleeve.get("trend_construction") or {}
        trend_signal = sleeve.get("trend_signal") or {}
        trend_benchmark = sleeve.get("trend_benchmark") or {}
        return {
            "cycles_observed": sleeve.get("cycles_observed"),
            "signal_cycle_rate": trend_focus.get("signal_cycle_rate"),
            "avg_targets_generated": trend_focus.get("avg_targets_generated"),
            "avg_signals_generated": trend_focus.get("avg_signals_generated"),
            "avg_target_weight_change_l1": trend_sparsity.get("avg_target_weight_change_l1"),
            "avg_target_name_turnover_rate": trend_sparsity.get("avg_target_name_turnover_rate"),
            "material_input_advancement_rate": trend_sparsity.get("material_input_advancement_rate"),
            "cycles_with_material_input_advancement": trend_sparsity.get("cycles_with_material_input_advancement"),
            "cycles_with_identical_latest_bar_dates": trend_sparsity.get("cycles_with_identical_latest_bar_dates"),
            "sparsity_interpretation": trend_sparsity.get("interpretation"),
            "sparsity_interpretation_reason": trend_sparsity.get("interpretation_reason"),
            "avg_positive_momentum_count": trend_construction.get("avg_positive_momentum_count"),
            "avg_capped_target_count": trend_construction.get("avg_capped_target_count"),
            "avg_scaling_factor": trend_construction.get("avg_scaling_factor"),
            "avg_top_weight_share": trend_construction.get("avg_top_weight_share"),
            "avg_effective_symbol_count": trend_construction.get("avg_effective_symbol_count"),
            "cycles_with_max_weight_binding": trend_construction.get("cycles_with_max_weight_binding"),
            "stable_positive_core_rate": trend_construction.get("stable_positive_core_rate"),
            "construction_interpretation": trend_construction.get("interpretation"),
            "construction_interpretation_reason": trend_construction.get("interpretation_reason"),
            "validation_interpretation": (sleeve.get("trend_validation") or {}).get("interpretation"),
            "validation_interpretation_reason": (sleeve.get("trend_validation") or {}).get("interpretation_reason"),
            "signal_interpretation": trend_signal.get("interpretation"),
            "signal_interpretation_reason": trend_signal.get("interpretation_reason"),
            "benchmark_interpretation": trend_benchmark.get("interpretation"),
            "benchmark_interpretation_reason": trend_benchmark.get("interpretation_reason"),
            "benchmark_strategy_cumulative_return": trend_benchmark.get("strategy_cumulative_return"),
            "benchmark_spy_qqq_excess_return": (trend_benchmark.get("spy_qqq_equal_weight") or {}).get("strategy_excess_return"),
            "benchmark_universe_excess_return": (trend_benchmark.get("trend_universe_equal_weight") or {}).get("strategy_excess_return"),
        }
    return None


def _build_trend_replay_diversity_report(*, scenarios: list[dict[str, object]]) -> dict[str, object]:
    scenario_rows = []
    for scenario in scenarios:
        metrics = _extract_trend_replay_diversity_metrics(scenario["report"]) or {}
        scenario_rows.append(
            {
                "scenario_name": scenario["scenario_name"],
                "description": scenario["description"],
                "trend_bootstrap_variant": scenario["trend_bootstrap_variant"],
                "trend_replay_advance_mode": scenario["trend_replay_advance_mode"],
                "trend_summary": metrics,
                "weight_shape_sensitivity": scenario.get("weight_shape_sensitivity"),
            }
        )
    baseline = next((row for row in scenario_rows if row["scenario_name"] == "cached_bootstrap_static"), None)
    shifted = next((row for row in scenario_rows if row["scenario_name"] == "cached_bootstrap_shifted"), None)
    interpretation = "insufficient_data"
    interpretation_reason = "missing_static_or_shifted_trend_summaries"
    if baseline is not None and shifted is not None:
        base_summary = baseline["trend_summary"]
        shifted_summary = shifted["trend_summary"]
        baseline_advancement = to_decimal(base_summary.get("material_input_advancement_rate") or "0")
        shifted_advancement = to_decimal(shifted_summary.get("material_input_advancement_rate") or "0")
        baseline_signal_rate = to_decimal(base_summary.get("signal_cycle_rate") or "0")
        shifted_signal_rate = to_decimal(shifted_summary.get("signal_cycle_rate") or "0")
        baseline_drift = to_decimal(base_summary.get("avg_target_weight_change_l1") or "0")
        shifted_drift = to_decimal(shifted_summary.get("avg_target_weight_change_l1") or "0")
        baseline_turnover = to_decimal(base_summary.get("avg_target_name_turnover_rate") or "0")
        shifted_turnover = to_decimal(shifted_summary.get("avg_target_name_turnover_rate") or "0")
        shifted_capped = to_decimal(shifted_summary.get("avg_capped_target_count") or "0")
        shifted_positive = to_decimal(shifted_summary.get("avg_positive_momentum_count") or "0")
        if shifted_advancement > baseline_advancement and (shifted_drift > baseline_drift or shifted_turnover > baseline_turnover or shifted_signal_rate > baseline_signal_rate):
            interpretation = "replay_diversity_unlocks_trend_drift"
            interpretation_reason = "advancing_cached_inputs_increase_target_drift_turnover_or_signal_rate"
        elif shifted_advancement > baseline_advancement and shifted_capped >= Decimal("2") and shifted_positive <= Decimal("2.2"):
            interpretation = "inputs_advance_but_construction_remains_saturated"
            interpretation_reason = "advancing_replay_changes_bars_but_positive_universe_and_max_weight_binding_stay_tight"
        else:
            interpretation = "trend_stays_sparse_even_when_replay_inputs_advance"
            interpretation_reason = "input_advancement_does_not_materially_raise_drift_turnover_or_signal_rate"
    return {
        "runtime_name": "trend_replay_diversity_check",
        "scenarios": scenario_rows,
        "interpretation": interpretation,
        "interpretation_reason": interpretation_reason,
    }


def _extract_trend_threshold_sensitivity_metrics(report: dict[str, object]) -> dict[str, object] | None:
    for sleeve in report.get("sleeves", []):
        if sleeve.get("sleeve_name") != "trend":
            continue
        trend_focus = sleeve.get("trend_focus") or {}
        trend_sparsity = sleeve.get("trend_sparsity") or {}
        trend_validation = sleeve.get("trend_validation") or {}
        trend_benchmark = sleeve.get("trend_benchmark") or {}
        return {
            "signal_cycle_rate": trend_focus.get("signal_cycle_rate"),
            "trade_cycle_rate": trend_validation.get("trade_cycle_rate"),
            "avg_target_weight_change_l1": trend_sparsity.get("avg_target_weight_change_l1"),
            "avg_threshold_suppressed_notional_ratio": trend_sparsity.get("avg_threshold_suppressed_notional_ratio"),
            "near_target_rate": trend_sparsity.get("near_target_rate"),
            "validation_interpretation": trend_validation.get("interpretation"),
            "benchmark_interpretation": trend_benchmark.get("interpretation"),
            "benchmark_strategy_cumulative_return": trend_benchmark.get("strategy_cumulative_return"),
            "benchmark_spy_qqq_excess_return": (trend_benchmark.get("spy_qqq_equal_weight") or {}).get("strategy_excess_return"),
            "benchmark_spy_qqq_outperformance_rate": (trend_benchmark.get("spy_qqq_equal_weight") or {}).get("interval_outperformance_rate"),
            "benchmark_universe_excess_return": (trend_benchmark.get("trend_universe_equal_weight") or {}).get("strategy_excess_return"),
        }
    return None


def _build_trend_threshold_sensitivity_report(*, scenarios: list[dict[str, object]]) -> dict[str, object]:
    scenario_rows = []
    best_row: dict[str, object] | None = None
    for scenario in scenarios:
        metrics = _extract_trend_threshold_sensitivity_metrics(scenario["report"]) or {}
        row = {
            "scenario_name": scenario["scenario_name"],
            "description": scenario["description"],
            "trend_min_rebalance_fraction": format(to_decimal(scenario["trend_min_rebalance_fraction"]), "f"),
            "trend_summary": metrics,
        }
        scenario_rows.append(row)
        if best_row is None:
            best_row = row
        else:
            current_excess = to_decimal((row["trend_summary"] or {}).get("benchmark_spy_qqq_excess_return") or "-999")
            best_excess = to_decimal((best_row["trend_summary"] or {}).get("benchmark_spy_qqq_excess_return") or "-999")
            current_outperformance = to_decimal((row["trend_summary"] or {}).get("benchmark_spy_qqq_outperformance_rate") or "0")
            best_outperformance = to_decimal((best_row["trend_summary"] or {}).get("benchmark_spy_qqq_outperformance_rate") or "0")
            if current_excess > best_excess or (current_excess == best_excess and current_outperformance > best_outperformance):
                best_row = row
    interpretation = "insufficient_data"
    interpretation_reason = "missing_threshold_sensitivity_scenarios"
    if len(scenario_rows) >= 2 and best_row is not None:
        base_row = scenario_rows[0]
        base_excess = to_decimal((base_row["trend_summary"] or {}).get("benchmark_spy_qqq_excess_return") or "0")
        best_excess = to_decimal((best_row["trend_summary"] or {}).get("benchmark_spy_qqq_excess_return") or "0")
        base_threshold = to_decimal(base_row["trend_min_rebalance_fraction"])
        best_threshold = to_decimal(best_row["trend_min_rebalance_fraction"])
        if best_threshold < base_threshold and best_excess > base_excess + Decimal("0.0025"):
            interpretation = "lower_rebalance_threshold_improves_benchmark_posture"
            interpretation_reason = "reduced_min_rebalance_fraction_improves_spy_qqq_excess_return_on_shifted_replay"
        elif best_excess <= base_excess + Decimal("0.0025"):
            interpretation = "threshold_changes_do_not_materially_improve_benchmark_posture"
            interpretation_reason = "rebalance_sensitivity_changes_do_not_meaningfully_close_the_passive_benchmark_gap"
        else:
            interpretation = "mixed_threshold_tradeoff"
            interpretation_reason = "lower_threshold_changes_trading_activity_but_not_cleanly_enough_to_call_it_better"
    return {
        "runtime_name": "trend_threshold_sensitivity_check",
        "scenarios": scenario_rows,
        "interpretation": interpretation,
        "interpretation_reason": interpretation_reason,
    }


def _weight_shape_current(*, momentum: Decimal, realized_vol: Decimal, target_vol: Decimal, max_weight: Decimal) -> Decimal:
    return target_weight_from_signal(
        momentum=momentum,
        realized_vol=realized_vol,
        target_vol=target_vol,
        max_weight=max_weight,
    )


def _weight_shape_soft_monotone(*, momentum: Decimal, realized_vol: Decimal, target_vol: Decimal, max_weight: Decimal) -> Decimal:
    if momentum <= 0 or realized_vol <= 0:
        return Decimal("0")
    base = target_vol / realized_vol
    strength = momentum / (momentum + Decimal("0.05"))
    return min(max_weight, base * strength)


def _weight_shape_rank_normalized(
    *,
    positive_rows: list[dict[str, Decimal]],
    max_weight: Decimal,
    max_gross_exposure: Decimal,
) -> dict[str, Decimal]:
    if not positive_rows:
        return {}
    scored: list[tuple[str, Decimal]] = []
    for row in positive_rows:
        score = row["momentum"] / max(row["realized_vol"], Decimal("0.0001"))
        if score > 0:
            scored.append((row["symbol"], score))
    total_score = sum((score for _, score in scored), Decimal("0"))
    if total_score <= 0:
        return {}
    weights: dict[str, Decimal] = {}
    remaining = max_gross_exposure
    for symbol, score in sorted(scored, key=lambda item: (-item[1], item[0])):
        raw = max_gross_exposure * (score / total_score)
        clipped = min(max_weight, raw, remaining)
        weights[symbol] = clipped
        remaining -= clipped
    return weights


def _weight_shape_gentle_score(
    *,
    positive_rows: list[dict[str, Decimal]],
    max_weight: Decimal,
    max_gross_exposure: Decimal,
) -> dict[str, Decimal]:
    if not positive_rows:
        return {}
    scored: list[tuple[str, Decimal]] = []
    for row in positive_rows:
        score = row["momentum"].sqrt() / max(row["realized_vol"].sqrt(), Decimal("0.01"))
        if score > 0:
            scored.append((row["symbol"], score))
    total_score = sum((score for _, score in scored), Decimal("0"))
    if total_score <= 0:
        return {}
    weights: dict[str, Decimal] = {}
    for symbol, score in scored:
        raw = max_gross_exposure * (score / total_score)
        weights[symbol] = min(max_weight, raw)
    gross = sum(weights.values(), Decimal("0"))
    if gross > max_gross_exposure and gross > 0:
        scale = max_gross_exposure / gross
        weights = {symbol: weight * scale for symbol, weight in weights.items()}
    return weights


def _effective_symbol_count(weights: dict[str, Decimal]) -> Decimal:
    gross = sum(weights.values(), Decimal("0"))
    if gross <= 0:
        return Decimal("0")
    squared_sum = sum((weight * weight for weight in weights.values()), Decimal("0"))
    if squared_sum <= 0:
        return Decimal("0")
    return (gross * gross) / squared_sum


def _simulate_weight_shape_cycle(
    *,
    construction_rows: list[dict[str, object]],
    current_weights: dict[str, Decimal],
    mode: str,
    target_vol: Decimal,
    max_weight: Decimal,
    max_gross_exposure: Decimal,
    min_rebalance_fraction: Decimal,
) -> dict[str, Decimal | int]:
    positive_rows: list[dict[str, Decimal]] = []
    raw_weights: dict[str, Decimal] = {}
    capped_count = 0
    for row in construction_rows:
        momentum_raw = row.get("momentum_return")
        realized_vol_raw = row.get("realized_volatility")
        if momentum_raw is None or realized_vol_raw is None:
            continue
        momentum = to_decimal(momentum_raw)
        realized_vol = to_decimal(realized_vol_raw)
        symbol = str(row["symbol"])
        if momentum > 0 and realized_vol > 0:
            positive_rows.append({"symbol": symbol, "momentum": momentum, "realized_vol": realized_vol})
            if mode == "current":
                raw = _weight_shape_current(momentum=momentum, realized_vol=realized_vol, target_vol=target_vol, max_weight=max_weight)
            elif mode == "soft_monotone":
                raw = _weight_shape_soft_monotone(momentum=momentum, realized_vol=realized_vol, target_vol=target_vol, max_weight=max_weight)
            else:
                raw = Decimal("0")
            if mode in {"current", "soft_monotone"}:
                raw_weights[symbol] = raw
                if raw >= max_weight:
                    capped_count += 1
    if mode == "rank_normalized":
        raw_weights = _weight_shape_rank_normalized(positive_rows=positive_rows, max_weight=max_weight, max_gross_exposure=max_gross_exposure)
        capped_count = sum(1 for weight in raw_weights.values() if weight >= max_weight)
    elif mode == "gentle_score":
        raw_weights = _weight_shape_gentle_score(positive_rows=positive_rows, max_weight=max_weight, max_gross_exposure=max_gross_exposure)
        capped_count = sum(1 for weight in raw_weights.values() if weight >= max_weight)
    gross_raw = sum(raw_weights.values(), Decimal("0"))
    scale = Decimal("1")
    final_weights = dict(raw_weights)
    if gross_raw > max_gross_exposure and gross_raw > 0:
        scale = max_gross_exposure / gross_raw
        final_weights = {symbol: weight * scale for symbol, weight in raw_weights.items()}
    union_symbols = set(final_weights) | set(current_weights)
    diffs = [abs(final_weights.get(symbol, Decimal("0")) - current_weights.get(symbol, Decimal("0"))) for symbol in union_symbols]
    distance_l1 = sum(diffs, Decimal("0"))
    threshold_suppressed = Decimal("0")
    near_miss = 0
    for symbol in union_symbols:
        target_weight = abs(final_weights.get(symbol, Decimal("0")))
        current_weight = abs(current_weights.get(symbol, Decimal("0")))
        scale_base = max(target_weight, current_weight, Decimal("0.000001"))
        rebalance_fraction = abs(final_weights.get(symbol, Decimal("0")) - current_weights.get(symbol, Decimal("0"))) / scale_base
        if abs(final_weights.get(symbol, Decimal("0")) - current_weights.get(symbol, Decimal("0"))) > 0 and rebalance_fraction < min_rebalance_fraction:
            threshold_suppressed += abs(final_weights.get(symbol, Decimal("0")) - current_weights.get(symbol, Decimal("0")))
            if rebalance_fraction >= min_rebalance_fraction * Decimal("0.8"):
                near_miss = 1
    top_share = max(final_weights.values()) if final_weights else Decimal("0")
    return {
        "positive_count": Decimal(len(positive_rows)),
        "capped_count": Decimal(capped_count),
        "gross_raw_target_weight": gross_raw,
        "scaling_factor": scale,
        "top_weight_share": top_share,
        "effective_symbol_count": _effective_symbol_count(final_weights),
        "target_weight_change_l1": distance_l1,
        "threshold_suppressed_ratio": threshold_suppressed,
        "near_miss": Decimal(near_miss),
    }


def _build_trend_weight_shape_sensitivity_report(
    *,
    sleeve_repo: SleeveRepository,
    cycles_completed: int,
    target_vol: Decimal,
    max_weight: Decimal,
    max_gross_exposure: Decimal,
    min_rebalance_fraction: Decimal,
) -> dict[str, object]:
    metrics = sleeve_repo.load_recent_metrics("trend", limit=max(cycles_completed, 1))
    if not metrics:
        return {"interpretation": "insufficient_data", "interpretation_reason": "missing_trend_metrics_for_weight_shape_check"}
    mode_rollups: dict[str, list[dict[str, Decimal | int]]] = {
        "current": [],
        "soft_monotone": [],
        "rank_normalized": [],
        "gentle_score": [],
    }
    for metric in metrics:
        details = json.loads(metric["details"]) if metric.get("details") else {}
        cycle = details.get("trend_cycle_diagnostics") or {}
        construction = cycle.get("construction") or {}
        rows = construction.get("rows") or []
        current_weights = {str(symbol): to_decimal(weight) for symbol, weight in (cycle.get("current_weights") or {}).items()}
        for mode in mode_rollups:
            mode_rollups[mode].append(
                _simulate_weight_shape_cycle(
                    construction_rows=rows,
                    current_weights=current_weights,
                    mode=mode,
                    target_vol=target_vol,
                    max_weight=max_weight,
                    max_gross_exposure=max_gross_exposure,
                    min_rebalance_fraction=min_rebalance_fraction,
                )
            )
    summaries: list[dict[str, object]] = []
    current_summary: dict[str, object] | None = None
    best_alternative: dict[str, object] | None = None
    for mode, rows in mode_rollups.items():
        avg_capped = _mean([to_decimal(item["capped_count"]) for item in rows])
        avg_top_share = _mean([to_decimal(item["top_weight_share"]) for item in rows])
        avg_effective_names = _mean([to_decimal(item["effective_symbol_count"]) for item in rows])
        avg_drift = _mean([to_decimal(item["target_weight_change_l1"]) for item in rows])
        avg_threshold = _mean([to_decimal(item["threshold_suppressed_ratio"]) for item in rows])
        near_miss_rate = _mean([to_decimal(item["near_miss"]) for item in rows])
        avg_positive = _mean([to_decimal(item["positive_count"]) for item in rows])
        summary = {
            "mode": mode,
            "avg_positive_momentum_count": None if avg_positive is None else format(avg_positive, "f"),
            "avg_capped_target_count": None if avg_capped is None else format(avg_capped, "f"),
            "avg_top_weight_share": None if avg_top_share is None else format(avg_top_share, "f"),
            "avg_effective_symbol_count": None if avg_effective_names is None else format(avg_effective_names, "f"),
            "avg_target_weight_change_l1": None if avg_drift is None else format(avg_drift, "f"),
            "avg_threshold_suppressed_notional_ratio": None if avg_threshold is None else format(avg_threshold, "f"),
            "near_miss_rebalance_rate": None if near_miss_rate is None else format(near_miss_rate, "f"),
        }
        summaries.append(summary)
        if mode == "current":
            current_summary = summary
        else:
            if best_alternative is None:
                best_alternative = summary
            else:
                if (
                    to_decimal(summary["avg_capped_target_count"] or "999") < to_decimal(best_alternative["avg_capped_target_count"] or "999")
                    or (
                        to_decimal(summary["avg_capped_target_count"] or "999") == to_decimal(best_alternative["avg_capped_target_count"] or "999")
                        and to_decimal(summary["avg_effective_symbol_count"] or "0") > to_decimal(best_alternative["avg_effective_symbol_count"] or "0")
                    )
                ):
                    best_alternative = summary
    interpretation = "insufficient_data"
    interpretation_reason = "missing_current_or_alternative_weight_shape_summary"
    if current_summary is not None and best_alternative is not None:
        current_capped = to_decimal(current_summary["avg_capped_target_count"] or "0")
        current_effective = to_decimal(current_summary["avg_effective_symbol_count"] or "0")
        best_capped = to_decimal(best_alternative["avg_capped_target_count"] or "0")
        best_effective = to_decimal(best_alternative["avg_effective_symbol_count"] or "0")
        if best_capped + Decimal("0.25") < current_capped or best_effective > current_effective + Decimal("0.25"):
            interpretation = "weight_shape_is_primary_construction_lever"
            interpretation_reason = f"{best_alternative['mode']}_reduces_cap_binding_or_improves_effective_name_count"
        else:
            interpretation = "weight_shape_changes_do_not_materially_relieve_saturation"
            interpretation_reason = "construction_remains_concentrated_even_under_alternative_weight_mappings"
    return {
        "modes": summaries,
        "interpretation": interpretation,
        "interpretation_reason": interpretation_reason,
    }


def _run_trend_replay_diversity_check(settings: Settings, *, cycles: int) -> dict[str, object]:
    base_dir = settings.trend_replay_diversity_report_path.parent
    base_dir.mkdir(parents=True, exist_ok=True)
    scenario_specs = [
        {
            "scenario_name": "cached_bootstrap_static",
            "description": "cached bootstrap as-is with no forced replay advancement",
            "trend_bootstrap_variant": "smooth",
            "trend_replay_advance_mode": "static",
        },
        {
            "scenario_name": "cached_bootstrap_shifted",
            "description": "cached replay with deterministic advancing bootstrap window",
            "trend_bootstrap_variant": "diverse",
            "trend_replay_advance_mode": "shift_window",
        },
    ]
    scenario_outputs: list[dict[str, object]] = []
    for spec in scenario_specs:
        stem = spec["scenario_name"]
        scenario_settings = replace(
            settings,
            db_path=base_dir / f"{stem}.db",
            report_path=base_dir / f"{stem}_paper_report.json",
            first_measurement_report_path=base_dir / f"{stem}_first_measurement.json",
            multi_cycle_research_report_path=base_dir / f"{stem}_research.json",
            dashboard_state_path=base_dir / f"{stem}_state.json",
            enable_kalshi_sleeve=False,
            poll_interval_seconds=1,
            trend_data_mode="cached",
            trend_bootstrap_variant=spec["trend_bootstrap_variant"],
            trend_replay_advance_mode=spec["trend_replay_advance_mode"],
        )
        for path in [
            scenario_settings.db_path,
            scenario_settings.report_path,
            scenario_settings.first_measurement_report_path,
            scenario_settings.multi_cycle_research_report_path,
            scenario_settings.dashboard_state_path,
        ]:
            path.unlink(missing_ok=True)
        scenario_db = Database(scenario_settings.db_path)
        _bootstrap_trend_replay_bars(
            scenario_settings,
            db=scenario_db,
            as_of=date.today(),
            variant=scenario_settings.trend_bootstrap_variant,
        )
        run_two_sleeve_paper_runtime(scenario_settings, cycles=cycles, start_server=False)
        scenario_report = json.loads(scenario_settings.multi_cycle_research_report_path.read_text(encoding="utf-8"))
        scenario_outputs.append(
            {
                **spec,
                "report": scenario_report,
                "weight_shape_sensitivity": _build_trend_weight_shape_sensitivity_report(
                    sleeve_repo=SleeveRepository(Database(scenario_settings.db_path)),
                    cycles_completed=cycles,
                    target_vol=Decimal("0.10"),
                    max_weight=Decimal("0.35"),
                    max_gross_exposure=Decimal("1.0"),
                    min_rebalance_fraction=Decimal("0.10"),
                ),
            }
        )
    comparison_report = _build_trend_replay_diversity_report(scenarios=scenario_outputs)
    settings.trend_replay_diversity_report_path.write_text(json.dumps(comparison_report, indent=2), encoding="utf-8")
    return comparison_report


def _run_trend_threshold_sensitivity_check(settings: Settings, *, cycles: int) -> dict[str, object]:
    base_dir = settings.trend_threshold_sensitivity_report_path.parent
    base_dir.mkdir(parents=True, exist_ok=True)
    scenario_specs = [
        {
            "scenario_name": "threshold_0_10",
            "description": "shifted replay with the current 10% rebalance threshold",
            "trend_min_rebalance_fraction": Decimal("0.10"),
        },
        {
            "scenario_name": "threshold_0_05",
            "description": "shifted replay with a 5% rebalance threshold",
            "trend_min_rebalance_fraction": Decimal("0.05"),
        },
        {
            "scenario_name": "threshold_0_02",
            "description": "shifted replay with a 2% rebalance threshold",
            "trend_min_rebalance_fraction": Decimal("0.02"),
        },
    ]
    scenario_outputs: list[dict[str, object]] = []
    for spec in scenario_specs:
        stem = spec["scenario_name"]
        scenario_settings = replace(
            settings,
            db_path=base_dir / f"{stem}.db",
            report_path=base_dir / f"{stem}_paper_report.json",
            first_measurement_report_path=base_dir / f"{stem}_first_measurement.json",
            multi_cycle_research_report_path=base_dir / f"{stem}_research.json",
            dashboard_state_path=base_dir / f"{stem}_state.json",
            enable_kalshi_sleeve=False,
            poll_interval_seconds=1,
            trend_data_mode="cached",
            trend_bootstrap_variant="diverse",
            trend_replay_advance_mode="shift_window",
            trend_min_rebalance_fraction=float(spec["trend_min_rebalance_fraction"]),
        )
        for path in [
            scenario_settings.db_path,
            scenario_settings.report_path,
            scenario_settings.first_measurement_report_path,
            scenario_settings.multi_cycle_research_report_path,
            scenario_settings.dashboard_state_path,
        ]:
            path.unlink(missing_ok=True)
        scenario_db = Database(scenario_settings.db_path)
        _bootstrap_trend_replay_bars(
            scenario_settings,
            db=scenario_db,
            as_of=date.today(),
            variant=scenario_settings.trend_bootstrap_variant,
        )
        run_two_sleeve_paper_runtime(scenario_settings, cycles=cycles, start_server=False)
        scenario_report = json.loads(scenario_settings.multi_cycle_research_report_path.read_text(encoding="utf-8"))
        scenario_outputs.append({**spec, "report": scenario_report})
    threshold_report = _build_trend_threshold_sensitivity_report(scenarios=scenario_outputs)
    settings.trend_threshold_sensitivity_report_path.write_text(json.dumps(threshold_report, indent=2), encoding="utf-8")
    return threshold_report


def _run_kalshi_connectivity_check(settings: Settings) -> dict[str, object]:
    started_at = datetime.now(timezone.utc)
    client = KalshiRestClient(settings)
    market_data = MarketDataService(client, settings)
    report: dict[str, object] = {
        "checked_at_utc": started_at.isoformat(),
        "kalshi_api_url": settings.kalshi_api_url,
        "kalshi_mode": settings.kalshi_mode,
        "public_data_reachable": False,
        "auth_configured": bool(settings.kalshi_api_key_id and settings.kalshi_private_key_path),
        "markets_seen": 0,
        "markets_loaded": 0,
        "sample_market": None,
        "sample_orderbook_loaded": False,
        "status": "data_unavailable",
        "error": None,
        "notes": [],
    }
    try:
        raw_markets = market_data.refresh_markets()
        report["markets_seen"] = len(raw_markets)
        report["public_data_reachable"] = client.last_error is None
        if not raw_markets:
            report["error"] = client.last_error or "no_open_markets_returned"
            report["notes"].append("Kalshi public market discovery returned no markets.")
        else:
            filtered = market_data.filter_markets(raw_markets)
            usable = market_data.hydrate_structural_quotes(filtered.included)
            report["markets_loaded"] = len(usable)
            sample = usable[0] if usable else filtered.included[0]
            report["sample_market"] = sample.ticker
            if usable:
                report["sample_orderbook_loaded"] = True
                report["status"] = "live_paper"
                report["notes"].append("Public market discovery and sample orderbook hydration succeeded.")
            else:
                report["status"] = "data_unavailable"
                report["notes"].append("Market discovery succeeded but no structurally usable markets were hydrated.")
    except Exception as exc:  # noqa: BLE001
        report["error"] = str(exc)
        report["notes"].append("Kalshi connectivity check failed before usable market hydration.")
    if not report["auth_configured"]:
        report["notes"].append("Auth not configured; public data can still work, but authenticated order placement cannot.")
    settings.kalshi_connectivity_report_path.write_text(json.dumps(report, indent=2), encoding="utf-8")
    return report


def _sync_observability(*, settings: Settings, db: Database, sleeve_repo: SleeveRepository, cycle_status: dict[str, dict[str, object]]) -> dict[str, object]:
    kalshi_repo = KalshiRepository(db)
    market_state_repo = MarketStateRepository(db)
    signals = kalshi_repo.load_signals()
    evaluations = kalshi_repo.load_evaluations()
    intents = kalshi_repo.load_order_intents()
    outcomes = kalshi_repo.load_execution_outcomes()
    states = market_state_repo.load_market_state_inputs()
    states_by_order = {item.order_id: item for item in states if item.order_id is not None}
    signal_by_ref = {f"{signal.strategy.value}:{signal.ticker}:{signal.ts.isoformat()}": signal for signal in signals}
    outcome_by_id = {outcome.client_order_id: outcome for outcome in outcomes}
    capital = _sleeve_capital(settings)
    replay = ExecutionReplayEngine()
    for intent in intents:
        signal = signal_by_ref.get(intent.signal_ref)
        outcome = outcome_by_id.get(intent.order_id)
        sleeve_name = sleeve_name_for_strategy(intent.strategy_name)
        state = states_by_order.get(intent.order_id)
        slippage = None
        if outcome is not None and state is not None and outcome.price_cents is not None and state.reference_price_cents:
            slippage = format((Decimal(outcome.price_cents) - Decimal(state.reference_price_cents)) / Decimal(state.reference_price_cents) * Decimal("10000"), "f")
        realized_edge = None
        if signal is not None and slippage is not None:
            realized_edge = format(signal.edge.edge_after_fees - (to_decimal(slippage) / Decimal("10000")), "f")
        row = {
            "trade_id": intent.order_id,
            "sleeve_name": sleeve_name,
            "timestamp": intent.created_at.isoformat(),
            "instrument_or_market": intent.instrument.symbol,
            "signal_type": None if signal is None else signal.action,
            "strategy_name": intent.strategy_name,
            "venue": intent.instrument.venue,
            "expected_edge_decimal": None if signal is None else format(signal.edge.edge_before_fees, "f"),
            "expected_edge_after_costs_decimal": None if signal is None else format(signal.edge.edge_after_fees, "f"),
            "bid_decimal": None if state is None or state.best_bid is None else format(state.best_bid, "f"),
            "ask_decimal": None if state is None or state.best_ask is None else format(state.best_ask, "f"),
            "decision_price_reference_decimal": None if state is None else format(state.reference_price, "f"),
            "order_type": intent.order_type,
            "intended_size_decimal": format(intent.quantity, "f"),
            "filled_size_decimal": None if outcome is None else str(outcome.filled_quantity),
            "fill_price_decimal": None if outcome is None or outcome.price_cents is None else format(Decimal(outcome.price_cents) / Decimal("100"), "f"),
            "fill_status": None if outcome is None else outcome.status,
            "fees_decimal": None,
            "slippage_estimate_bps_decimal": slippage,
            "realized_pnl_decimal": None,
            "notes_or_error_code": None if outcome is None else outcome.metadata.get("error"),
            "order_id": intent.order_id,
            "realized_edge_after_costs_decimal": realized_edge,
        }
        sleeve_repo.persist_trade_ledger_row(row)
    for sleeve_name, definition in enabled_sleeves(settings).items():
        sleeve_evaluations = [evaluation for evaluation in evaluations if sleeve_name_for_strategy(evaluation.strategy.value) == sleeve_name]
        sleeve_signals = [signal for signal in signals if sleeve_name_for_strategy(signal.strategy.value) == sleeve_name]
        sleeve_intents = [intent for intent in intents if sleeve_name_for_strategy(intent.strategy_name) == sleeve_name]
        order_ids = {intent.order_id for intent in sleeve_intents}
        sleeve_outcomes = [outcome for outcome in outcomes if outcome.client_order_id in order_ids]
        sleeve_states = [item for item in states if item.order_id in order_ids]
        report = build_execution_report(signals=sleeve_signals, order_intents=sleeve_intents, execution_outcomes=sleeve_outcomes, market_state_inputs=sleeve_states)
        replay_result = replay.replay(signals=sleeve_signals, order_intents=sleeve_intents, execution_outcomes=sleeve_outcomes, market_state_inputs=sleeve_states, starting_cash=capital[sleeve_name])
        recent_ledger = sleeve_repo.load_trade_ledger(sleeve_name=sleeve_name, limit=500)
        expected_edges = [to_decimal(evaluation.edge_after_fees) for evaluation in sleeve_evaluations if evaluation.generated]
        if not expected_edges and sleeve_signals:
            expected_edges = [signal.edge.edge_after_fees for signal in sleeve_signals]
        if not expected_edges:
            expected_edges = [to_decimal(evaluation.edge_after_fees) for evaluation in sleeve_evaluations]
        realized_edges = [to_decimal(row["realized_edge_after_costs_decimal"]) for row in recent_ledger if row.get("realized_edge_after_costs_decimal") is not None]
        avg_expected = _mean(expected_edges)
        avg_realized = _mean(realized_edges)
        gap = None if avg_expected is None or avg_realized is None else avg_expected - avg_realized
        slippages = [to_decimal(row["slippage_estimate_bps_decimal"]) for row in recent_ledger if row.get("slippage_estimate_bps_decimal") is not None]
        fill_rate = None
        if report.summary["orders"]:
            fill_rate = Decimal(str(report.summary["filled_orders"])) / Decimal(str(report.summary["orders"]))
        realized = replay_result.snapshot.realized_pnl
        unrealized = Decimal(str(report.summary.get("marked_unrealized_pnl", 0.0)))
        errors = sleeve_repo.load_recent_errors(sleeve_name=sleeve_name, limit=100)
        status = str(cycle_status.get(sleeve_name, {}).get("status", "running"))
        details = cycle_status.get(sleeve_name, {})
        run_mode = str(details.get("run_mode", "data_unavailable"))
        opportunities_seen = int(details.get("opportunities_seen", len(sleeve_evaluations) or len(sleeve_signals)))
        trades_attempted = int(details.get("signals_attempted", report.summary["orders"]))
        trades_filled = int(report.summary["filled_orders"])
        no_trade_reasons = dict(details.get("no_trade_reasons", {}))
        sleeve_repo.upsert_runtime_status(
            sleeve_name=sleeve_name,
            maturity=definition.maturity,
            run_mode=run_mode,
            status=status,
            enabled=True,
            paper_capable=definition.paper_capable,
            live_capable=definition.live_capable,
            capital_weight=Decimal(str(settings.two_sleeve_capital_weights.get(sleeve_name, definition.capital_weight_default))),
            capital_assigned=capital[sleeve_name],
            opportunities_seen=opportunities_seen,
            trades_attempted=trades_attempted,
            trades_filled=trades_filled,
            open_positions=len(replay_result.snapshot.positions),
            realized_pnl=realized,
            unrealized_pnl=unrealized,
            fees=None,
            avg_slippage_bps=_mean(slippages),
            fill_rate=fill_rate,
            trade_count=int(report.summary["orders"]),
            error_count=len(errors),
            avg_expected_edge=avg_expected,
            avg_realized_edge=avg_realized,
            expected_realized_gap=gap,
            no_trade_reasons=no_trade_reasons,
            details=details,
        )
        sleeve_repo.append_metrics_snapshot(
            sleeve_name=sleeve_name,
            run_mode=run_mode,
            status=status,
            capital_assigned=capital[sleeve_name],
            opportunities_seen=opportunities_seen,
            trades_attempted=trades_attempted,
            trades_filled=trades_filled,
            open_positions=len(replay_result.snapshot.positions),
            realized_pnl=realized,
            unrealized_pnl=unrealized,
            fees=None,
            avg_slippage_bps=_mean(slippages),
            fill_rate=fill_rate,
            trade_count=int(report.summary["orders"]),
            error_count=len(errors),
            avg_expected_edge=avg_expected,
            avg_realized_edge=avg_realized,
            expected_realized_gap=gap,
            no_trade_reasons=no_trade_reasons,
            details=details,
        )
    return _dashboard_state(settings=settings, sleeve_repo=sleeve_repo)


def _run_single_cycle(settings: Settings, *, db: Database | None = None, start_server: bool = True, cycle_index: int = 0) -> dict[str, object]:
    cycle_started_at = datetime.now(timezone.utc)
    logger = setup_logging(settings)
    startup_check = run_startup_checks(settings, "two_sleeve_paper")
    for warning in startup_check.warnings:
        logger.warning("startup_validation_warning", extra={"event": {"warning": warning}})
    if startup_check.errors:
        write_runtime_status(settings, "two_sleeve_paper", "startup_failed", message=",".join(startup_check.errors), details=startup_check.details, startup_check=startup_check)
        raise RuntimeError(f"two_sleeve_paper_startup_failed:{','.join(startup_check.errors)}")
    db = db or Database(settings.db_path)
    kalshi_repo = KalshiRepository(db)
    market_state_repo = MarketStateRepository(db)
    trend_repo = TrendRepository(db)
    sleeve_repo = SleeveRepository(db)
    notifier = Notifier(settings)
    client = KalshiRestClient(settings)
    risk = RiskManager(
        max_daily_loss_pct=settings.max_daily_loss_pct,
        max_total_exposure_pct=settings.max_total_exposure_pct,
        max_position_per_trade_pct=settings.max_position_per_trade_pct,
        max_failed_trades_per_day=settings.max_failed_trades_per_day,
        logger=logger,
    )
    trend_router = ExecutionRouter(client, kalshi_repo, PortfolioState(client), risk, logger, False, market_state_repo=market_state_repo)
    kalshi_router = ExecutionRouter(client, kalshi_repo, PortfolioState(client), risk, logger, False, market_state_repo=market_state_repo)
    trend_engine = TrendPaperEngine(
        signal_repo=kalshi_repo,
        trend_repo=trend_repo,
        risk=risk,
        execution_router=trend_router,
        universe=settings.trend_symbols,
        weight_mode=settings.trend_weight_mode,
        min_rebalance_fraction=Decimal(str(settings.trend_min_rebalance_fraction)),
        starting_capital=_sleeve_capital(settings)["trend"],
    )
    kalshi_engine = KalshiBoosterSleeve(settings=settings, repo=kalshi_repo, market_state_repo=market_state_repo, logger=logger, client=client, execution_router=kalshi_router, portfolio=PortfolioState(client))
    if start_server:
        start_control_panel(state_path=settings.dashboard_state_path, host=settings.control_panel_host, port=settings.control_panel_port, logger=logger)
    write_runtime_status(settings, "two_sleeve_paper", "starting", details={"control_panel_url": f"http://{settings.control_panel_host}:{settings.control_panel_port}/"}, startup_check=startup_check)
    cycle_status: dict[str, dict[str, object]] = {}
    try:
        disk_headroom = assert_runtime_headroom(settings)
        if settings.enable_trend_sleeve:
            try:
                if settings.trend_data_mode == "cached" and settings.trend_replay_advance_mode == "shift_window":
                    _bootstrap_trend_replay_bars(
                        settings,
                        db=db,
                        as_of=date.today() + timedelta(days=cycle_index),
                        variant=settings.trend_bootstrap_variant,
                    )
                bar_stats = TrendBarUpdater(trend_repo, logger, settings).refresh(settings.trend_symbols, as_of=datetime.now(timezone.utc))
                trend_result = trend_engine.run(date.today())
                reason_counts: dict[str, int] = {}
                for error in bar_stats["errors"]:
                    symbol, _, message = str(error).partition(":")
                    sleeve_repo.record_error(
                        sleeve_name="trend",
                        error_code="trend_market_data_error",
                        message=message or str(error),
                        details={"symbol": symbol},
                    )
                    _bump_reason(reason_counts, "broker_or_data_failure")
                for halt_reason in trend_result.readiness.halt_reasons:
                    sleeve_repo.record_error(
                        sleeve_name="trend",
                        error_code="trend_halt",
                        message=halt_reason,
                        details={},
                    )
                    _bump_reason(reason_counts, _normalize_no_trade_reason(halt_reason, "trend"))
                if not trend_result.signals and trend_result.readiness.can_trade:
                    _bump_reason(reason_counts, "no_rebalance_needed" if trend_result.targets else "insufficient_history")
                blocker_notes = []
                if bar_stats["run_mode"] == "data_unavailable":
                    blocker_notes.append("trend data fetch unavailable and no cached history present")
                elif bar_stats["run_mode"] == "replay_paper":
                    blocker_notes.append("trend running from cached historical bars")
                trend_cycle_diagnostics = _build_trend_cycle_diagnostics(
                    trend_repo=trend_repo,
                    trend_result=trend_result,
                    trend_engine=trend_engine,
                    min_rebalance_fraction=trend_engine.min_rebalance_fraction,
                )
                cycle_status["trend"] = {
                    "status": "running" if trend_result.readiness.can_trade else "paused",
                    "run_mode": bar_stats["run_mode"],
                    "bar_stats": bar_stats,
                    "halt_reasons": trend_result.readiness.halt_reasons,
                    "signals_generated": len(trend_result.signals),
                    "signals_attempted": len(trend_result.outcomes),
                    "targets_generated": len(trend_result.targets),
                    "opportunities_seen": len(trend_result.targets),
                    "no_trade_reasons": reason_counts,
                    "cash": format(trend_result.snapshot.cash, "f"),
                    "equity": format(trend_result.snapshot.equity, "f"),
                    "gross_exposure": format(trend_result.snapshot.gross_exposure, "f"),
                    "net_exposure": format(trend_result.snapshot.net_exposure, "f"),
                    "drawdown": format(trend_result.snapshot.drawdown, "f"),
                    "position_count": len(trend_result.snapshot.positions),
                    "blocker_notes": blocker_notes,
                    "trend_cycle_diagnostics": trend_cycle_diagnostics,
                    "sleeve_name": "trend",
                }
                logger.info("sleeve_cycle_complete", extra={"event": cycle_status["trend"]})
            except Exception as exc:  # noqa: BLE001
                cycle_status["trend"] = {"status": "error", "run_mode": "data_unavailable", "error": str(exc), "no_trade_reasons": {"broker_or_data_failure": 1}, "blocker_notes": [str(exc)], "sleeve_name": "trend"}
                sleeve_repo.record_error(sleeve_name="trend", error_code="trend_cycle_error", message=str(exc), details={})
                logger.error("sleeve_cycle_error", extra={"event": {"sleeve_name": "trend", "error": str(exc)}})
        if settings.enable_kalshi_sleeve:
            try:
                cycle_status["kalshi_event"] = kalshi_engine.run_cycle(_sleeve_capital(settings)["kalshi_event"])
                for halt_reason in cycle_status["kalshi_event"].get("trading_halt_reasons", []):
                    sleeve_repo.record_error(
                        sleeve_name="kalshi_event",
                        error_code="kalshi_halt",
                        message=str(halt_reason),
                        details={},
                    )
                if cycle_status["kalshi_event"].get("data_error"):
                    sleeve_repo.record_error(
                        sleeve_name="kalshi_event",
                        error_code="kalshi_data_error",
                        message=str(cycle_status["kalshi_event"]["data_error"]),
                        details={"path": client.last_error_path},
                    )
                if not cycle_status["kalshi_event"].get("markets_seen"):
                    cycle_status["kalshi_event"].setdefault("blocker_notes", []).append("Kalshi discovery returned no markets")
                logger.info("sleeve_cycle_complete", extra={"event": cycle_status["kalshi_event"]})
            except Exception as exc:  # noqa: BLE001
                cycle_status["kalshi_event"] = {"status": "error", "run_mode": "data_unavailable", "error": str(exc), "no_trade_reasons": {"data_failure": 1}, "blocker_notes": [str(exc)], "sleeve_name": "kalshi_event"}
                sleeve_repo.record_error(sleeve_name="kalshi_event", error_code="kalshi_cycle_error", message=str(exc), details={})
                logger.error("sleeve_cycle_error", extra={"event": {"sleeve_name": "kalshi_event", "error": str(exc)}})
        state = _sync_observability(settings=settings, db=db, sleeve_repo=sleeve_repo, cycle_status=cycle_status)
        settings.dashboard_state_path.write_text(json.dumps(state, indent=2), encoding="utf-8")
        settings.report_path.write_text(
            json.dumps(
                {
                    "runtime_name": "two_sleeve_paper",
                    "run_mode": state["run_mode"],
                    "control_panel_url": state["control_panel_url"],
                    "ledger_explanation": state.get("ledger_explanation"),
                    "sleeves": state["sleeves"],
                    "comparison": state["comparison"],
                },
                indent=2,
            ),
            encoding="utf-8",
        )
        report_ended_at = datetime.now(timezone.utc)
        _write_measurement_report(settings=settings, state=state, started_at=cycle_started_at, ended_at=report_ended_at, cycles_completed=1)
        _write_strategy_transparency_pack(settings=settings, state=state)
        _write_multi_cycle_research_report(
            settings=settings,
            sleeve_repo=sleeve_repo,
            started_at=cycle_started_at,
            ended_at=report_ended_at,
            cycles_completed=1,
        )
        write_runtime_status(settings, "two_sleeve_paper", "running", details={"disk_headroom": disk_headroom, "control_panel_url": state["control_panel_url"], "sleeves": cycle_status}, update_heartbeat=True)
        return state
    except Exception as exc:  # noqa: BLE001
        write_runtime_status(settings, "two_sleeve_paper", "error", message=str(exc), details={"sleeves": cycle_status})
        try:
            notifier.send("Two sleeve paper runtime crash", str(exc))
        except Exception:  # noqa: BLE001
            logger.error("notifier_error", extra={"event": {"error": "failed_to_send_crash_notification"}})
        raise


def run_two_sleeve_paper_runtime(settings: Settings, *, cycles: int | None = None, start_server: bool = True) -> None:
    run_started_at = datetime.now(timezone.utc)
    completed = 0
    db = Database(settings.db_path)
    last_state = _run_single_cycle(settings, db=db, start_server=start_server, cycle_index=0)
    completed += 1
    while cycles is None or completed < cycles:
        time.sleep(settings.poll_interval_seconds)
        last_state = _run_single_cycle(settings, db=db, start_server=False, cycle_index=completed)
        completed += 1
    if cycles is not None:
        ended_at = datetime.now(timezone.utc)
        _write_measurement_report(
            settings=settings,
            state=last_state,
            started_at=run_started_at,
            ended_at=ended_at,
            cycles_completed=completed,
        )
        _write_strategy_transparency_pack(settings=settings, state=last_state)
        _write_multi_cycle_research_report(
            settings=settings,
            sleeve_repo=SleeveRepository(db),
            started_at=run_started_at,
            ended_at=ended_at,
            cycles_completed=completed,
        )


def main() -> int:
    parser = argparse.ArgumentParser(description="Run the two-sleeve paper runtime.")
    parser.add_argument("--once", action="store_true")
    parser.add_argument("--no-server", action="store_true")
    parser.add_argument("--cycles", type=int, default=None)
    parser.add_argument("--seed-trend-bootstrap", action="store_true")
    parser.add_argument("--check-kalshi", action="store_true")
    parser.add_argument("--trend-replay-diversity-check", action="store_true")
    parser.add_argument("--trend-threshold-sensitivity-check", action="store_true")
    args = parser.parse_args()
    settings = Settings()
    settings.ensure_directories()
    if args.check_kalshi:
        print(json.dumps(_run_kalshi_connectivity_check(settings), indent=2))
        return 0
    if args.trend_replay_diversity_check:
        print(json.dumps(_run_trend_replay_diversity_check(settings, cycles=args.cycles or 5), indent=2))
        return 0
    if args.trend_threshold_sensitivity_check:
        print(json.dumps(_run_trend_threshold_sensitivity_check(settings, cycles=args.cycles or 5), indent=2))
        return 0
    if args.seed_trend_bootstrap:
        bootstrap_db = Database(settings.db_path)
        summary = _bootstrap_trend_replay_bars(settings, db=bootstrap_db)
        print(json.dumps({"seed_trend_bootstrap": summary}))
        if settings.trend_data_mode != "cached":
            settings.trend_data_mode = "cached"
        if args.once:
            _run_single_cycle(settings, db=bootstrap_db, start_server=not args.no_server, cycle_index=0)
            return 0
        if args.cycles is not None:
            run_two_sleeve_paper_runtime(settings, cycles=args.cycles, start_server=not args.no_server)
            return 0
        return 0
    if args.once:
        _run_single_cycle(settings, start_server=not args.no_server)
        return 0
    run_two_sleeve_paper_runtime(settings, cycles=args.cycles, start_server=not args.no_server)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
