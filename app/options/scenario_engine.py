from __future__ import annotations

import math
import random
from dataclasses import dataclass
from datetime import datetime, timedelta
from decimal import Decimal

from app.core.money import bps_to_fraction, quantize_cents, to_decimal
from app.options.models import OptionChainSnapshot, OptionQuote, OptionStateVector
from app.options.pricing import black_scholes_price, intrinsic_value, years_to_expiry
from app.options.structures import OptionStructure, OptionStructureLeg


@dataclass(frozen=True, slots=True)
class ScenarioAssumptions:
    horizon_days: int
    path_count: int
    seed: int
    annual_drift: Decimal = Decimal("0")
    annual_volatility: Decimal | None = None
    event_day: int | None = None
    event_jump_mean: Decimal = Decimal("0")
    event_jump_stddev: Decimal = Decimal("0")
    iv_mean_reversion: Decimal = Decimal("0.15")
    iv_regime_level: Decimal | None = None
    iv_volatility: Decimal = Decimal("0.10")
    event_crush: Decimal = Decimal("0")
    exit_spread_fraction: Decimal = Decimal("0.50")
    slippage_bps: Decimal = Decimal("0")
    stop_out_loss: Decimal | None = None


@dataclass(frozen=True, slots=True)
class StructureScenarioSummary:
    underlying: str
    observed_at: datetime
    source: str
    structure_name: str
    horizon_days: int
    path_count: int
    seed: int
    expected_pnl: Decimal
    probability_of_profit: Decimal
    cvar_95_loss: Decimal
    expected_max_drawdown: Decimal
    stop_out_probability: Decimal
    predicted_daily_vol: Decimal
    expected_entry_cost: Decimal
    stress_expected_pnl: Decimal | None
    model_uncertainty_penalty: Decimal | None


def summarize_structure(
    *,
    snapshot: OptionChainSnapshot,
    state_vector: OptionStateVector,
    structure: OptionStructure,
    assumptions: ScenarioAssumptions,
) -> StructureScenarioSummary:
    pnl_paths = simulate_structure_paths(
        snapshot=snapshot,
        state_vector=state_vector,
        structure=structure,
        assumptions=assumptions,
    )
    stress = _stress_assumptions(assumptions)
    stress_paths = simulate_structure_paths(
        snapshot=snapshot,
        state_vector=state_vector,
        structure=structure,
        assumptions=stress,
    )
    expected_pnl = _mean(pnl_paths.terminal_pnls)
    stress_expected_pnl = _mean(stress_paths.terminal_pnls)
    return StructureScenarioSummary(
        underlying=snapshot.underlying,
        observed_at=snapshot.observed_at,
        source=snapshot.source,
        structure_name=structure.name,
        horizon_days=assumptions.horizon_days,
        path_count=assumptions.path_count,
        seed=assumptions.seed,
        expected_pnl=expected_pnl,
        probability_of_profit=_probability_of_profit(pnl_paths.terminal_pnls),
        cvar_95_loss=_cvar_95(pnl_paths.terminal_pnls),
        expected_max_drawdown=_mean(pnl_paths.max_drawdowns),
        stop_out_probability=_stop_out_probability(pnl_paths.stop_outs),
        predicted_daily_vol=_predicted_daily_vol(pnl_paths.daily_pnl_samples),
        expected_entry_cost=pnl_paths.entry_cost,
        stress_expected_pnl=stress_expected_pnl,
        model_uncertainty_penalty=expected_pnl - stress_expected_pnl,
    )


@dataclass(frozen=True, slots=True)
class _SimulatedPaths:
    terminal_pnls: list[Decimal]
    max_drawdowns: list[Decimal]
    stop_outs: list[bool]
    daily_pnl_samples: list[Decimal]
    entry_cost: Decimal


def simulate_structure_paths(
    *,
    snapshot: OptionChainSnapshot,
    state_vector: OptionStateVector,
    structure: OptionStructure,
    assumptions: ScenarioAssumptions,
) -> _SimulatedPaths:
    quotes_by_key = {quote.contract.contract_key(): quote for quote in snapshot.quotes}
    entry_cost = _entry_cost(structure=structure, quotes_by_key=quotes_by_key)
    annual_vol = assumptions.annual_volatility or state_vector.realized_vol_20d or state_vector.atm_iv_near or Decimal("0.20")
    iv_regime_level = assumptions.iv_regime_level or state_vector.atm_iv_next or state_vector.atm_iv_near or Decimal("0.20")
    rng = random.Random(assumptions.seed)
    terminal_pnls: list[Decimal] = []
    max_drawdowns: list[Decimal] = []
    stop_outs: list[bool] = []
    daily_pnl_samples: list[Decimal] = []

    for _ in range(assumptions.path_count):
        spot = snapshot.spot_price
        iv_shift = Decimal("0")
        peak_pnl = -entry_cost
        current_pnl = -entry_cost
        max_drawdown = Decimal("0")
        stop_out = False

        for day in range(1, assumptions.horizon_days + 1):
            z_spot = to_decimal(rng.gauss(0.0, 1.0))
            z_iv = to_decimal(rng.gauss(0.0, 1.0))
            daily_drift = assumptions.annual_drift / Decimal("252")
            daily_vol = annual_vol / Decimal("15.874507866387544")
            spot = spot * _exp_decimal(daily_drift + daily_vol * z_spot)
            if assumptions.event_day is not None and day == assumptions.event_day and assumptions.event_jump_stddev > 0:
                jump = assumptions.event_jump_mean + assumptions.event_jump_stddev * to_decimal(rng.gauss(0.0, 1.0))
                spot = spot * _exp_decimal(jump)
            iv_shift = _next_iv_shift(
                current_shift=iv_shift,
                current_base=state_vector.atm_iv_near or iv_regime_level,
                regime_level=iv_regime_level,
                z_iv=z_iv,
                assumptions=assumptions,
                day=day,
            )
            structure_value = _structure_mark_value(
                structure=structure,
                spot=spot,
                quote_time=snapshot.observed_at + timedelta(days=day),
                quotes_by_key=quotes_by_key,
                risk_free_rate=snapshot.risk_free_rate,
                dividend_yield=snapshot.dividend_yield,
                iv_shift=iv_shift,
                exit_spread_fraction=assumptions.exit_spread_fraction,
                slippage_bps=assumptions.slippage_bps,
            )
            previous_pnl = current_pnl
            current_pnl = structure_value - entry_cost
            daily_pnl_samples.append(current_pnl - previous_pnl)
            if current_pnl > peak_pnl:
                peak_pnl = current_pnl
            drawdown = peak_pnl - current_pnl
            if drawdown > max_drawdown:
                max_drawdown = drawdown
            if assumptions.stop_out_loss is not None and current_pnl <= assumptions.stop_out_loss:
                stop_out = True

        terminal_pnls.append(quantize_cents(current_pnl))
        max_drawdowns.append(quantize_cents(max_drawdown))
        stop_outs.append(stop_out)

    return _SimulatedPaths(
        terminal_pnls=terminal_pnls,
        max_drawdowns=max_drawdowns,
        stop_outs=stop_outs,
        daily_pnl_samples=daily_pnl_samples,
        entry_cost=entry_cost,
    )


def _entry_cost(*, structure: OptionStructure, quotes_by_key: dict[str, OptionQuote]) -> Decimal:
    total = Decimal("0")
    for leg in structure.legs:
        quote = quotes_by_key[leg.contract.contract_key()]
        if leg.quantity > 0:
            unit_price = _entry_unit_price(quote, side="buy")
            total += to_decimal(abs(leg.quantity)) * unit_price * Decimal(quote.contract.multiplier)
        else:
            unit_price = _entry_unit_price(quote, side="sell")
            total -= to_decimal(abs(leg.quantity)) * unit_price * Decimal(quote.contract.multiplier)
    return quantize_cents(total)


def _entry_unit_price(quote: OptionQuote, *, side: str) -> Decimal:
    if side == "buy":
        if quote.ask is not None:
            return quote.ask
    else:
        if quote.bid is not None:
            return quote.bid
    if quote.bid is not None and quote.ask is not None:
        return (quote.bid + quote.ask) / Decimal("2")
    if quote.last is not None:
        return quote.last
    raise KeyError(f"missing entry quote for {quote.contract.contract_key()}")


def _structure_mark_value(
    *,
    structure: OptionStructure,
    spot: Decimal,
    quote_time,
    quotes_by_key: dict[str, OptionQuote],
    risk_free_rate: Decimal,
    dividend_yield: Decimal,
    iv_shift: Decimal,
    exit_spread_fraction: Decimal,
    slippage_bps: Decimal,
) -> Decimal:
    total = Decimal("0")
    for leg in structure.legs:
        quote = quotes_by_key[leg.contract.contract_key()]
        days_to_expiry = (leg.contract.expiration_date - quote_time.date()).days
        mark = _repriced_option_mark(
            quote=quote,
            spot=spot,
            days_to_expiry=days_to_expiry,
            risk_free_rate=risk_free_rate,
            dividend_yield=dividend_yield,
            iv_shift=iv_shift,
            exit_spread_fraction=exit_spread_fraction,
            side="sell" if leg.quantity > 0 else "buy",
            slippage_bps=slippage_bps,
        )
        total += to_decimal(leg.quantity) * mark * Decimal(quote.contract.multiplier)
    return quantize_cents(total)


def _repriced_option_mark(
    *,
    quote: OptionQuote,
    spot: Decimal,
    days_to_expiry: int,
    risk_free_rate: Decimal,
    dividend_yield: Decimal,
    iv_shift: Decimal,
    exit_spread_fraction: Decimal,
    side: str,
    slippage_bps: Decimal,
) -> Decimal:
    if days_to_expiry <= 0:
        theoretical = intrinsic_value(spot=spot, strike=quote.contract.strike, option_type=quote.contract.option_type)
    else:
        current_iv = quote.implied_volatility or Decimal("0.20")
        future_iv = max(Decimal("0.01"), current_iv + iv_shift)
        theoretical = black_scholes_price(
            spot=spot,
            strike=quote.contract.strike,
            time_to_expiry_years=years_to_expiry(days_to_expiry),
            risk_free_rate=risk_free_rate,
            dividend_yield=dividend_yield,
            implied_volatility=future_iv,
            option_type=quote.contract.option_type,
        )
    spread = _spread(quote)
    if side == "sell":
        adjusted = theoretical - (spread * exit_spread_fraction)
        adjusted *= Decimal("1") - bps_to_fraction(slippage_bps)
    else:
        adjusted = theoretical + (spread * exit_spread_fraction)
        adjusted *= Decimal("1") + bps_to_fraction(slippage_bps)
    return max(Decimal("0.01"), adjusted)


def _spread(quote: OptionQuote) -> Decimal:
    if quote.bid is not None and quote.ask is not None and quote.ask >= quote.bid:
        return quote.ask - quote.bid
    return Decimal("0.10")


def _next_iv_shift(
    *,
    current_shift: Decimal,
    current_base: Decimal,
    regime_level: Decimal,
    z_iv: Decimal,
    assumptions: ScenarioAssumptions,
    day: int,
) -> Decimal:
    mean_reversion = assumptions.iv_mean_reversion * ((regime_level - current_base) - current_shift) / Decimal("252")
    iv_noise = assumptions.iv_volatility * z_iv / Decimal("15.874507866387544")
    crush = assumptions.event_crush if assumptions.event_day is not None and day >= assumptions.event_day else Decimal("0")
    next_shift = current_shift + mean_reversion + iv_noise - crush
    floor = Decimal("0.01") - current_base
    return max(floor, next_shift)


def _mean(values: list[Decimal]) -> Decimal:
    if not values:
        return Decimal("0")
    return quantize_cents(sum(values, Decimal("0")) / Decimal(len(values)))


def _probability_of_profit(values: list[Decimal]) -> Decimal:
    if not values:
        return Decimal("0")
    wins = sum(1 for value in values if value > 0)
    return to_decimal(wins) / to_decimal(len(values))


def _cvar_95(values: list[Decimal]) -> Decimal:
    if not values:
        return Decimal("0")
    ordered = sorted(values)
    tail_count = max(1, math.ceil(len(ordered) * 0.05))
    tail = ordered[:tail_count]
    return quantize_cents(sum(tail, Decimal("0")) / Decimal(len(tail)))


def _stop_out_probability(values: list[bool]) -> Decimal:
    if not values:
        return Decimal("0")
    hits = sum(1 for value in values if value)
    return to_decimal(hits) / to_decimal(len(values))


def _predicted_daily_vol(values: list[Decimal]) -> Decimal:
    if len(values) < 2:
        return Decimal("0")
    mean = sum(values, Decimal("0")) / Decimal(len(values))
    variance = sum((value - mean) * (value - mean) for value in values) / Decimal(len(values) - 1)
    return quantize_cents(variance.sqrt())


def _exp_decimal(value: Decimal) -> Decimal:
    return to_decimal(math.exp(float(value)))


def _stress_assumptions(base: ScenarioAssumptions) -> ScenarioAssumptions:
    return ScenarioAssumptions(
        horizon_days=base.horizon_days,
        path_count=base.path_count,
        seed=base.seed + 10_000,
        annual_drift=base.annual_drift,
        annual_volatility=base.annual_volatility,
        event_day=base.event_day,
        event_jump_mean=base.event_jump_mean,
        event_jump_stddev=base.event_jump_stddev * Decimal("1.30"),
        iv_mean_reversion=base.iv_mean_reversion,
        iv_regime_level=base.iv_regime_level,
        iv_volatility=base.iv_volatility,
        event_crush=base.event_crush * Decimal("0.50"),
        exit_spread_fraction=min(Decimal("1.0"), base.exit_spread_fraction * Decimal("1.50")),
        slippage_bps=base.slippage_bps * Decimal("1.50"),
        stop_out_loss=base.stop_out_loss,
    )
