from __future__ import annotations

import math
from decimal import Decimal

from app.core.money import to_decimal
from app.strategies.trend.models import DailyBar


def realized_volatility(bars: list[DailyBar], lookback_bars: int) -> Decimal | None:
    if len(bars) <= lookback_bars:
        return None
    returns: list[float] = []
    window = bars[-(lookback_bars + 1) :]
    for previous, current in zip(window, window[1:]):
        if previous.close <= 0:
            return None
        returns.append(float((current.close / previous.close) - Decimal("1")))
    if len(returns) < 2:
        return None
    mean = math.fsum(returns) / len(returns)
    variance = math.fsum((value - mean) ** 2 for value in returns) / (len(returns) - 1)
    if variance <= 0:
        return None
    return to_decimal(math.sqrt(252.0) * math.sqrt(variance))


def target_weight_from_signal(
    *,
    momentum: Decimal,
    realized_vol: Decimal,
    target_vol: Decimal,
    max_weight: Decimal,
) -> Decimal:
    if momentum <= 0 or realized_vol is None or realized_vol <= 0:
        return Decimal("0")
    raw_weight = target_vol / realized_vol
    return min(max_weight, raw_weight)


def _soft_monotone_weight(
    *,
    momentum: Decimal,
    realized_vol: Decimal,
    target_vol: Decimal,
    max_weight: Decimal,
) -> Decimal:
    if momentum <= 0 or realized_vol is None or realized_vol <= 0:
        return Decimal("0")
    base = target_vol / realized_vol
    strength = momentum / (momentum + Decimal("0.05"))
    return min(max_weight, base * strength)


def build_target_weight_plan(
    *,
    symbol_rows: list[dict[str, Decimal | str]],
    target_vol: Decimal,
    max_weight: Decimal,
    max_gross_exposure: Decimal,
    weight_mode: str = "current",
) -> dict[str, object]:
    normalized_mode = str(weight_mode or "current").strip().lower()
    raw_weights: dict[str, Decimal] = {}
    capped_symbols: set[str] = set()
    positive_rows: list[dict[str, Decimal]] = []
    for row in symbol_rows:
        symbol = str(row["symbol"])
        momentum = to_decimal(row["momentum"])
        realized_vol = to_decimal(row["realized_vol"])
        raw_weights[symbol] = Decimal("0")
        if momentum > 0 and realized_vol > 0:
            positive_rows.append({"symbol": symbol, "momentum": momentum, "realized_vol": realized_vol})
    if normalized_mode in {"current", "soft_monotone"}:
        for row in positive_rows:
            if normalized_mode == "current":
                raw = target_weight_from_signal(
                    momentum=row["momentum"],
                    realized_vol=row["realized_vol"],
                    target_vol=target_vol,
                    max_weight=max_weight,
                )
            else:
                raw = _soft_monotone_weight(
                    momentum=row["momentum"],
                    realized_vol=row["realized_vol"],
                    target_vol=target_vol,
                    max_weight=max_weight,
                )
            raw_weights[row["symbol"]] = raw
            if raw >= max_weight:
                capped_symbols.add(row["symbol"])
    elif normalized_mode == "rank_normalized":
        scored: list[tuple[str, Decimal]] = []
        for row in positive_rows:
            score = row["momentum"] / max(row["realized_vol"], Decimal("0.0001"))
            if score > 0:
                scored.append((row["symbol"], score))
        total_score = sum((score for _, score in scored), Decimal("0"))
        if total_score > 0:
            remaining = max_gross_exposure
            for symbol, score in sorted(scored, key=lambda item: (-item[1], item[0])):
                raw = max_gross_exposure * (score / total_score)
                clipped = min(max_weight, raw, remaining)
                raw_weights[symbol] = clipped
                if clipped >= max_weight:
                    capped_symbols.add(symbol)
                remaining -= clipped
    elif normalized_mode == "gentle_score":
        scored: list[tuple[str, Decimal]] = []
        for row in positive_rows:
            score = row["momentum"].sqrt() / max(row["realized_vol"].sqrt(), Decimal("0.01"))
            if score > 0:
                scored.append((row["symbol"], score))
        total_score = sum((score for _, score in scored), Decimal("0"))
        if total_score > 0:
            for symbol, score in scored:
                raw = max_gross_exposure * (score / total_score)
                clipped = min(max_weight, raw)
                raw_weights[symbol] = clipped
                if clipped >= max_weight:
                    capped_symbols.add(symbol)
    else:
        raise ValueError(f"unknown_trend_weight_mode:{weight_mode}")
    gross_raw = sum(raw_weights.values(), Decimal("0"))
    scale = Decimal("0")
    if gross_raw > 0:
        scale = min(Decimal("1"), max_gross_exposure / gross_raw)
    final_weights = {symbol: weight * scale for symbol, weight in raw_weights.items()}
    return {
        "weight_mode": normalized_mode,
        "raw_weights": raw_weights,
        "final_weights": final_weights,
        "capped_symbols": sorted(capped_symbols),
        "gross_raw_weight": gross_raw,
        "gross_final_weight": sum(final_weights.values(), Decimal("0")),
        "scaling_factor": scale,
    }
