from __future__ import annotations

from decimal import Decimal
from datetime import datetime, timedelta, timezone

from app.execution.models import SignalLegIntent
from app.instruments.models import InstrumentRef
from app.market_data.fees import balance_aligned_fee_dollars, edge_threshold
from app.models import Market, Signal, SignalEdge, StrategyEvaluation, StrategyName
from app.strategies.base import Strategy


class MonotonicityStrategy(Strategy):
    def __init__(
        self,
        min_edge_bps: float,
        slippage_buffer_bps: float,
        max_position_pct: float,
        min_top_level_depth: int = 50,
        min_size_multiple: float = 2.0,
        ladder_cooldown_seconds: int = 15,
    ) -> None:
        self.min_edge_bps = min_edge_bps
        self.slippage_buffer_bps = slippage_buffer_bps
        self.max_position_pct = max_position_pct
        self.min_top_level_depth = min_top_level_depth
        self.min_size_multiple = min_size_multiple
        self.ladder_cooldown = timedelta(seconds=ladder_cooldown_seconds)
        self._last_signal_at: dict[tuple[str, str], datetime] = {}

    def evaluate(self, ladder: list[Market], equity: float) -> tuple[list[Signal], list[StrategyEvaluation]]:
        signals: list[Signal] = []
        evaluations: list[StrategyEvaluation] = []
        now = datetime.now(timezone.utc)
        valid_strike_count = sum(1 for market in ladder if market.strike is not None)
        if len(ladder) < 2 or valid_strike_count < 2:
            ticker = ladder[0].event_ticker if ladder else "unknown"
            return [], [
                StrategyEvaluation(
                    strategy=StrategyName.MONOTONICITY,
                    ticker=ticker,
                    generated=False,
                    reason="invalid_ladder_structure",
                    edge_before_fees=0.0,
                    edge_after_fees=0.0,
                    fee_estimate=0.0,
                    quantity=0,
                    metadata={"ladder_size": len(ladder), "valid_strikes": valid_strike_count},
                )
            ]
        for lower, higher in zip(ladder, ladder[1:]):
            low_price = lower.yes_ask
            high_price = higher.yes_bid
            quote_context = _signal_time_quote_context(lower, higher, self.min_top_level_depth)
            if low_price is None or high_price is None:
                missing_fields: list[str] = []
                if low_price is None:
                    missing_fields.append("lower.yes_ask")
                if high_price is None:
                    missing_fields.append("higher.yes_bid")
                evaluations.append(
                    StrategyEvaluation(
                        strategy=StrategyName.MONOTONICITY,
                        ticker=lower.ticker,
                        generated=False,
                        reason="missing_prices",
                        edge_before_fees=0.0,
                        edge_after_fees=0.0,
                        fee_estimate=0.0,
                        quantity=0,
                        metadata={"lower": lower.ticker, "higher": higher.ticker, "missing_fields": missing_fields, **quote_context},
                    )
                )
                continue
            if not quote_context["depth_verified"]:
                evaluations.append(
                    StrategyEvaluation(
                        strategy=StrategyName.MONOTONICITY,
                        ticker=lower.ticker,
                        generated=False,
                        reason="insufficient_signal_time_depth",
                        edge_before_fees=0.0,
                        edge_after_fees=0.0,
                        fee_estimate=0.0,
                        quantity=0,
                        metadata={"lower": lower.ticker, "higher": higher.ticker, **quote_context},
                    )
                )
                continue
            raw_edge = Decimal(str(high_price - low_price)) / Decimal("100")
            fees = balance_aligned_fee_dollars(Decimal(low_price) / Decimal("100"), side="buy") + balance_aligned_fee_dollars(Decimal(high_price) / Decimal("100"), side="sell")
            gate = edge_threshold(
                edge_before_fees=raw_edge,
                fee_estimate=fees,
                execution_buffer=Decimal(str(self.slippage_buffer_bps)) / Decimal("10000"),
            )
            edge_bps = float(gate.edge_after_fees * Decimal("10000"))
            qty = self.size_from_equity(equity, self.max_position_pct, low_price / 100.0)
            size_multiple_lower = (quote_context["lower_yes_ask_size"] / qty) if qty > 0 else 0.0
            size_multiple_higher = (quote_context["higher_yes_bid_size"] / qty) if qty > 0 else 0.0
            metadata = {
                "lower": lower.ticker,
                "higher": higher.ticker,
                "raw_edge": float(gate.edge_before_fees),
                "fees": float(gate.fee_estimate),
                "size_multiple_lower": size_multiple_lower,
                "size_multiple_higher": size_multiple_higher,
                "min_size_multiple": self.min_size_multiple,
                **quote_context,
            }
            if size_multiple_lower < self.min_size_multiple or size_multiple_higher < self.min_size_multiple:
                evaluations.append(
                    StrategyEvaluation(
                        strategy=StrategyName.MONOTONICITY,
                        ticker=lower.ticker,
                        generated=False,
                        reason="insufficient_size_multiple",
                        edge_before_fees=float(gate.edge_before_fees),
                        edge_after_fees=float(gate.edge_after_fees),
                        fee_estimate=float(gate.fee_estimate),
                        quantity=qty,
                        metadata=metadata,
                    )
                )
                continue
            if not gate.passes_bps(self.min_edge_bps):
                evaluations.append(
                    StrategyEvaluation(
                        strategy=StrategyName.MONOTONICITY,
                        ticker=lower.ticker,
                        generated=False,
                        reason="edge_below_threshold",
                        edge_before_fees=float(gate.edge_before_fees),
                        edge_after_fees=float(gate.edge_after_fees),
                        fee_estimate=float(gate.fee_estimate),
                        quantity=qty,
                        metadata=metadata,
                    )
                )
                continue
            pair_key = (lower.ticker, higher.ticker)
            last_signal_at = self._last_signal_at.get(pair_key)
            if last_signal_at is not None and now - last_signal_at < self.ladder_cooldown:
                metadata["cooldown_remaining_seconds"] = max(0.0, self.ladder_cooldown.total_seconds() - (now - last_signal_at).total_seconds())
                evaluations.append(
                    StrategyEvaluation(
                        strategy=StrategyName.MONOTONICITY,
                        ticker=lower.ticker,
                        generated=False,
                        reason="ladder_cooldown_active",
                        edge_before_fees=float(gate.edge_before_fees),
                        edge_after_fees=float(gate.edge_after_fees),
                        fee_estimate=float(gate.fee_estimate),
                        quantity=qty,
                        metadata=metadata,
                    )
                )
                continue
            signals.append(
                Signal(
                    strategy=StrategyName.MONOTONICITY,
                    ticker=lower.ticker,
                    action="buy_lower_sell_higher",
                    quantity=qty,
                    edge=SignalEdge.from_values(
                        edge_before_fees=gate.edge_before_fees,
                        edge_after_fees=gate.edge_after_fees,
                        fee_estimate=gate.fee_estimate,
                        execution_buffer=Decimal(str(self.slippage_buffer_bps)) / Decimal("10000"),
                    ),
                    source="monotonicity_ladder",
                    confidence=1.0,
                    priority=1,
                    legs=[
                        SignalLegIntent(
                            instrument=InstrumentRef(symbol=lower.ticker, contract_side="yes"),
                            side="buy",
                            order_type="limit",
                            limit_price=Decimal(int(low_price)) / Decimal("100"),
                            time_in_force="FOK",
                        ),
                        SignalLegIntent(
                            instrument=InstrumentRef(symbol=higher.ticker, contract_side="yes"),
                            side="sell",
                            order_type="limit",
                            limit_price=Decimal(int(high_price)) / Decimal("100"),
                            time_in_force="IOC",
                        ),
                    ],
                )
            )
            self._last_signal_at[pair_key] = now
            evaluations.append(
                StrategyEvaluation(
                    strategy=StrategyName.MONOTONICITY,
                    ticker=lower.ticker,
                    generated=True,
                    reason="signal_generated",
                    edge_before_fees=float(gate.edge_before_fees),
                    edge_after_fees=float(gate.edge_after_fees),
                    fee_estimate=float(gate.fee_estimate),
                    quantity=qty,
                    metadata=metadata,
                )
            )
        return signals, evaluations

    def generate(self, ladder: list[Market], equity: float) -> list[Signal]:
        signals, _ = self.evaluate(ladder, equity)
        return signals


def _signal_time_quote_context(lower: Market, higher: Market, min_top_level_depth: int) -> dict:
    lower_diag = dict(lower.metadata.get("quote_diagnostics", {}))
    higher_diag = dict(higher.metadata.get("quote_diagnostics", {}))
    lower_yes_ask_size = int(lower_diag.get("best_yes_ask_size") or 0)
    higher_yes_bid_size = int(higher_diag.get("best_yes_bid_size") or 0)
    lower_yes_bid_size = int(lower_diag.get("best_yes_bid_size") or 0)
    higher_yes_ask_size = int(higher_diag.get("best_yes_ask_size") or 0)
    executable_sides_meet_min_size = lower_yes_ask_size >= min_top_level_depth and higher_yes_bid_size >= min_top_level_depth
    average_signal_time_depth = (lower_yes_ask_size + higher_yes_bid_size) / 2.0
    insufficient_fields: list[str] = []
    if lower_yes_ask_size < min_top_level_depth:
        insufficient_fields.append("lower.yes_ask_size")
    if higher_yes_bid_size < min_top_level_depth:
        insufficient_fields.append("higher.yes_bid_size")
    return {
        "lower_yes_ask_price": lower.yes_ask,
        "lower_yes_ask_size": lower_yes_ask_size,
        "higher_yes_bid_price": higher.yes_bid,
        "higher_yes_bid_size": higher_yes_bid_size,
        "lower_yes_bid_price": lower.yes_bid,
        "lower_yes_bid_size": lower_yes_bid_size,
        "higher_yes_ask_price": higher.yes_ask,
        "higher_yes_ask_size": higher_yes_ask_size,
        "depth_verified": executable_sides_meet_min_size,
        "average_signal_time_depth": average_signal_time_depth,
        "min_top_level_depth": min_top_level_depth,
        "insufficient_depth_fields": insufficient_fields,
        "quote_context_verified": bool(lower_diag.get("orderbook_snapshot_exists")) and bool(higher_diag.get("orderbook_snapshot_exists")),
    }
