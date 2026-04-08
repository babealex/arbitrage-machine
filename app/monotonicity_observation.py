from __future__ import annotations

import time
from decimal import Decimal
from datetime import datetime, timezone

from app.market_data.fees import balance_aligned_fee_dollars, edge_threshold
from app.market_data.service import MarketDataService
from app.persistence.contracts import MonotonicityObservationRecord
from app.persistence.kalshi_repo import KalshiRepository


def build_monotonicity_observation(signal) -> MonotonicityObservationRecord:
    accepted_at = signal.ts.isoformat()
    lower = signal.legs[0].instrument.symbol if signal.legs else signal.ticker
    higher = signal.legs[1].instrument.symbol if len(signal.legs) > 1 else signal.ticker
    product_family = lower.split("-")[0]
    return MonotonicityObservationRecord(
        signal_key=f"{accepted_at}:{lower}:{higher}",
        ticker=signal.ticker,
        lower_ticker=lower,
        higher_ticker=higher,
        product_family=product_family,
        accepted_at=accepted_at,
        edge_before_fees=float(signal.edge.edge_before_fees),
        edge_after_fees=signal.probability_edge,
        lower_yes_ask_price=signal.legs[0].price_cents if signal.legs else None,
        lower_yes_ask_size=signal.quantity if signal.legs else 0,
        higher_yes_bid_price=signal.legs[1].price_cents if len(signal.legs) > 1 else None,
        higher_yes_bid_size=signal.quantity if len(signal.legs) > 1 else 0,
        next_refresh_status=None,
        next_refresh_observed_at=None,
        time_to_disappear_seconds=None,
        latest_status="accepted",
        latest_observed_at=accepted_at,
        metadata_json={
            "quantity": signal.quantity,
            "source": signal.source,
            "confidence": signal.confidence,
            "detection_ts": accepted_at,
        },
    )


def recheck_monotonicity_signal(
    signal,
    market_data: MarketDataService,
    min_edge_bps: float,
    slippage_buffer_bps: float,
    min_top_level_depth: int,
    min_size_multiple: float,
    confirmation_delay_seconds: float,
) -> dict:
    lower_ticker = signal.legs[0].instrument.symbol if signal.legs else signal.ticker
    higher_ticker = signal.legs[1].instrument.symbol if len(signal.legs) > 1 else signal.ticker
    detection_ts = signal.ts
    if confirmation_delay_seconds > 0:
        time.sleep(confirmation_delay_seconds)
    confirmation_ts = datetime.now(timezone.utc)
    confirmation_delay = (confirmation_ts - detection_ts).total_seconds()
    result = evaluate_monotonicity_pair(
        lower_ticker,
        higher_ticker,
        signal.quantity,
        market_data,
        min_edge_bps,
        slippage_buffer_bps,
        min_top_level_depth,
        min_size_multiple,
    )
    return {
        "immediate_recheck_survived": result["survived"],
        "immediate_recheck_reason": result["reason"],
        "confirmation_delay_seconds": confirmation_delay,
        "confirmation_lower_yes_ask_price": result["lower_yes_ask_price"],
        "confirmation_lower_yes_ask_size": result["lower_yes_ask_size"],
        "confirmation_higher_yes_bid_price": result["higher_yes_bid_price"],
        "confirmation_higher_yes_bid_size": result["higher_yes_bid_size"],
        "confirmation_edge_before_fees": result["edge_before_fees"],
        "confirmation_edge_after_fees": result["edge_after_fees"],
        "confirmation_size_multiple_lower": result["size_multiple_lower"],
        "confirmation_size_multiple_higher": result["size_multiple_higher"],
    }


def followup_monotonicity_signal(
    signal,
    market_data: MarketDataService,
    min_edge_bps: float,
    slippage_buffer_bps: float,
    min_top_level_depth: int,
    min_size_multiple: float,
    followup_delay_seconds: float,
) -> dict:
    lower_ticker = signal.legs[0].instrument.symbol if signal.legs else signal.ticker
    higher_ticker = signal.legs[1].instrument.symbol if len(signal.legs) > 1 else signal.ticker
    detection_ts = signal.ts
    if followup_delay_seconds > 0:
        time.sleep(followup_delay_seconds)
    followup_ts = datetime.now(timezone.utc)
    followup_delay = (followup_ts - detection_ts).total_seconds()
    result = evaluate_monotonicity_pair(
        lower_ticker,
        higher_ticker,
        signal.quantity,
        market_data,
        min_edge_bps,
        slippage_buffer_bps,
        min_top_level_depth,
        min_size_multiple,
    )
    return {
        "followup_survived": result["survived"],
        "followup_reason": result["reason"],
        "followup_delay_seconds": followup_delay,
        "followup_lower_yes_ask_price": result["lower_yes_ask_price"],
        "followup_lower_yes_ask_size": result["lower_yes_ask_size"],
        "followup_higher_yes_bid_price": result["higher_yes_bid_price"],
        "followup_higher_yes_bid_size": result["higher_yes_bid_size"],
        "followup_edge_before_fees": result["edge_before_fees"],
        "followup_edge_after_fees": result["edge_after_fees"],
        "followup_size_multiple_lower": result["size_multiple_lower"],
        "followup_size_multiple_higher": result["size_multiple_higher"],
    }


def evaluate_monotonicity_pair(
    lower_ticker: str,
    higher_ticker: str,
    quantity: int,
    market_data: MarketDataService,
    min_edge_bps: float,
    slippage_buffer_bps: float,
    min_top_level_depth: int,
    min_size_multiple: float,
) -> dict:
    lower_snapshot = market_data.refresh_orderbook(lower_ticker)
    higher_snapshot = market_data.refresh_orderbook(higher_ticker)
    lower_best = market_data.orderbooks.get(lower_ticker, lower_snapshot)
    higher_best = market_data.orderbooks.get(higher_ticker, higher_snapshot)
    lower_yes_ask_price = lower_best.best_yes_ask()
    higher_yes_bid_price = higher_best.best_yes_bid()
    lower_yes_ask_price_cents = int(round(lower_yes_ask_price * 100)) if lower_yes_ask_price is not None else None
    higher_yes_bid_price_cents = int(round(higher_yes_bid_price * 100)) if higher_yes_bid_price is not None else None
    lower_yes_ask_size = lower_best.best_yes_ask_size()
    higher_yes_bid_size = higher_best.best_yes_bid_size()
    size_multiple_lower = (lower_yes_ask_size / quantity) if quantity > 0 else 0.0
    size_multiple_higher = (higher_yes_bid_size / quantity) if quantity > 0 else 0.0
    result = {
        "survived": False,
        "reason": "missing_prices",
        "lower_yes_ask_price": lower_yes_ask_price_cents,
        "lower_yes_ask_size": lower_yes_ask_size,
        "higher_yes_bid_price": higher_yes_bid_price_cents,
        "higher_yes_bid_size": higher_yes_bid_size,
        "edge_before_fees": None,
        "edge_after_fees": None,
        "size_multiple_lower": size_multiple_lower,
        "size_multiple_higher": size_multiple_higher,
    }
    if lower_yes_ask_price_cents is None or higher_yes_bid_price_cents is None:
        return result
    gate = edge_threshold(
        edge_before_fees=Decimal(higher_yes_bid_price_cents - lower_yes_ask_price_cents) / Decimal("100"),
        fee_estimate=balance_aligned_fee_dollars(Decimal(lower_yes_ask_price_cents) / Decimal("100"), side="buy")
        + balance_aligned_fee_dollars(Decimal(higher_yes_bid_price_cents) / Decimal("100"), side="sell"),
        execution_buffer=Decimal(str(slippage_buffer_bps)) / Decimal("10000"),
    )
    result["edge_before_fees"] = float(gate.edge_before_fees)
    result["edge_after_fees"] = float(gate.edge_after_fees)
    if lower_yes_ask_size < min_top_level_depth or higher_yes_bid_size < min_top_level_depth:
        result["reason"] = "insufficient_signal_time_depth"
    elif size_multiple_lower < min_size_multiple or size_multiple_higher < min_size_multiple:
        result["reason"] = "insufficient_size_multiple"
    elif not gate.passes_bps(min_edge_bps):
        result["reason"] = "edge_below_threshold"
    else:
        result["survived"] = True
        result["reason"] = "signal_survived_check"
    return result


def update_monotonicity_observations(
    pending: dict[str, MonotonicityObservationRecord],
    market_lookup: dict[str, object],
    min_edge_bps: float,
    slippage_buffer_bps: float,
    min_top_level_depth: int,
    repo: KalshiRepository,
    logger,
) -> None:
    to_remove: list[str] = []
    now = datetime.now(timezone.utc)
    for signal_key, observation in pending.items():
        lower = market_lookup.get(observation.lower_ticker)
        higher = market_lookup.get(observation.higher_ticker)
        status, status_metadata = monotonicity_quote_status(lower, higher, observation, min_edge_bps, slippage_buffer_bps, min_top_level_depth)
        accepted_at = datetime.fromisoformat(observation.accepted_at)
        elapsed = (now - accepted_at).total_seconds()
        observation.latest_status = status
        observation.latest_observed_at = now.isoformat()
        observation.metadata_json.update(status_metadata)
        if observation.next_refresh_status is None:
            observation.next_refresh_status = status
            observation.next_refresh_observed_at = now.isoformat()
            logger.info(
                "monotonicity_signal_next_refresh",
                extra={
                    "event": {
                        "signal_key": signal_key,
                        "timestamp": observation.accepted_at,
                        "ladder_pair": [observation.lower_ticker, observation.higher_ticker],
                        "lower_yes_ask_price": observation.lower_yes_ask_price,
                        "lower_yes_ask_size": observation.lower_yes_ask_size,
                        "higher_yes_bid_price": observation.higher_yes_bid_price,
                        "higher_yes_bid_size": observation.higher_yes_bid_size,
                        "edge_before_fees": observation.edge_before_fees,
                        "edge_after_fees": observation.edge_after_fees,
                        "next_refresh_quote_status": status,
                        "time_to_disappear_seconds": elapsed if status == "gone" else None,
                    }
                },
            )
        if observation.next_refresh_status == "gone" and observation.time_to_disappear_seconds is None:
            observation.time_to_disappear_seconds = elapsed
            to_remove.append(signal_key)
        repo.persist_monotonicity_observation(observation)
    for signal_key in to_remove:
        pending.pop(signal_key, None)


def monotonicity_quote_status(lower, higher, observation: MonotonicityObservationRecord, min_edge_bps: float, slippage_buffer_bps: float, min_top_level_depth: int) -> tuple[str, dict]:
    if lower is None or higher is None:
        return "gone", {"current_lower_present": lower is not None, "current_higher_present": higher is not None}
    lower_diag = dict(lower.metadata.get("quote_diagnostics", {}))
    higher_diag = dict(higher.metadata.get("quote_diagnostics", {}))
    current_low_price = lower.yes_ask
    current_high_price = higher.yes_bid
    current_low_size = int(lower_diag.get("best_yes_ask_size") or 0)
    current_high_size = int(higher_diag.get("best_yes_bid_size") or 0)
    metadata = {
        "current_lower_yes_ask_price": current_low_price,
        "current_lower_yes_ask_size": current_low_size,
        "current_higher_yes_bid_price": current_high_price,
        "current_higher_yes_bid_size": current_high_size,
    }
    if current_low_price is None or current_high_price is None:
        return "gone", metadata
    gate = edge_threshold(
        edge_before_fees=Decimal(str(current_high_price - current_low_price)) / Decimal("100"),
        fee_estimate=balance_aligned_fee_dollars(Decimal(str(current_low_price)) / Decimal("100"), side="buy")
        + balance_aligned_fee_dollars(Decimal(str(current_high_price)) / Decimal("100"), side="sell"),
        execution_buffer=Decimal(str(slippage_buffer_bps)) / Decimal("10000"),
    )
    metadata.update({"current_raw_edge": float(gate.edge_before_fees), "current_edge_after_fees": float(gate.edge_after_fees)})
    if current_low_size < min_top_level_depth or current_high_size < min_top_level_depth:
        return "partially_present", metadata
    if gate.passes_bps(min_edge_bps):
        return "still_present", metadata
    return "partially_present", metadata


def quote_rejection_events(evaluation, market_lookup: dict[str, object]) -> list[dict]:
    if evaluation.reason not in {"missing_prices", "missing_quotes"}:
        return []
    ticker_order: list[str] = []
    metadata = evaluation.metadata or {}
    for key in ("lower", "higher"):
        ticker = metadata.get(key)
        if ticker:
            ticker_order.append(ticker)
    ticker_order.extend(metadata.get("members", []))
    events: list[dict] = []
    for ticker in ticker_order:
        market = market_lookup.get(ticker)
        if market is None:
            continue
        diagnostics = dict(market.metadata.get("quote_diagnostics", {}))
        diagnostics.update(
            {
                "rejection_reason": evaluation.reason,
                "strategy": evaluation.strategy.value,
            }
        )
        events.append(diagnostics)
    return events
