from __future__ import annotations

import json
import math
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path

from app.config import Settings
from app.db import Database
from app.events.models import BrokerFill, TradeIdea
from app.execution.broker import PaperBroker, PaperExecutionResult
from app.execution.event_engine import EventExecutionEngine
from app.execution.router import ExecutionRouter
from app.persistence.contracts import EventEntryQueueRecord, TradeCandidateRecord
from app.persistence.event_repo import EventRepository
from app.persistence.kalshi_repo import KalshiRepository
from app.persistence.market_state_repo import MarketStateRepository
from app.persistence.reporting_repo import ReportingRepository
from app.reporting import PaperTradingReporter, build_event_run_manifest
from app.portfolio.state import PortfolioState
from app.risk.manager import RiskManager


@dataclass(slots=True)
class ValidationScenario:
    name: str
    symbol: str
    side: str
    quantity: int
    price_path: list[float]
    order_style: str = "marketable"
    limit_price: float | None = None
    top_level_size: int = 25
    depth_levels: int = 3
    half_spread_bps: float = 8.0
    submission_latency_seconds: float = 2.0
    max_hold_seconds: int = 0
    signal_timestamp_utc: datetime = datetime(2026, 3, 20, 18, 0, 0, tzinfo=timezone.utc)


class SequencePriceFeed:
    def __init__(self, values: list[float]) -> None:
        self.values = list(values)
        self.index = 0

    def __call__(self, symbol: str) -> float | None:
        if not self.values:
            return None
        if self.index >= len(self.values):
            return self.values[-1]
        value = self.values[self.index]
        self.index += 1
        return value


class LegacyOptimisticPaperBroker:
    def __init__(self, starting_capital: float, execution_enabled: bool, price_fetcher) -> None:
        self.cash = starting_capital
        self.execution_enabled = execution_enabled
        self._price_fetcher = price_fetcher

    def place_order(
        self,
        symbol: str,
        side: str,
        quantity: int,
        *,
        signal_ts: datetime | None = None,
        order_style: str = "marketable",
        limit_price: float | None = None,
    ) -> PaperExecutionResult:
        submitted_at = signal_ts.astimezone(timezone.utc) if signal_ts is not None else datetime.now(timezone.utc)
        audit_events = [
            {
                "timestamp_utc": submitted_at.isoformat(),
                "state": "submitted",
                "decision": "submit_order",
                "symbol": symbol,
                "side": side,
                "requested_quantity": quantity,
                "order_style": order_style,
            }
        ]
        if not self.execution_enabled or quantity <= 0:
            audit_events.append(
                {
                    "timestamp_utc": submitted_at.isoformat(),
                    "state": "rejected",
                    "decision": "reject_order",
                    "reason": "execution_disabled" if not self.execution_enabled else "non_positive_quantity",
                }
            )
            return PaperExecutionResult(
                fill=None,
                requested_quantity=quantity,
                filled_quantity=0,
                remaining_quantity=max(0, quantity),
                status="rejected",
                submitted_at=submitted_at,
                order_style=order_style,
                book=None,
                audit_events=audit_events,
            )
        price = self._price_fetcher(symbol)
        if price is None:
            audit_events.append(
                {
                    "timestamp_utc": submitted_at.isoformat(),
                    "state": "rejected",
                    "decision": "reject_order",
                    "reason": "price_unavailable",
                }
            )
            return PaperExecutionResult(
                fill=None,
                requested_quantity=quantity,
                filled_quantity=0,
                remaining_quantity=quantity,
                status="rejected",
                submitted_at=submitted_at,
                order_style=order_style,
                book=None,
                audit_events=audit_events,
            )
        cash_delta = price * quantity
        if side == "buy":
            self.cash -= cash_delta
        else:
            self.cash += cash_delta
        fill = BrokerFill(
            symbol=symbol,
            side=side,
            quantity=quantity,
            price=price,
            timestamp_utc=submitted_at,
            metadata={"legacy_fill_model": "optimistic_mid_fill", "requested_quantity": quantity},
        )
        audit_events.append(
            {
                "timestamp_utc": submitted_at.isoformat(),
                "state": "filled",
                "decision": "optimistic_spot_fill",
                "average_fill_price": price,
                "filled_quantity": quantity,
                "remaining_quantity": 0,
            }
        )
        return PaperExecutionResult(
            fill=fill,
            requested_quantity=quantity,
            filled_quantity=quantity,
            remaining_quantity=0,
            status="filled",
            submitted_at=submitted_at,
            order_style=order_style,
            book=None,
            audit_events=audit_events,
        )

    def get_price(self, symbol: str, *, liquidation_side: str | None = None) -> float | None:
        return self._price_fetcher(symbol)

    def planned_exit_at(self, max_hold_seconds: int) -> datetime:
        return datetime.now(timezone.utc) + timedelta(seconds=max_hold_seconds)

    def snapshot_order_book(self, symbol: str, signal_ts=None):
        from app.execution.broker import PaperBookLevel, PaperOrderBook

        ts = signal_ts.astimezone(timezone.utc) if signal_ts is not None else datetime.now(timezone.utc)
        price = self._price_fetcher(symbol)
        if price is None:
            return None
        return PaperOrderBook(
            symbol=symbol,
            mid_price=price,
            bid_price=price,
            ask_price=price,
            bid_levels=[PaperBookLevel(price=price, size=10_000)],
            ask_levels=[PaperBookLevel(price=price, size=10_000)],
            ts=ts,
            source="legacy_mid_quote",
        )


def run_paper_execution_comparison(output_dir: Path) -> dict:
    output_dir.mkdir(parents=True, exist_ok=True)
    scenarios = _default_scenarios()
    legacy_results = [_run_scenario(output_dir / "legacy", scenario, model="legacy") for scenario in scenarios]
    conservative_results = [_run_scenario(output_dir / "conservative", scenario, model="conservative") for scenario in scenarios]
    comparison = _build_comparison(scenarios, legacy_results, conservative_results)
    output_path = output_dir / "event_paper_execution_comparison.json"
    output_path.write_text(json.dumps(comparison, indent=2), encoding="utf-8")
    return comparison


def _run_scenario(base_dir: Path, scenario: ValidationScenario, *, model: str) -> dict:
    scenario_dir = base_dir / scenario.name
    scenario_dir.mkdir(parents=True, exist_ok=True)
    db = Database(scenario_dir / "validation.db")
    event_repo = EventRepository(db)
    reporting_repo = ReportingRepository(db)
    feed = SequencePriceFeed(scenario.price_path)
    if model == "legacy":
        broker = LegacyOptimisticPaperBroker(100000.0, True, feed)
        settings = Settings(
            enable_event_driven_mode=True,
            event_trading_only_mode=True,
            event_execution_enabled=True,
            event_paper_submission_latency_seconds=0.0,
            event_paper_half_spread_bps=0.0,
            event_paper_top_level_size=scenario.quantity,
            event_paper_depth_levels=1,
            event_paper_passive_recheck_seconds=0.0,
        )
    else:
        broker = PaperBroker(
            100000.0,
            True,
            submission_latency_seconds=scenario.submission_latency_seconds,
            half_spread_bps=scenario.half_spread_bps,
            top_level_size=scenario.top_level_size,
            depth_levels=scenario.depth_levels,
            passive_recheck_seconds=1.0,
            price_fetcher=feed,
        )
        settings = Settings(
            enable_event_driven_mode=True,
            event_trading_only_mode=True,
            event_execution_enabled=True,
            event_paper_submission_latency_seconds=scenario.submission_latency_seconds,
            event_paper_half_spread_bps=scenario.half_spread_bps,
            event_paper_top_level_size=scenario.top_level_size,
            event_paper_depth_levels=scenario.depth_levels,
            event_paper_passive_recheck_seconds=1.0,
        )
    signal_repo = KalshiRepository(db)
    engine = EventExecutionEngine(
        event_repo,
        signal_repo,
        broker,
        ExecutionRouter(
            None,
            signal_repo,
            PortfolioState(None),
            RiskManager(1.0, 10.0, 10.0, 10),
            __import__("logging").getLogger("event-validation"),
            False,
            market_state_repo=MarketStateRepository(db),
        ),
    )
    candidate_id = event_repo.persist_trade_candidate(
        TradeCandidateRecord(
            classified_event_id=1,
            symbol=scenario.symbol,
            side=scenario.side,
            event_type="macro",
            status="confirmed",
            confidence=1.0,
            confirmation_score=1.0,
            metadata_json={"scenario": scenario.name},
            created_at=datetime.now(timezone.utc).isoformat(),
        )
    )
    event_repo.persist_entry_queue_item(
        EventEntryQueueRecord(
            trade_candidate_id=candidate_id,
            due_at=scenario.signal_timestamp_utc.isoformat(),
            symbol=scenario.symbol,
            side=scenario.side,
            quantity=scenario.quantity,
            tranche_index=1,
            total_tranches=1,
            status="pending",
            metadata_json={
                "news_event_id": 1,
                "paper_order_style": scenario.order_style,
                "paper_limit_price": scenario.limit_price,
            },
            created_at=scenario.signal_timestamp_utc.isoformat(),
        )
    )
    processed = engine.process_due_entries(
        stop_loss_pct=0.01,
        max_hold_seconds=scenario.max_hold_seconds,
        logger=_NoopLogger(),
        now=scenario.signal_timestamp_utc + timedelta(seconds=max(1.0, scenario.submission_latency_seconds + 1.0)),
    )
    closed = engine.close_due_positions(
        _NoopLogger(),
        now=scenario.signal_timestamp_utc + timedelta(seconds=max(2.0, scenario.submission_latency_seconds + 2.0)),
    )
    report = PaperTradingReporter(
        reporting_repo,
        scenario_dir / "report.json",
        run_manifest=build_event_run_manifest(settings),
    ).write({"event_driven_loop_stats": {"scenario_name": scenario.name, "filled_entries": processed, "closed_positions": closed}})
    event_bundle = reporting_repo.load_event_reporting_bundle()
    return {
        "scenario": scenario.name,
        "db_path": str((scenario_dir / "validation.db").resolve()),
        "report_path": str((scenario_dir / "report.json").resolve()),
        "processed_entries": processed,
        "closed_positions": closed,
        "queue_rows": [_row_to_dict(row) for row in event_bundle.entry_queue],
        "position_rows": [_row_to_dict(row) for row in event_bundle.positions],
        "outcome_rows": [_row_to_dict(row) for row in event_bundle.trade_outcomes],
        "audit_rows": [_row_to_dict(row) for row in event_bundle.paper_execution_audit],
        "report": report,
    }


def _build_comparison(scenarios: list[ValidationScenario], legacy_results: list[dict], conservative_results: list[dict]) -> dict:
    scenario_comparisons = []
    for scenario, legacy, conservative in zip(scenarios, legacy_results, conservative_results, strict=True):
        scenario_comparisons.append(
            _scenario_comparison(
                scenario,
                legacy,
                conservative,
            )
        )
    summary = {
        "legacy": _aggregate_results(legacy_results),
        "conservative": _aggregate_results(conservative_results),
    }
    summary["delta"] = _delta_summary(summary["legacy"], summary["conservative"])
    return {
        "comparison_schema_version": "event-paper-execution-comparison-v1",
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "scenario_count": len(scenarios),
        "summary": summary,
        "scenario_comparisons": scenario_comparisons,
    }


def _scenario_comparison(scenario: ValidationScenario, legacy: dict, conservative: dict) -> dict:
    legacy_metrics = _extract_metrics(legacy)
    conservative_metrics = _extract_metrics(conservative)
    return {
        "scenario": scenario.name,
        "symbol": scenario.symbol,
        "side": scenario.side,
        "requested_quantity": scenario.quantity,
        "legacy": legacy_metrics,
        "conservative": conservative_metrics,
        "delta": _delta_summary(legacy_metrics, conservative_metrics),
    }


def _aggregate_results(results: list[dict]) -> dict:
    metrics = [_extract_metrics(result) for result in results]
    total_requested = sum(item["requested_quantity"] for item in metrics)
    total_filled = sum(item["filled_entry_quantity"] for item in metrics)
    closed_returns = [item["average_return_pct"] for item in metrics if item["closed_trade_count"] > 0]
    pnl_values = [item["net_pnl_dollars"] for item in metrics]
    return {
        "scenario_count": len(metrics),
        "trades_accepted": sum(item["accepted_trade_count"] for item in metrics),
        "closed_trade_count": sum(item["closed_trade_count"] for item in metrics),
        "fill_rate": (total_filled / total_requested) if total_requested else 0.0,
        "partial_fill_frequency": (sum(1 for item in metrics if item["partial_fill_count"] > 0) / len(metrics)) if metrics else 0.0,
        "avg_realized_entry_disadvantage": _avg(item["avg_realized_entry_disadvantage"] for item in metrics),
        "avg_realized_exit_disadvantage": _avg(item["avg_realized_exit_disadvantage"] for item in metrics),
        "net_pnl_dollars": sum(pnl_values),
        "average_return_pct": _avg(closed_returns),
        "hit_rate": (sum(1 for value in closed_returns if value > 0) / len(closed_returns)) if closed_returns else 0.0,
        "nonstandard_sharpe_like": _sharpe_like(closed_returns),
        "max_drawdown_proxy": _max_drawdown_proxy(pnl_values),
    }


def _extract_metrics(result: dict) -> dict:
    queue_rows = result["queue_rows"]
    outcome_rows = result["outcome_rows"]
    audit_rows = result["audit_rows"]
    accepted_trade_count = sum(1 for row in queue_rows if row["status"] in {"filled", "partially_filled"})
    filled_entry_quantity = sum(
        int(_metadata_value(row["metadata_json"], "filled_quantity", 0))
        for row in queue_rows
        if row["status"] in {"filled", "partially_filled"}
    )
    requested_quantity = sum(int(row["quantity"]) for row in queue_rows)
    partial_fill_count = sum(1 for row in queue_rows if row["status"] == "partially_filled")
    entry_disadvantages = []
    exit_disadvantages = []
    for row in audit_rows:
        metadata = json.loads(row["metadata_json"]) if row["metadata_json"] else {}
        average_fill_price = metadata.get("average_fill_price")
        if average_fill_price is None:
            continue
        if row["queue_id"] is not None:
            reference_mid = metadata.get("mid_price", metadata.get("best_bid"))
            if reference_mid is not None:
                entry_disadvantages.append(_price_disadvantage(row["side"], float(average_fill_price), float(reference_mid)))
        elif row["position_id"] is not None:
            reference_mid = metadata.get("mid_price", metadata.get("best_bid"))
            if reference_mid is not None:
                exit_disadvantages.append(_price_disadvantage(row["side"], float(average_fill_price), float(reference_mid)))
    net_pnl = 0.0
    closed_returns: list[float] = []
    for row in outcome_rows:
        return_pct = float(row["return_pct"])
        closed_returns.append(return_pct)
        quantity = _position_quantity_for_outcome(row["position_id"], result["position_rows"])
        net_pnl += (float(row["exit_price"]) - float(row["entry_price"])) * quantity * (1 if _outcome_side(row) == "buy" else -1)
    return {
        "requested_quantity": requested_quantity,
        "trades_accepted": accepted_trade_count,
        "accepted_trade_count": accepted_trade_count,
        "closed_trade_count": len(outcome_rows),
        "filled_entry_quantity": filled_entry_quantity,
        "fill_rate": (filled_entry_quantity / requested_quantity) if requested_quantity else 0.0,
        "partial_fill_frequency": 1.0 if partial_fill_count > 0 else 0.0,
        "partial_fill_count": partial_fill_count,
        "avg_realized_entry_disadvantage": _avg(entry_disadvantages),
        "avg_realized_exit_disadvantage": _avg(exit_disadvantages),
        "net_pnl_dollars": round(net_pnl, 6),
        "average_return_pct": _avg(closed_returns),
        "hit_rate": (sum(1 for value in closed_returns if value > 0) / len(closed_returns)) if closed_returns else 0.0,
        "nonstandard_sharpe_like": _sharpe_like(closed_returns),
        "max_drawdown_proxy": _max_drawdown_proxy([float(row["return_pct"]) for row in outcome_rows]),
        "queue_status_breakdown": result["report"]["event_driven_summary"]["queue_status_breakdown"],
        "shared_execution_orders": result["report"]["execution_report"]["by_strategy"].get("event_driven", {}).get("orders", 0),
    }


def _delta_summary(left: dict, right: dict) -> dict:
    fields = [
        "trades_accepted",
        "closed_trade_count",
        "fill_rate",
        "partial_fill_frequency",
        "avg_realized_entry_disadvantage",
        "avg_realized_exit_disadvantage",
        "net_pnl_dollars",
        "average_return_pct",
        "hit_rate",
        "nonstandard_sharpe_like",
        "max_drawdown_proxy",
    ]
    delta = {}
    for field in fields:
        left_value = float(left.get(field, 0.0))
        right_value = float(right.get(field, 0.0))
        delta[field] = right_value - left_value
    return delta


def _default_scenarios() -> list[ValidationScenario]:
    return [
        ValidationScenario(
            name="latency_taker_long",
            symbol="SPY",
            side="buy",
            quantity=10,
            price_path=[100.0, 100.4, 100.4],
            top_level_size=10,
            depth_levels=1,
            half_spread_bps=10.0,
            submission_latency_seconds=3.0,
            signal_timestamp_utc=datetime(2026, 3, 20, 18, 0, 0, tzinfo=timezone.utc),
        ),
        ValidationScenario(
            name="depth_exhaustion_partial",
            symbol="QQQ",
            side="buy",
            quantity=8,
            price_path=[200.0, 200.0, 200.2, 200.2],
            top_level_size=3,
            depth_levels=1,
            half_spread_bps=12.0,
            submission_latency_seconds=2.0,
            signal_timestamp_utc=datetime(2026, 3, 20, 18, 0, 0, tzinfo=timezone.utc),
        ),
        ValidationScenario(
            name="walk_the_book_short_exit",
            symbol="XLE",
            side="sell",
            quantity=9,
            price_path=[90.0, 89.6, 89.6],
            top_level_size=4,
            depth_levels=3,
            half_spread_bps=15.0,
            submission_latency_seconds=2.0,
            signal_timestamp_utc=datetime(2026, 3, 20, 18, 0, 0, tzinfo=timezone.utc),
        ),
        ValidationScenario(
            name="passive_non_fill",
            symbol="TLT",
            side="buy",
            quantity=5,
            price_path=[100.0, 100.0],
            order_style="passive",
            limit_price=99.95,
            top_level_size=5,
            depth_levels=1,
            half_spread_bps=10.0,
            submission_latency_seconds=2.0,
            signal_timestamp_utc=datetime(2026, 3, 20, 18, 0, 0, tzinfo=timezone.utc),
        ),
    ]


def _row_to_dict(row) -> dict:
    return {key: row[key] for key in row.keys()}


def _avg(values) -> float:
    values = list(values)
    return math.fsum(values) / len(values) if values else 0.0


def _sharpe_like(returns: list[float]) -> float:
    if len(returns) < 2:
        return 0.0
    mean = _avg(returns)
    variance = math.fsum((value - mean) ** 2 for value in returns) / (len(returns) - 1)
    if variance <= 0:
        return 0.0
    return mean / math.sqrt(variance)


def _max_drawdown_proxy(values: list[float]) -> float:
    if not values:
        return 0.0
    cumulative = 0.0
    peak = 0.0
    max_drawdown = 0.0
    for value in values:
        cumulative += value
        peak = max(peak, cumulative)
        max_drawdown = min(max_drawdown, cumulative - peak)
    return max_drawdown


def _price_disadvantage(side: str, fill_price: float, reference_mid: float) -> float:
    if reference_mid == 0:
        return 0.0
    if side == "buy":
        return (fill_price / reference_mid) - 1.0
    return (reference_mid / fill_price) - 1.0


def _metadata_value(raw: str, key: str, default):
    if not raw:
        return default
    try:
        return json.loads(raw).get(key, default)
    except json.JSONDecodeError:
        return default


def _position_quantity_for_outcome(position_id: int, positions: list[dict]) -> int:
    for row in positions:
        if int(row["id"]) == int(position_id):
            return int(row["quantity"])
    return 0


def _outcome_side(row: dict) -> str:
    metadata = json.loads(row["metadata_json"]) if row.get("metadata_json") else {}
    return str(metadata.get("side", "buy"))


class _NoopLogger:
    def info(self, *args, **kwargs) -> None:
        return None
