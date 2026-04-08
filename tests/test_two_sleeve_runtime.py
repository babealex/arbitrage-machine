import json
import logging
from datetime import date, datetime, timezone
from pathlib import Path
from uuid import uuid4
from urllib.request import urlopen
from decimal import Decimal

from app.db import Database
from app.config import Settings
from app.models import StrategyEvaluation, StrategyName
from app.persistence.kalshi_repo import KalshiRepository
from app.persistence.sleeve_repo import SleeveRepository
from app.persistence.trend_repo import TrendRepository
from app.runtime.dashboard import start_control_panel
from app.runtime.two_sleeve import TrendBarUpdater, _build_multi_cycle_research_summary, _build_trend_benchmark_summary, _build_trend_construction_summary, _build_trend_focus_summary, _build_trend_replay_diversity_report, _build_trend_signal_summary, _build_trend_sparsity_summary, _build_trend_threshold_sensitivity_report, _build_trend_validation_summary, _build_trend_weight_shape_sensitivity_report, _dashboard_state, _run_kalshi_connectivity_check, _summarize_kalshi_evaluations, _sync_observability, _write_measurement_report, _write_strategy_transparency_pack
from app.sleeves import SLEEVE_REGISTRY, enabled_sleeves, sleeve_name_for_strategy
from app.strategies.trend.models import DailyBar


def test_two_sleeve_defaults_are_truthful() -> None:
    settings = Settings()

    assert settings.enable_two_sleeve_runtime is True
    assert settings.enable_trend_sleeve is True
    assert settings.enable_kalshi_sleeve is True
    assert settings.enable_options_sleeve is False
    assert settings.enable_convexity_sleeve is False
    assert settings.trend_weight_mode == "gentle_score"
    assert settings.trend_symbols == ["SPY", "QQQ", "IWM", "EFA", "EEM", "TLT", "IEF", "TIP", "LQD", "HYG", "GLD", "DBC"]
    assert settings.two_sleeve_capital_weights == {"trend": 0.75, "kalshi_event": 0.25}

    enabled = enabled_sleeves(settings)
    assert set(enabled) == {"trend", "kalshi_event"}
    assert SLEEVE_REGISTRY["trend"].enabled_by_default is True
    assert SLEEVE_REGISTRY["kalshi_event"].enabled_by_default is True
    assert SLEEVE_REGISTRY["options"].enabled_by_default is False
    assert SLEEVE_REGISTRY["convexity"].enabled_by_default is False
    assert sleeve_name_for_strategy("momentum") == "trend"
    assert sleeve_name_for_strategy("partition") == "kalshi_event"


def test_control_panel_serves_state_and_routes() -> None:
    state_path = Path("data") / f"test_control_panel_state_{uuid4().hex}.json"
    state_path.parent.mkdir(parents=True, exist_ok=True)
    try:
        state_path.write_text(
            json.dumps(
                {
                    "runtime_name": "two_sleeve_paper",
                    "run_mode": "data_unavailable",
                    "master_thesis": "Build a capital-allocation machine, not a strategy zoo.",
                    "benchmark_question": "What can this machine do that a simple SPY/QQQ allocator cannot?",
                    "operating_hierarchy": ["trend", "kalshi_event"],
                    "updated_at_utc": "2026-03-28T20:00:00+00:00",
                    "sleeves": [
                        {"sleeve_name": "trend", "role": "core_engine", "mandate": "Primary scalable capital-allocation sleeve across liquid cross-asset ETFs.", "scale_profile": "High relative scalability; should carry most paper risk budget and most validation effort.", "promotion_gate": "Must beat a simple passive benchmark on state changes, allocation quality, and risk-adjusted behavior before the repo deserves more complexity.", "maturity": "active_core", "run_mode": "replay_paper", "status": "running", "capital_assigned": "75000", "daily_pnl": "0", "cumulative_pnl": "0", "opportunities_seen": 3, "trades_attempted": 1, "trades_filled": 1, "open_positions": 0, "trade_count": 1, "error_count": 0, "realized_pnl": "0", "unrealized_pnl": "0", "fees": None, "avg_slippage_bps": None, "fill_rate": None, "avg_expected_edge": None, "avg_realized_edge": None, "expected_realized_gap": None, "no_trade_reasons": {"stale_data": 1}, "top_no_trade_reasons": [{"reason": "stale_data", "count": 1}], "blocker_notes": ["trend running from cached historical bars"], "recent_trades": [], "recent_errors": [], "pnl_curve": []},
                        {"sleeve_name": "kalshi_event", "role": "booster_sleeve", "mandate": "Opportunistic structural and event-contract alpha sleeve, especially in ladders and cross-market mispricings.", "scale_profile": "Capacity-limited; useful for additive alpha and belief extraction, not the backbone allocator.", "promotion_gate": "Must show repeatable positive opportunity quality after fees and realistic depth checks without pretending it can absorb large size.", "maturity": "active_core", "run_mode": "data_unavailable", "status": "paused", "capital_assigned": "25000", "daily_pnl": "0", "cumulative_pnl": "0", "opportunities_seen": 0, "trades_attempted": 0, "trades_filled": 0, "open_positions": 0, "trade_count": 0, "error_count": 1, "realized_pnl": "0", "unrealized_pnl": "0", "fees": None, "avg_slippage_bps": None, "fill_rate": None, "avg_expected_edge": None, "avg_realized_edge": None, "expected_realized_gap": None, "no_trade_reasons": {"data_failure": 1}, "top_no_trade_reasons": [{"reason": "data_failure", "count": 1}], "blocker_notes": ["Kalshi discovery returned no markets"], "recent_trades": [], "recent_errors": [], "pnl_curve": []},
                    ],
                    "comparison": [],
                    "ledger": [],
                    "ledger_explanation": "No trade ledger rows yet.",
                    "errors": [],
                    "disabled_modules": ["options", "convexity"],
                }
            ),
            encoding="utf-8",
        )
        server = start_control_panel(state_path=state_path, host="127.0.0.1", port=0, logger=logging.getLogger("test"))
        port = server.server_address[1]
        try:
            with urlopen(f"http://127.0.0.1:{port}/api/state") as response:
                payload = json.loads(response.read().decode("utf-8"))
            assert payload["runtime_name"] == "two_sleeve_paper"
            assert [item["sleeve_name"] for item in payload["sleeves"]] == ["trend", "kalshi_event"]

            with urlopen(f"http://127.0.0.1:{port}/ledger") as response:
                body = response.read().decode("utf-8")
            assert "Trade Ledger" in body
            assert "ledger-sleeve" in body
            assert "Kalshi/Event" in body
            with urlopen(f"http://127.0.0.1:{port}/comparison") as response:
                comparison_body = response.read().decode("utf-8")
            assert "Benchmark Question" in comparison_body
        finally:
            server.shutdown()
            server.server_close()
    finally:
        state_path.unlink(missing_ok=True)


def test_trend_bar_refresh_uses_cached_mode_when_bars_exist(monkeypatch) -> None:
    db_path = Path("data") / f"test_trend_cached_{uuid4().hex}.db"
    monkeypatch.setenv("DB_PATH", str(db_path))
    monkeypatch.setenv("TREND_DATA_MODE", "cached")
    settings = Settings()
    db = Database(settings.db_path)
    repo = TrendRepository(db)
    repo.persist_bars([DailyBar(symbol="SPY", bar_date=date(2026, 3, 27), close=Decimal("500"))])

    stats = TrendBarUpdater(repo, logging.getLogger("test"), settings).refresh(["SPY"], as_of=datetime(2026, 3, 28, tzinfo=timezone.utc))

    assert stats["run_mode"] == "replay_paper"
    assert stats["live_fetch_attempted"] is False
    assert stats["cached_history_available"] is True


def test_dashboard_state_and_measurement_report_include_run_modes(monkeypatch) -> None:
    db_path = Path("data") / f"test_two_sleeve_state_{uuid4().hex}.db"
    report_path = Path("data") / f"test_first_measurement_{uuid4().hex}.json"
    transparency_path = Path("data") / f"test_transparency_pack_{uuid4().hex}.json"
    monkeypatch.setenv("DB_PATH", str(db_path))
    monkeypatch.setenv("FIRST_MEASUREMENT_REPORT_PATH", str(report_path))
    monkeypatch.setenv("STRATEGY_TRANSPARENCY_PACK_PATH", str(transparency_path))
    settings = Settings()
    settings.ensure_directories()
    db = Database(settings.db_path)
    repo = SleeveRepository(db)
    repo.upsert_runtime_status(
        sleeve_name="trend",
        maturity="active_core",
        run_mode="replay_paper",
        status="paused",
        enabled=True,
        paper_capable=True,
        live_capable=False,
        capital_weight=Decimal("0.75"),
        capital_assigned=Decimal("75000"),
        opportunities_seen=3,
        trades_attempted=1,
        trades_filled=1,
        open_positions=0,
        realized_pnl=Decimal("10"),
        unrealized_pnl=Decimal("0"),
        fees=None,
        avg_slippage_bps=None,
        fill_rate=Decimal("1"),
        trade_count=1,
        error_count=2,
        avg_expected_edge=Decimal("0.02"),
        avg_realized_edge=Decimal("0.01"),
        expected_realized_gap=Decimal("0.01"),
        no_trade_reasons={"stale_data": 2},
        details={"blocker_notes": ["trend running from cached historical bars"]},
    )
    repo.append_metrics_snapshot(
        sleeve_name="trend",
        run_mode="replay_paper",
        status="paused",
        capital_assigned=Decimal("75000"),
        opportunities_seen=3,
        trades_attempted=1,
        trades_filled=1,
        open_positions=0,
        realized_pnl=Decimal("10"),
        unrealized_pnl=Decimal("0"),
        fees=None,
        avg_slippage_bps=None,
        fill_rate=Decimal("1"),
        trade_count=1,
        error_count=2,
        avg_expected_edge=Decimal("0.02"),
        avg_realized_edge=Decimal("0.01"),
        expected_realized_gap=Decimal("0.01"),
        no_trade_reasons={"stale_data": 2},
        details={"blocker_notes": ["trend running from cached historical bars"]},
    )

    state = _dashboard_state(settings=settings, sleeve_repo=repo)
    report = _write_measurement_report(
        settings=settings,
        state=state,
        started_at=datetime(2026, 3, 28, 20, 0, tzinfo=timezone.utc),
        ended_at=datetime(2026, 3, 28, 20, 5, tzinfo=timezone.utc),
        cycles_completed=3,
    )
    transparency_pack = _write_strategy_transparency_pack(settings=settings, state=state)

    assert state["run_mode"] == "replay_paper"
    assert state["operating_hierarchy"] == ["trend", "kalshi_event"]
    assert "SPY/QQQ allocator" in state["benchmark_question"]
    assert state["sleeves"][0]["role"] == "core_engine"
    assert state["sleeves"][0]["opportunities_seen"] == 3
    assert state["sleeves"][0]["top_no_trade_reasons"][0]["reason"] == "stale_data"
    assert report["cycles_completed"] == 3
    assert report["operating_hierarchy"] == ["trend", "kalshi_event"]
    assert json.loads(report_path.read_text(encoding="utf-8"))["run_mode"] == "replay_paper"
    assert transparency_pack["commercial_wedge"] == "paper_trading_plus_alerts_plus_verification"
    assert transparency_pack["sleeves"][0]["sleeve_name"] == "trend"
    assert json.loads(transparency_path.read_text(encoding="utf-8"))["operator_disclosures"]["human_oversight_required"] is True

    report_path.unlink(missing_ok=True)
    transparency_path.unlink(missing_ok=True)


def test_kalshi_connectivity_check_writes_truthful_report(monkeypatch) -> None:
    report_path = Path("data") / f"test_kalshi_connectivity_{uuid4().hex}.json"
    monkeypatch.setenv("KALSHI_CONNECTIVITY_REPORT_PATH", str(report_path))
    settings = Settings()

    report = _run_kalshi_connectivity_check(settings)

    written = json.loads(report_path.read_text(encoding="utf-8"))
    assert written["kalshi_api_url"] == settings.kalshi_api_url
    assert written["status"] in {"live_paper", "data_unavailable"}
    assert "auth_configured" in written
    assert written == report

    report_path.unlink(missing_ok=True)


def test_sync_observability_uses_kalshi_evaluations_for_expected_edge_without_trades(monkeypatch) -> None:
    db_path = Path("data") / f"test_kalshi_expected_edge_{uuid4().hex}.db"
    monkeypatch.setenv("DB_PATH", str(db_path))
    settings = Settings()
    settings.ensure_directories()
    db = Database(settings.db_path)
    sleeve_repo = SleeveRepository(db)
    kalshi_repo = KalshiRepository(db)
    kalshi_repo.persist_evaluation(
        StrategyEvaluation(
            strategy=StrategyName.MONOTONICITY,
            ticker="KXTEST-YES",
            generated=False,
            reason="insufficient_signal_time_depth",
            edge_before_fees=0.001,
            edge_after_fees=0.015,
            fee_estimate=0.0,
            quantity=0,
            metadata={},
            ts=datetime(2026, 3, 28, 20, 0, tzinfo=timezone.utc),
        )
    )
    kalshi_repo.persist_evaluation(
        StrategyEvaluation(
            strategy=StrategyName.PARTITION,
            ticker="KXTEST-PARTITION",
            generated=False,
            reason="edge_below_threshold",
            edge_before_fees=-0.002,
            edge_after_fees=-0.005,
            fee_estimate=0.0,
            quantity=0,
            metadata={},
            ts=datetime(2026, 3, 28, 20, 1, tzinfo=timezone.utc),
        )
    )

    _sync_observability(
        settings=settings,
        db=db,
        sleeve_repo=sleeve_repo,
        cycle_status={
            "trend": {"status": "paused", "run_mode": "data_unavailable", "opportunities_seen": 0, "signals_attempted": 0, "no_trade_reasons": {"stale_data": 1}},
            "kalshi_event": {"status": "running", "run_mode": "live_paper", "opportunities_seen": 2, "signals_attempted": 0, "no_trade_reasons": {"edge_below_threshold": 1, "insufficient_depth": 1}},
        },
    )

    statuses = {row["sleeve_name"]: row for row in sleeve_repo.load_runtime_statuses()}
    assert statuses["kalshi_event"]["avg_expected_edge_decimal"] == "0.005"
    assert statuses["kalshi_event"]["opportunities_seen"] == 2


def test_kalshi_evaluation_summary_exposes_best_missed_candidates() -> None:
    summary = _summarize_kalshi_evaluations(
        [
            StrategyEvaluation(
                strategy=StrategyName.MONOTONICITY,
                ticker="KX-A",
                generated=False,
                reason="insufficient_signal_time_depth",
                edge_before_fees=0.01,
                edge_after_fees=0.02,
                fee_estimate=0.0,
                quantity=0,
                metadata={},
                ts=datetime(2026, 3, 28, 20, 0, tzinfo=timezone.utc),
            ),
            StrategyEvaluation(
                strategy=StrategyName.PARTITION,
                ticker="KX-B",
                generated=False,
                reason="edge_below_threshold",
                edge_before_fees=0.03,
                edge_after_fees=0.015,
                fee_estimate=0.0,
                quantity=0,
                metadata={},
                ts=datetime(2026, 3, 28, 20, 1, tzinfo=timezone.utc),
            ),
            StrategyEvaluation(
                strategy=StrategyName.CROSS_MARKET,
                ticker="KX-C",
                generated=True,
                reason="signal_generated",
                edge_before_fees=0.05,
                edge_after_fees=0.04,
                fee_estimate=0.0,
                quantity=1,
                metadata={},
                ts=datetime(2026, 3, 28, 20, 2, tzinfo=timezone.utc),
            ),
        ]
    )

    assert summary["avg_expected_edge"] == "0.025"
    assert summary["median_expected_edge"] == "0.02"
    assert summary["best_edge_seen"] == "0.04"
    assert [item["ticker"] for item in summary["top_candidates_nearest_to_trade"]] == ["KX-A", "KX-B"]


def test_multi_cycle_research_summary_rolls_up_rejections_and_near_trades(monkeypatch) -> None:
    db_path = Path("data") / f"test_multi_cycle_summary_{uuid4().hex}.db"
    monkeypatch.setenv("DB_PATH", str(db_path))
    settings = Settings()
    settings.ensure_directories()
    db = Database(settings.db_path)
    repo = SleeveRepository(db)
    kalshi_repo = KalshiRepository(db)
    repo.upsert_runtime_status(
        sleeve_name="kalshi_event",
        maturity="active_core",
        run_mode="live_paper",
        status="running",
        enabled=True,
        paper_capable=True,
        live_capable=False,
        capital_weight=Decimal("0.25"),
        capital_assigned=Decimal("25000"),
        opportunities_seen=3,
        trades_attempted=0,
        trades_filled=0,
        open_positions=0,
        realized_pnl=Decimal("0"),
        unrealized_pnl=Decimal("0"),
        fees=None,
        avg_slippage_bps=None,
        fill_rate=None,
        trade_count=0,
        error_count=0,
        avg_expected_edge=Decimal("-0.10"),
        avg_realized_edge=None,
        expected_realized_gap=None,
        no_trade_reasons={"edge_below_threshold": 2, "insufficient_depth": 1},
        details={"edge_summary": {"best_edge_seen": "0.03"}, "top_candidates_nearest_to_trade": [{"ticker": "KX-1", "strategy": "monotonicity", "reason": "edge_below_threshold", "edge_after_fees": "0.03", "quantity": 10}]},
    )
    repo.append_metrics_snapshot(
        sleeve_name="kalshi_event",
        run_mode="live_paper",
        status="running",
        capital_assigned=Decimal("25000"),
        opportunities_seen=3,
        trades_attempted=0,
        trades_filled=0,
        open_positions=0,
        realized_pnl=Decimal("0"),
        unrealized_pnl=Decimal("0"),
        fees=None,
        avg_slippage_bps=None,
        fill_rate=None,
        trade_count=0,
        error_count=0,
        avg_expected_edge=Decimal("-0.10"),
        avg_realized_edge=None,
        expected_realized_gap=None,
        no_trade_reasons={"edge_below_threshold": 2, "insufficient_depth": 1},
        details={"edge_summary": {"best_edge_seen": "0.03"}, "top_candidates_nearest_to_trade": [{"ticker": "KX-1", "strategy": "monotonicity", "reason": "edge_below_threshold", "edge_after_fees": "0.03", "quantity": 10}]},
    )
    repo.append_metrics_snapshot(
        sleeve_name="kalshi_event",
        run_mode="live_paper",
        status="running",
        capital_assigned=Decimal("25000"),
        opportunities_seen=5,
        trades_attempted=0,
        trades_filled=0,
        open_positions=0,
        realized_pnl=Decimal("0"),
        unrealized_pnl=Decimal("0"),
        fees=None,
        avg_slippage_bps=None,
        fill_rate=None,
        trade_count=0,
        error_count=0,
        avg_expected_edge=Decimal("-0.05"),
        avg_realized_edge=None,
        expected_realized_gap=None,
        no_trade_reasons={"edge_below_threshold": 1, "invalid_ladder_structure": 1},
        details={"edge_summary": {"best_edge_seen": "0.08"}, "top_candidates_nearest_to_trade": [{"ticker": "KX-1", "strategy": "monotonicity", "reason": "edge_below_threshold", "edge_after_fees": "0.08", "quantity": 12}, {"ticker": "KX-2", "strategy": "partition", "reason": "invalid_ladder_structure", "edge_after_fees": "0.01", "quantity": 0}]},
    )
    kalshi_repo.persist_evaluation(
        StrategyEvaluation(
            strategy=StrategyName.MONOTONICITY,
            ticker="KXTEST-1",
            generated=False,
            reason="edge_below_threshold",
            edge_before_fees=0.01,
            edge_after_fees=0.03,
            fee_estimate=0.0,
            quantity=10,
            metadata={},
            ts=datetime.now(timezone.utc),
        )
    )
    kalshi_repo.persist_evaluation(
        StrategyEvaluation(
            strategy=StrategyName.PARTITION,
            ticker="KXTEST-2",
            generated=False,
            reason="invalid_ladder_structure",
            edge_before_fees=0.0,
            edge_after_fees=0.01,
            fee_estimate=0.0,
            quantity=0,
            metadata={},
            ts=datetime.now(timezone.utc),
        )
    )
    kalshi_repo.persist_evaluation(
        StrategyEvaluation(
            strategy=StrategyName.MONOTONICITY,
            ticker="KX3MTBILL-26MAR31-T4.00",
            generated=False,
            reason="insufficient_size_multiple",
            edge_before_fees=0.2,
            edge_after_fees=0.1665,
            fee_estimate=0.03,
            quantity=10000,
            metadata={
                "lower_yes_ask_size": 87,
                "higher_yes_bid_size": 104,
                "average_signal_time_depth": 95.5,
                "min_size_multiple": 2.0,
            },
            ts=datetime.now(timezone.utc),
        )
    )

    summary = _build_multi_cycle_research_summary(sleeve_repo=repo, cycles_completed=2)
    kalshi_summary = summary["sleeves"][0]
    assert kalshi_summary["avg_opportunities_seen"] == "4"
    assert kalshi_summary["avg_expected_edge_over_cycles"] == "-0.075"
    assert kalshi_summary["best_edge_seen_over_cycles"] == "0.08"
    assert kalshi_summary["dominant_rejection_reason"]["reason"] == "edge_below_threshold"
    assert kalshi_summary["rejection_reason_counts"]["edge_below_threshold"] == 3
    assert kalshi_summary["family_rollup"][0]["family"] == "KXTEST"
    assert kalshi_summary["family_rollup"][0]["dominant_reason"]["reason"] == "edge_below_threshold"
    assert kalshi_summary["bill_focus"]["focus_family"] == "KX3MTBILL"
    assert kalshi_summary["bill_focus"]["top_candidate"]["ticker"] == "KX3MTBILL-26MAR31-T4.00"
    assert kalshi_summary["bill_focus"]["interpretation"] == "likely_sizing_limited"
    assert kalshi_summary["top_repeated_near_trades"][0]["ticker"] == "KX-1"
    assert kalshi_summary["top_repeated_near_trades"][0]["appearances"] == 2
    assert kalshi_summary["avg_realized_edge_over_cycles"] is None
    assert kalshi_summary["avg_expected_realized_gap_over_cycles"] is None


def test_trend_focus_summary_distinguishes_no_rebalance_from_risk_gate() -> None:
    metrics = [
        {
            "capital_assigned_decimal": "75000",
            "no_trade_reasons": "{}",
            "details": json.dumps({"signals_generated": 2, "targets_generated": 2, "cash": "25000", "gross_exposure": "50000", "drawdown": "0.01", "position_count": 2}),
        },
        {
            "capital_assigned_decimal": "75000",
            "no_trade_reasons": json.dumps({"no_rebalance_needed": 1}),
            "details": json.dumps({"signals_generated": 0, "targets_generated": 2, "cash": "26000", "gross_exposure": "49000", "drawdown": "0.02", "position_count": 2}),
        },
        {
            "capital_assigned_decimal": "75000",
            "no_trade_reasons": json.dumps({"no_rebalance_needed": 1}),
            "details": json.dumps({"signals_generated": 0, "targets_generated": 2, "cash": "25500", "gross_exposure": "49500", "drawdown": "0.03", "position_count": 2}),
        },
    ]

    summary = _build_trend_focus_summary(metrics)
    assert summary is not None
    assert summary["cycles_with_signals"] == 1
    assert summary["cycles_without_signals"] == 2
    assert summary["signal_cycle_rate"] == "0.3333333333333333333333333333"
    assert summary["target_cycle_rate"] == "1"
    assert summary["avg_targets_generated"] == "2"
    assert summary["avg_signals_generated"] == "0.6666666666666666666666666667"
    assert summary["dominant_no_trade_reason"]["reason"] == "no_rebalance_needed"
    assert summary["avg_position_count"] == "2"
    assert summary["avg_drawdown"] == "0.02"
    assert summary["interpretation"] == "already_positioned_not_retriggering"


def test_trend_focus_summary_flags_underdeployed_static_profile() -> None:
    metrics = [
        {
            "capital_assigned_decimal": "75000",
            "no_trade_reasons": "{}",
            "details": json.dumps({"signals_generated": 0, "targets_generated": 2, "cash": "60000", "gross_exposure": "15000", "drawdown": "0.00", "position_count": 1}),
        },
        {
            "capital_assigned_decimal": "75000",
            "no_trade_reasons": "{}",
            "details": json.dumps({"signals_generated": 0, "targets_generated": 2, "cash": "61000", "gross_exposure": "14000", "drawdown": "0.00", "position_count": 1}),
        },
    ]

    summary = _build_trend_focus_summary(metrics)
    assert summary is not None
    assert summary["cycles_with_signals"] == 0
    assert summary["target_cycle_rate"] == "1"
    assert summary["avg_gross_exposure_ratio"] == "0.1933333333333333333333333334"
    assert summary["interpretation"] == "underdeployed_without_rebalance_triggers"


def _trend_metric_row(
    *,
    run_mode: str = "replay_paper",
    no_trade_reasons: dict[str, int] | None = None,
    target_weights: dict[str, str] | None = None,
    current_weights: dict[str, str] | None = None,
    top_target_symbol: str | None = None,
    latest_bar_dates: dict[str, str] | None = None,
    latest_closes: dict[str, str] | None = None,
    portfolio_distance_l1: str = "0.05",
    portfolio_distance_l2: str = "0.03",
    unconstrained_ratio: str = "0.05",
    threshold_ratio: str = "0",
    constraint_ratio: str = "0",
    near_target: bool = False,
    near_miss_rebalance: bool = False,
    avg_signal_distance_from_trigger: str | None = None,
    cross_sectional_dispersion: str | None = None,
    avg_realized_volatility_proxy: str | None = None,
    realized_pnl: str = "0",
    unrealized_pnl: str = "0",
) -> dict[str, object]:
    details = {
        "signals_generated": 0,
        "targets_generated": len(target_weights or {}),
        "cash": "25000",
        "gross_exposure": "50000",
        "drawdown": "0",
        "position_count": len(current_weights or {}),
        "trend_cycle_diagnostics": {
            "target_symbols": sorted((target_weights or {}).keys()),
            "target_weights": target_weights or {},
            "current_weights": current_weights or {},
            "top_target_symbol": top_target_symbol,
            "latest_closes": latest_closes or {},
            "latest_bar_dates": latest_bar_dates or {},
            "portfolio_distance_to_target_l1": portfolio_distance_l1,
            "portfolio_distance_to_target_l2": portfolio_distance_l2,
            "unconstrained_rebalance_notional_ratio": unconstrained_ratio,
            "threshold_suppressed_notional_ratio": threshold_ratio,
            "constraint_suppressed_notional_ratio": constraint_ratio,
            "near_target": near_target,
            "near_miss_rebalance": near_miss_rebalance,
            "avg_signal_distance_from_trigger": avg_signal_distance_from_trigger,
            "cross_sectional_dispersion": cross_sectional_dispersion,
            "avg_realized_volatility_proxy": avg_realized_volatility_proxy,
        },
    }
    return {
        "run_mode": run_mode,
        "capital_assigned_decimal": "75000",
        "realized_pnl_decimal": realized_pnl,
        "unrealized_pnl_decimal": unrealized_pnl,
        "no_trade_reasons": json.dumps(no_trade_reasons or {}),
        "details": json.dumps(details),
    }


def test_trend_sparsity_summary_classifies_already_at_target() -> None:
    metrics = [
        _trend_metric_row(
            target_weights={"QQQ": "0.35", "SPY": "0.35"},
            current_weights={"QQQ": "0.34", "SPY": "0.33"},
            top_target_symbol="QQQ",
            latest_bar_dates={"QQQ": "2026-03-28", "SPY": "2026-03-28"},
            portfolio_distance_l1="0.03",
            portfolio_distance_l2="0.02",
            unconstrained_ratio="0.03",
            near_target=True,
            cross_sectional_dispersion="0.02",
            avg_realized_volatility_proxy="0.12",
        ),
        _trend_metric_row(
            target_weights={"QQQ": "0.35", "SPY": "0.35"},
            current_weights={"QQQ": "0.35", "SPY": "0.34"},
            top_target_symbol="QQQ",
            latest_bar_dates={"QQQ": "2026-03-29", "SPY": "2026-03-29"},
            portfolio_distance_l1="0.02",
            portfolio_distance_l2="0.015",
            unconstrained_ratio="0.02",
            near_target=True,
            cross_sectional_dispersion="0.02",
            avg_realized_volatility_proxy="0.12",
        ),
    ]

    summary = _build_trend_sparsity_summary(metrics)
    assert summary is not None
    assert summary["avg_target_count"] == "2"
    assert summary["interpretation"] == "already_at_target"
    assert summary["interpretation_reason"] == "portfolio_distance_to_target_stays_low_and_required_turnover_is_small"


def test_trend_sparsity_summary_classifies_threshold_too_wide() -> None:
    metrics = [
        _trend_metric_row(
            target_weights={"QQQ": "0.32", "SPY": "0.28"},
            current_weights={"QQQ": "0.27", "SPY": "0.23"},
            top_target_symbol="QQQ",
            latest_bar_dates={"QQQ": "2026-03-28", "SPY": "2026-03-28"},
            portfolio_distance_l1="0.10",
            portfolio_distance_l2="0.07",
            unconstrained_ratio="0.10",
            threshold_ratio="0.05",
            near_miss_rebalance=True,
            avg_signal_distance_from_trigger="0.01",
            cross_sectional_dispersion="0.03",
            avg_realized_volatility_proxy="0.15",
        ),
        _trend_metric_row(
            target_weights={"QQQ": "0.34", "SPY": "0.26"},
            current_weights={"QQQ": "0.29", "SPY": "0.21"},
            top_target_symbol="QQQ",
            latest_bar_dates={"QQQ": "2026-03-29", "SPY": "2026-03-29"},
            portfolio_distance_l1="0.10",
            portfolio_distance_l2="0.07",
            unconstrained_ratio="0.10",
            threshold_ratio="0.05",
            near_miss_rebalance=True,
            avg_signal_distance_from_trigger="0.01",
            cross_sectional_dispersion="0.03",
            avg_realized_volatility_proxy="0.15",
        ),
        _trend_metric_row(
            target_weights={"QQQ": "0.31", "SPY": "0.29"},
            current_weights={"QQQ": "0.26", "SPY": "0.24"},
            top_target_symbol="QQQ",
            latest_bar_dates={"QQQ": "2026-03-30", "SPY": "2026-03-30"},
            portfolio_distance_l1="0.10",
            portfolio_distance_l2="0.07",
            unconstrained_ratio="0.10",
            threshold_ratio="0.05",
            near_miss_rebalance=True,
            avg_signal_distance_from_trigger="0.01",
            cross_sectional_dispersion="0.03",
            avg_realized_volatility_proxy="0.15",
        ),
    ]

    summary = _build_trend_sparsity_summary(metrics)
    assert summary is not None
    assert summary["cycles_with_threshold_suppression"] == 3
    assert summary["interpretation"] == "threshold_too_wide"


def test_trend_sparsity_summary_classifies_universe_too_stable() -> None:
    metrics = [
        _trend_metric_row(
            run_mode="live_paper",
            target_weights={"QQQ": "0.34", "SPY": "0.33", "TLT": "0.10"},
            current_weights={"QQQ": "0.20", "SPY": "0.20"},
            top_target_symbol="QQQ",
            latest_bar_dates={"QQQ": "2026-03-28", "SPY": "2026-03-28", "TLT": "2026-03-28"},
            portfolio_distance_l1="0.20",
            portfolio_distance_l2="0.12",
            unconstrained_ratio="0.20",
            cross_sectional_dispersion="0.05",
            avg_realized_volatility_proxy="0.14",
        ),
        _trend_metric_row(
            run_mode="live_paper",
            target_weights={"QQQ": "0.341", "SPY": "0.329", "TLT": "0.10"},
            current_weights={"QQQ": "0.21", "SPY": "0.20"},
            top_target_symbol="QQQ",
            latest_bar_dates={"QQQ": "2026-03-29", "SPY": "2026-03-29", "TLT": "2026-03-29"},
            portfolio_distance_l1="0.19",
            portfolio_distance_l2="0.11",
            unconstrained_ratio="0.19",
            cross_sectional_dispersion="0.05",
            avg_realized_volatility_proxy="0.14",
        ),
        _trend_metric_row(
            run_mode="live_paper",
            target_weights={"QQQ": "0.34", "SPY": "0.33", "TLT": "0.10"},
            current_weights={"QQQ": "0.22", "SPY": "0.20"},
            top_target_symbol="QQQ",
            latest_bar_dates={"QQQ": "2026-03-30", "SPY": "2026-03-30", "TLT": "2026-03-30"},
            portfolio_distance_l1="0.18",
            portfolio_distance_l2="0.11",
            unconstrained_ratio="0.18",
            cross_sectional_dispersion="0.05",
            avg_realized_volatility_proxy="0.14",
        ),
    ]

    summary = _build_trend_sparsity_summary(metrics)
    assert summary is not None
    assert summary["avg_target_name_turnover_rate"] == "0"
    assert summary["stable_target_core_rate"] == "1"
    assert summary["interpretation"] == "universe_too_stable"
    assert summary["material_input_advancement_rate"] == "1"


def test_trend_sparsity_summary_falls_back_to_insufficient_data() -> None:
    summary = _build_trend_sparsity_summary(
        [
            {
                "run_mode": "replay_paper",
                "capital_assigned_decimal": "75000",
                "no_trade_reasons": "{}",
                "details": json.dumps({"signals_generated": 0}),
            }
        ]
    )
    assert summary is not None
    assert summary["interpretation"] == "insufficient_data"


def test_trend_sparsity_summary_reports_no_material_input_advancement_for_static_replay() -> None:
    metrics = [
        _trend_metric_row(
            target_weights={"QQQ": "0.35", "SPY": "0.35", "TLT": "0"},
            current_weights={"QQQ": "0.348", "SPY": "0.346"},
            top_target_symbol="SPY",
            latest_bar_dates={"QQQ": "2026-03-28", "SPY": "2026-03-28", "TLT": "2026-03-28"},
            portfolio_distance_l1="0.006",
            unconstrained_ratio="0.006",
            threshold_ratio="0.006",
            near_target=True,
            cross_sectional_dispersion="0.11",
            avg_realized_volatility_proxy="0.01",
        ),
        _trend_metric_row(
            target_weights={"QQQ": "0.35", "SPY": "0.35", "TLT": "0"},
            current_weights={"QQQ": "0.348", "SPY": "0.346"},
            top_target_symbol="SPY",
            latest_bar_dates={"QQQ": "2026-03-28", "SPY": "2026-03-28", "TLT": "2026-03-28"},
            portfolio_distance_l1="0.006",
            unconstrained_ratio="0.006",
            threshold_ratio="0.006",
            near_target=True,
            cross_sectional_dispersion="0.11",
            avg_realized_volatility_proxy="0.01",
        ),
    ]
    summary = _build_trend_sparsity_summary(metrics)
    assert summary is not None
    assert summary["cycles_with_material_input_advancement"] == 0
    assert summary["material_input_advancement_rate"] == "0"
    assert summary["interpretation"] == "bootstrap_too_smooth"


def test_trend_replay_diversity_report_detects_when_advancing_inputs_increase_drift() -> None:
    report = _build_trend_replay_diversity_report(
        scenarios=[
            {
                "scenario_name": "cached_bootstrap_static",
                "description": "static",
                "trend_bootstrap_variant": "smooth",
                "trend_replay_advance_mode": "static",
                "report": {
                    "sleeves": [
                        {
                            "sleeve_name": "trend",
                            "cycles_observed": 5,
                            "trend_focus": {"signal_cycle_rate": "0.0", "avg_targets_generated": "3", "avg_signals_generated": "0"},
                            "trend_sparsity": {
                                "avg_target_weight_change_l1": "0.00",
                                "avg_target_name_turnover_rate": "0",
                                "material_input_advancement_rate": "0",
                                "cycles_with_material_input_advancement": 0,
                                "cycles_with_identical_latest_bar_dates": 4,
                                "interpretation": "bootstrap_too_smooth",
                                "interpretation_reason": "replay_cycles_have_unchanged_bar_dates_and_near_zero_target_drift",
                            },
                            "trend_construction": {
                                "avg_positive_momentum_count": "2",
                                "avg_capped_target_count": "2",
                                "avg_scaling_factor": "1",
                                "avg_top_weight_share": "0.35",
                                "avg_effective_symbol_count": "2",
                                "cycles_with_max_weight_binding": 5,
                                "stable_positive_core_rate": "1",
                                "interpretation": "narrow_positive_universe_with_weight_saturation",
                                "interpretation_reason": "same_small_positive_symbol_set_repeatedly_hits_max_weight",
                            },
                        }
                    ]
                },
            },
            {
                "scenario_name": "cached_bootstrap_shifted",
                "description": "shifted",
                "trend_bootstrap_variant": "diverse",
                "trend_replay_advance_mode": "shift_window",
                "report": {
                    "sleeves": [
                        {
                            "sleeve_name": "trend",
                            "cycles_observed": 5,
                            "trend_focus": {"signal_cycle_rate": "0.4", "avg_targets_generated": "3", "avg_signals_generated": "0.8"},
                            "trend_sparsity": {
                                "avg_target_weight_change_l1": "0.12",
                                "avg_target_name_turnover_rate": "0.25",
                                "material_input_advancement_rate": "1",
                                "cycles_with_material_input_advancement": 4,
                                "cycles_with_identical_latest_bar_dates": 0,
                                "interpretation": "mixed_sparse",
                                "interpretation_reason": "multiple_sparse_forces_present_without_single_dominant_driver",
                            },
                            "trend_construction": {
                                "avg_positive_momentum_count": "3",
                                "avg_capped_target_count": "1",
                                "avg_scaling_factor": "0.8",
                                "avg_top_weight_share": "0.34",
                                "avg_effective_symbol_count": "2.7",
                                "cycles_with_max_weight_binding": 1,
                                "stable_positive_core_rate": "0.5",
                                "interpretation": "mixed_construction",
                                "interpretation_reason": "construction_constraints_are_present_but_not_singular",
                            },
                        }
                    ]
                },
            },
        ]
    )
    assert report["interpretation"] == "replay_diversity_unlocks_trend_drift"


def test_trend_replay_diversity_report_detects_saturated_construction_after_input_advancement() -> None:
    report = _build_trend_replay_diversity_report(
        scenarios=[
            {
                "scenario_name": "cached_bootstrap_static",
                "description": "static",
                "trend_bootstrap_variant": "smooth",
                "trend_replay_advance_mode": "static",
                "report": {
                    "sleeves": [
                        {
                            "sleeve_name": "trend",
                            "cycles_observed": 5,
                            "trend_focus": {"signal_cycle_rate": "0.2", "avg_targets_generated": "3", "avg_signals_generated": "0.4"},
                            "trend_sparsity": {
                                "avg_target_weight_change_l1": "0.00",
                                "avg_target_name_turnover_rate": "0",
                                "material_input_advancement_rate": "0",
                                "cycles_with_material_input_advancement": 0,
                                "cycles_with_identical_latest_bar_dates": 4,
                                "interpretation": "bootstrap_too_smooth",
                                "interpretation_reason": "replay_cycles_have_unchanged_bar_dates_and_near_zero_target_drift",
                            },
                            "trend_construction": {
                                "avg_positive_momentum_count": "2",
                                "avg_capped_target_count": "2",
                                "avg_scaling_factor": "1",
                                "avg_top_weight_share": "0.35",
                                "avg_effective_symbol_count": "2",
                                "cycles_with_max_weight_binding": 5,
                                "stable_positive_core_rate": "1",
                                "interpretation": "narrow_positive_universe_with_weight_saturation",
                                "interpretation_reason": "same_small_positive_symbol_set_repeatedly_hits_max_weight",
                            },
                        }
                    ]
                },
            },
            {
                "scenario_name": "cached_bootstrap_shifted",
                "description": "shifted",
                "trend_bootstrap_variant": "diverse",
                "trend_replay_advance_mode": "shift_window",
                "report": {
                    "sleeves": [
                        {
                            "sleeve_name": "trend",
                            "cycles_observed": 5,
                            "trend_focus": {"signal_cycle_rate": "0.2", "avg_targets_generated": "3", "avg_signals_generated": "0.4"},
                            "trend_sparsity": {
                                "avg_target_weight_change_l1": "0.00",
                                "avg_target_name_turnover_rate": "0",
                                "material_input_advancement_rate": "1",
                                "cycles_with_material_input_advancement": 4,
                                "cycles_with_identical_latest_bar_dates": 0,
                                "interpretation": "already_at_target",
                                "interpretation_reason": "portfolio_distance_to_target_stays_low_and_required_turnover_is_small",
                            },
                            "trend_construction": {
                                "avg_positive_momentum_count": "2",
                                "avg_capped_target_count": "2",
                                "avg_scaling_factor": "1",
                                "avg_top_weight_share": "0.35",
                                "avg_effective_symbol_count": "2",
                                "cycles_with_max_weight_binding": 5,
                                "stable_positive_core_rate": "1",
                                "interpretation": "narrow_positive_universe_with_weight_saturation",
                                "interpretation_reason": "same_small_positive_symbol_set_repeatedly_hits_max_weight",
                            },
                        }
                    ]
                },
            },
        ]
    )
    assert report["interpretation"] == "inputs_advance_but_construction_remains_saturated"


def test_trend_threshold_sensitivity_report_detects_material_benchmark_improvement() -> None:
    report = _build_trend_threshold_sensitivity_report(
        scenarios=[
            {
                "scenario_name": "threshold_0_10",
                "description": "base",
                "trend_min_rebalance_fraction": Decimal("0.10"),
                "report": {
                    "sleeves": [
                        {
                            "sleeve_name": "trend",
                            "trend_focus": {"signal_cycle_rate": "0.2"},
                            "trend_sparsity": {"avg_target_weight_change_l1": "0.008", "avg_threshold_suppressed_notional_ratio": "0.008", "near_target_rate": "1"},
                            "trend_validation": {"trade_cycle_rate": "1", "interpretation": "expressive_but_low_drift"},
                            "trend_benchmark": {
                                "interpretation": "lagging_simple_spy_qqq_in_sample",
                                "strategy_cumulative_return": "0",
                                "spy_qqq_equal_weight": {"strategy_excess_return": "-0.010", "interval_outperformance_rate": "0"},
                                "trend_universe_equal_weight": {"strategy_excess_return": "-0.009"},
                            },
                        }
                    ]
                },
            },
            {
                "scenario_name": "threshold_0_05",
                "description": "better",
                "trend_min_rebalance_fraction": Decimal("0.05"),
                "report": {
                    "sleeves": [
                        {
                            "sleeve_name": "trend",
                            "trend_focus": {"signal_cycle_rate": "0.4"},
                            "trend_sparsity": {"avg_target_weight_change_l1": "0.020", "avg_threshold_suppressed_notional_ratio": "0.003", "near_target_rate": "0.4"},
                            "trend_validation": {"trade_cycle_rate": "1", "interpretation": "responsive_allocator_with_light_rebalance"},
                            "trend_benchmark": {
                                "interpretation": "lagging_simple_spy_qqq_in_sample",
                                "strategy_cumulative_return": "0.002",
                                "spy_qqq_equal_weight": {"strategy_excess_return": "-0.006", "interval_outperformance_rate": "0.5"},
                                "trend_universe_equal_weight": {"strategy_excess_return": "-0.004"},
                            },
                        }
                    ]
                },
            },
        ]
    )
    assert report["interpretation"] == "lower_rebalance_threshold_improves_benchmark_posture"


def test_trend_threshold_sensitivity_report_flags_no_material_benchmark_improvement() -> None:
    report = _build_trend_threshold_sensitivity_report(
        scenarios=[
            {
                "scenario_name": "threshold_0_10",
                "description": "base",
                "trend_min_rebalance_fraction": Decimal("0.10"),
                "report": {
                    "sleeves": [
                        {
                            "sleeve_name": "trend",
                            "trend_focus": {"signal_cycle_rate": "0.2"},
                            "trend_sparsity": {"avg_target_weight_change_l1": "0.008", "avg_threshold_suppressed_notional_ratio": "0.008", "near_target_rate": "1"},
                            "trend_validation": {"trade_cycle_rate": "1", "interpretation": "expressive_but_low_drift"},
                            "trend_benchmark": {
                                "interpretation": "lagging_simple_spy_qqq_in_sample",
                                "strategy_cumulative_return": "0",
                                "spy_qqq_equal_weight": {"strategy_excess_return": "-0.010", "interval_outperformance_rate": "0"},
                                "trend_universe_equal_weight": {"strategy_excess_return": "-0.009"},
                            },
                        }
                    ]
                },
            },
            {
                "scenario_name": "threshold_0_05",
                "description": "not better",
                "trend_min_rebalance_fraction": Decimal("0.05"),
                "report": {
                    "sleeves": [
                        {
                            "sleeve_name": "trend",
                            "trend_focus": {"signal_cycle_rate": "0.4"},
                            "trend_sparsity": {"avg_target_weight_change_l1": "0.020", "avg_threshold_suppressed_notional_ratio": "0.003", "near_target_rate": "0.4"},
                            "trend_validation": {"trade_cycle_rate": "1", "interpretation": "responsive_allocator_with_light_rebalance"},
                            "trend_benchmark": {
                                "interpretation": "lagging_simple_spy_qqq_in_sample",
                                "strategy_cumulative_return": "0.001",
                                "spy_qqq_equal_weight": {"strategy_excess_return": "-0.009", "interval_outperformance_rate": "0.25"},
                                "trend_universe_equal_weight": {"strategy_excess_return": "-0.008"},
                            },
                        }
                    ]
                },
            },
        ]
    )
    assert report["interpretation"] == "threshold_changes_do_not_materially_improve_benchmark_posture"


def test_trend_validation_summary_flags_expressive_but_low_drift() -> None:
    metrics = [
        _trend_metric_row(
            target_weights={"DBC": "0.07", "EEM": "0.08", "EFA": "0.08", "GLD": "0.14", "HYG": "0.10", "IWM": "0.09", "LQD": "0.13", "QQQ": "0.02", "SPY": "0.12", "TIP": "0.16"},
            current_weights={"DBC": "0.069", "EEM": "0.081", "EFA": "0.081", "GLD": "0.138", "HYG": "0.102", "IWM": "0.091", "LQD": "0.128", "QQQ": "0.018", "SPY": "0.120", "TIP": "0.159"},
            top_target_symbol="TIP",
            latest_bar_dates={"DBC": "2026-03-30", "EEM": "2026-03-30", "EFA": "2026-03-30", "GLD": "2026-03-30", "HYG": "2026-03-30", "IWM": "2026-03-30", "LQD": "2026-03-30", "QQQ": "2026-03-30", "SPY": "2026-03-30", "TIP": "2026-03-30"},
            portfolio_distance_l1="0.0098",
            unconstrained_ratio="0.0098",
            near_target=True,
        ),
        _trend_metric_row(
            target_weights={"DBC": "0.063", "EEM": "0.070", "EFA": "0.078", "GLD": "0.156", "HYG": "0.090", "IWM": "0.083", "LQD": "0.119", "QQQ": "0.094", "SPY": "0.110", "TIP": "0.138"},
            current_weights={"DBC": "0.063", "EEM": "0.070", "EFA": "0.078", "GLD": "0.156", "HYG": "0.090", "IWM": "0.081", "LQD": "0.118", "QQQ": "0.091", "SPY": "0.108", "TIP": "0.137"},
            top_target_symbol="GLD",
            latest_bar_dates={"DBC": "2026-03-31", "EEM": "2026-03-31", "EFA": "2026-03-31", "GLD": "2026-03-31", "HYG": "2026-03-31", "IWM": "2026-03-31", "LQD": "2026-03-31", "QQQ": "2026-03-31", "SPY": "2026-03-31", "TIP": "2026-03-31"},
            portfolio_distance_l1="0.0078",
            unconstrained_ratio="0.0078",
            near_target=True,
        ),
    ]
    for metric in metrics:
        details = json.loads(metric["details"])
        details["signals_generated"] = 1
        details["position_count"] = 10
        details["gross_exposure"] = "74000"
        details["cash"] = "1000"
        details["trend_cycle_diagnostics"]["construction"] = {
            "rows": [],
            "positive_momentum_symbols": ["DBC", "EEM", "EFA", "GLD", "HYG", "IWM", "LQD", "QQQ", "SPY", "TIP"],
            "positive_momentum_count": 10,
            "capped_target_symbols": [],
            "capped_target_count": 0,
            "gross_raw_target_weight": "1.0",
            "gross_scaled_target_weight": "1.0",
            "scaling_factor": "1",
            "top_weight_share": "0.155",
            "effective_symbol_count": "9.2",
        }
        metric["trade_count"] = 1
        metric["avg_realized_edge_decimal"] = "0.03"
        metric["expected_realized_gap_decimal"] = "0.002"
        metric["details"] = json.dumps(details)
    summary = _build_trend_validation_summary(metrics)
    assert summary is not None
    assert summary["interpretation"] == "expressive_but_low_drift"
    assert summary["avg_capped_target_count"] == "0"


def test_trend_validation_summary_flags_construction_still_saturated() -> None:
    metrics = [
        _trend_metric_row(
            target_weights={"QQQ": "0.20", "SPY": "0.20", "IWM": "0.20", "EFA": "0.20", "EEM": "0.20"},
            current_weights={"QQQ": "0.19", "SPY": "0.19", "IWM": "0.19", "EFA": "0.19", "EEM": "0.19"},
            top_target_symbol="QQQ",
            latest_bar_dates={"QQQ": "2026-03-31", "SPY": "2026-03-31", "IWM": "2026-03-31", "EFA": "2026-03-31", "EEM": "2026-03-31"},
            portfolio_distance_l1="0.03",
            unconstrained_ratio="0.03",
        ),
    ]
    details = json.loads(metrics[0]["details"])
    details["signals_generated"] = 1
    details["position_count"] = 5
    details["gross_exposure"] = "70000"
    details["cash"] = "5000"
    details["trend_cycle_diagnostics"]["construction"] = {
        "rows": [],
        "positive_momentum_symbols": ["QQQ", "SPY", "IWM", "EFA", "EEM"],
        "positive_momentum_count": 5,
        "capped_target_symbols": ["QQQ", "SPY", "IWM", "EFA", "EEM"],
        "capped_target_count": 5,
        "gross_raw_target_weight": "1.75",
        "gross_scaled_target_weight": "1.0",
        "scaling_factor": "0.57",
        "top_weight_share": "0.20",
        "effective_symbol_count": "5",
    }
    metrics[0]["trade_count"] = 1
    metrics[0]["details"] = json.dumps(details)
    summary = _build_trend_validation_summary(metrics)
    assert summary is not None
    assert summary["interpretation"] == "construction_still_saturated"


def test_trend_benchmark_summary_compares_against_passive_baselines() -> None:
    metrics = [
        _trend_metric_row(
            target_weights={"QQQ": "0.5", "SPY": "0.5"},
            current_weights={"QQQ": "0.5", "SPY": "0.5"},
            top_target_symbol="QQQ",
            latest_bar_dates={"QQQ": "2026-03-30", "SPY": "2026-03-30"},
            latest_closes={"QQQ": "100", "SPY": "100"},
            realized_pnl="0",
            unrealized_pnl="0",
        ),
        _trend_metric_row(
            target_weights={"QQQ": "0.55", "SPY": "0.45"},
            current_weights={"QQQ": "0.55", "SPY": "0.45"},
            top_target_symbol="QQQ",
            latest_bar_dates={"QQQ": "2026-03-31", "SPY": "2026-03-31"},
            latest_closes={"QQQ": "105", "SPY": "101"},
            realized_pnl="4500",
            unrealized_pnl="0",
        ),
    ]

    summary = _build_trend_benchmark_summary(metrics)

    assert summary is not None
    assert summary["intervals_compared"] == 1
    assert summary["interpretation"] == "outperforming_simple_spy_qqq_in_sample"
    assert summary["spy_qqq_equal_weight"]["cumulative_return"] == "0.030"
    assert summary["spy_qqq_equal_weight"]["strategy_excess_return"] == "0.030"
    assert summary["trend_universe_equal_weight"]["intervals_compared"] == 1


def test_multi_cycle_research_summary_adds_trend_benchmark_block(monkeypatch) -> None:
    db_path = Path("data") / f"test_trend_benchmark_multi_cycle_{uuid4().hex}.db"
    monkeypatch.setenv("DB_PATH", str(db_path))
    settings = Settings()
    settings.ensure_directories()
    db = Database(settings.db_path)
    repo = SleeveRepository(db)
    first = _trend_metric_row(
        target_weights={"QQQ": "0.5", "SPY": "0.5"},
        current_weights={"QQQ": "0.49", "SPY": "0.49"},
        top_target_symbol="QQQ",
        latest_bar_dates={"QQQ": "2026-03-30", "SPY": "2026-03-30"},
        latest_closes={"QQQ": "100", "SPY": "100"},
    )
    second = _trend_metric_row(
        target_weights={"QQQ": "0.55", "SPY": "0.45"},
        current_weights={"QQQ": "0.55", "SPY": "0.45"},
        top_target_symbol="QQQ",
        latest_bar_dates={"QQQ": "2026-03-31", "SPY": "2026-03-31"},
        latest_closes={"QQQ": "103", "SPY": "101"},
        realized_pnl="3750",
    )
    for details in [first, second]:
        repo.append_metrics_snapshot(
            sleeve_name="trend",
            run_mode="replay_paper",
            status="running",
            capital_assigned=Decimal("75000"),
            opportunities_seen=2,
            trades_attempted=1,
            trades_filled=1,
            open_positions=2,
            realized_pnl=Decimal(details["realized_pnl_decimal"]),
            unrealized_pnl=Decimal(details["unrealized_pnl_decimal"]),
            fees=None,
            avg_slippage_bps=None,
            fill_rate=Decimal("1"),
            trade_count=1,
            error_count=0,
            avg_expected_edge=Decimal("0.02"),
            avg_realized_edge=Decimal("0.018"),
            expected_realized_gap=Decimal("0.002"),
            no_trade_reasons={},
            details=json.loads(details["details"]),
        )
    repo.upsert_runtime_status(
        sleeve_name="trend",
        maturity="active_core",
        run_mode="replay_paper",
        status="running",
        enabled=True,
        paper_capable=True,
        live_capable=False,
        capital_weight=Decimal("0.75"),
        capital_assigned=Decimal("75000"),
        opportunities_seen=2,
        trades_attempted=1,
        trades_filled=1,
        open_positions=2,
        realized_pnl=Decimal("3750"),
        unrealized_pnl=Decimal("0"),
        fees=None,
        avg_slippage_bps=None,
        fill_rate=Decimal("1"),
        trade_count=1,
        error_count=0,
        avg_expected_edge=Decimal("0.02"),
        avg_realized_edge=Decimal("0.018"),
        expected_realized_gap=Decimal("0.002"),
        no_trade_reasons={},
        details=json.loads(second["details"]),
    )

    summary = _build_multi_cycle_research_summary(sleeve_repo=repo, cycles_completed=2)
    trend_summary = summary["sleeves"][0]

    assert trend_summary["trend_benchmark"]["interpretation"] == "outperforming_simple_spy_qqq_in_sample"
    assert trend_summary["trend_benchmark"]["spy_qqq_equal_weight"]["intervals_compared"] == 1


def test_trend_signal_summary_flags_broad_risk_on_low_separation() -> None:
    metrics = [_trend_metric_row(), _trend_metric_row()]
    row_template = [
        {"symbol": "SPY", "momentum_return": "0.041", "realized_volatility": "0.01"},
        {"symbol": "QQQ", "momentum_return": "0.038", "realized_volatility": "0.01"},
        {"symbol": "IWM", "momentum_return": "0.036", "realized_volatility": "0.01"},
        {"symbol": "EFA", "momentum_return": "0.034", "realized_volatility": "0.01"},
        {"symbol": "EEM", "momentum_return": "0.033", "realized_volatility": "0.01"},
        {"symbol": "TLT", "momentum_return": "-0.010", "realized_volatility": "0.01"},
    ]
    for metric in metrics:
        details = json.loads(metric["details"])
        details["trend_cycle_diagnostics"]["construction"] = {"rows": row_template}
        metric["details"] = json.dumps(details)
    summary = _build_trend_signal_summary(metrics)
    assert summary is not None
    assert summary["interpretation"] == "broad_risk_on_low_separation"


def test_trend_signal_summary_flags_meaningful_rank_dispersion() -> None:
    metrics = [_trend_metric_row(), _trend_metric_row()]
    rows_one = [
        {"symbol": "DBC", "momentum_return": "0.16", "realized_volatility": "0.02"},
        {"symbol": "GLD", "momentum_return": "0.08", "realized_volatility": "0.01"},
        {"symbol": "SPY", "momentum_return": "0.04", "realized_volatility": "0.01"},
        {"symbol": "QQQ", "momentum_return": "0.01", "realized_volatility": "0.02"},
        {"symbol": "TLT", "momentum_return": "-0.06", "realized_volatility": "0.01"},
    ]
    rows_two = [
        {"symbol": "DBC", "momentum_return": "0.15", "realized_volatility": "0.02"},
        {"symbol": "GLD", "momentum_return": "0.09", "realized_volatility": "0.01"},
        {"symbol": "SPY", "momentum_return": "0.03", "realized_volatility": "0.01"},
        {"symbol": "QQQ", "momentum_return": "0.00", "realized_volatility": "0.02"},
        {"symbol": "TLT", "momentum_return": "-0.05", "realized_volatility": "0.01"},
    ]
    for metric, rows in zip(metrics, [rows_one, rows_two]):
        details = json.loads(metric["details"])
        details["trend_cycle_diagnostics"]["construction"] = {"rows": rows}
        metric["details"] = json.dumps(details)
    summary = _build_trend_signal_summary(metrics)
    assert summary is not None
    assert summary["interpretation"] == "meaningful_rank_dispersion"


def test_multi_cycle_research_summary_preserves_trend_focus_and_adds_trend_sparsity(monkeypatch) -> None:
    db_path = Path("data") / f"test_trend_sparsity_multi_cycle_{uuid4().hex}.db"
    monkeypatch.setenv("DB_PATH", str(db_path))
    settings = Settings()
    settings.ensure_directories()
    db = Database(settings.db_path)
    repo = SleeveRepository(db)
    details_one = {
        "signals_generated": 1,
        "targets_generated": 2,
        "cash": "25000",
        "gross_exposure": "50000",
        "drawdown": "0",
        "position_count": 2,
        "trend_cycle_diagnostics": {
            "target_symbols": ["QQQ", "SPY"],
            "target_weights": {"QQQ": "0.35", "SPY": "0.35"},
            "current_weights": {"QQQ": "0.34", "SPY": "0.33"},
            "top_target_symbol": "QQQ",
            "latest_bar_dates": {"QQQ": "2026-03-28", "SPY": "2026-03-28"},
            "portfolio_distance_to_target_l1": "0.03",
            "portfolio_distance_to_target_l2": "0.02",
            "unconstrained_rebalance_notional_ratio": "0.03",
            "threshold_suppressed_notional_ratio": "0",
            "constraint_suppressed_notional_ratio": "0",
            "near_target": True,
            "near_miss_rebalance": False,
            "avg_signal_distance_from_trigger": None,
            "cross_sectional_dispersion": "0.02",
            "avg_realized_volatility_proxy": "0.12",
            "construction": {
                "rows": [],
                "positive_momentum_symbols": ["QQQ", "SPY"],
                "positive_momentum_count": 2,
                "capped_target_symbols": ["QQQ", "SPY"],
                "capped_target_count": 2,
                "gross_raw_target_weight": "0.70",
                "gross_scaled_target_weight": "0.70",
                "scaling_factor": "1",
                "top_weight_share": "0.35",
                "effective_symbol_count": "2",
            },
        },
    }
    details_two = {
        "signals_generated": 0,
        "targets_generated": 2,
        "cash": "26000",
        "gross_exposure": "49000",
        "drawdown": "0",
        "position_count": 2,
        "trend_cycle_diagnostics": {
            "target_symbols": ["QQQ", "SPY"],
            "target_weights": {"QQQ": "0.35", "SPY": "0.35"},
            "current_weights": {"QQQ": "0.35", "SPY": "0.34"},
            "top_target_symbol": "QQQ",
            "latest_bar_dates": {"QQQ": "2026-03-29", "SPY": "2026-03-29"},
            "portfolio_distance_to_target_l1": "0.02",
            "portfolio_distance_to_target_l2": "0.015",
            "unconstrained_rebalance_notional_ratio": "0.02",
            "threshold_suppressed_notional_ratio": "0",
            "constraint_suppressed_notional_ratio": "0",
            "near_target": True,
            "near_miss_rebalance": False,
            "avg_signal_distance_from_trigger": None,
            "cross_sectional_dispersion": "0.02",
            "avg_realized_volatility_proxy": "0.12",
            "construction": {
                "rows": [],
                "positive_momentum_symbols": ["QQQ", "SPY"],
                "positive_momentum_count": 2,
                "capped_target_symbols": ["QQQ", "SPY"],
                "capped_target_count": 2,
                "gross_raw_target_weight": "0.70",
                "gross_scaled_target_weight": "0.70",
                "scaling_factor": "1",
                "top_weight_share": "0.35",
                "effective_symbol_count": "2",
            },
        },
    }
    for no_trade_reasons, details in [({}, details_one), ({"no_rebalance_needed": 1}, details_two)]:
        repo.append_metrics_snapshot(
            sleeve_name="trend",
            run_mode="replay_paper",
            status="running",
            capital_assigned=Decimal("75000"),
            opportunities_seen=2,
            trades_attempted=0,
            trades_filled=0,
            open_positions=2,
            realized_pnl=Decimal("0"),
            unrealized_pnl=Decimal("0"),
            fees=None,
            avg_slippage_bps=None,
            fill_rate=None,
            trade_count=0,
            error_count=0,
            avg_expected_edge=Decimal("0.02"),
            avg_realized_edge=Decimal("0.019"),
            expected_realized_gap=Decimal("0.001"),
            no_trade_reasons=no_trade_reasons,
            details=details,
        )
    repo.upsert_runtime_status(
        sleeve_name="trend",
        maturity="active_core",
        run_mode="replay_paper",
        status="running",
        enabled=True,
        paper_capable=True,
        live_capable=False,
        capital_weight=Decimal("0.75"),
        capital_assigned=Decimal("75000"),
        opportunities_seen=2,
        trades_attempted=0,
        trades_filled=0,
        open_positions=2,
        realized_pnl=Decimal("0"),
        unrealized_pnl=Decimal("0"),
        fees=None,
        avg_slippage_bps=None,
        fill_rate=None,
        trade_count=0,
        error_count=0,
        avg_expected_edge=Decimal("0.02"),
        avg_realized_edge=Decimal("0.019"),
        expected_realized_gap=Decimal("0.001"),
        no_trade_reasons={"no_rebalance_needed": 1},
        details=details_two,
    )

    summary = _build_multi_cycle_research_summary(sleeve_repo=repo, cycles_completed=2)
    trend_summary = summary["sleeves"][0]
    assert trend_summary["trend_focus"]["cycles_with_signals"] == 1
    assert trend_summary["trend_sparsity"]["interpretation"] == "already_at_target"
    assert trend_summary["trend_construction"]["interpretation"] == "narrow_positive_universe_with_weight_saturation"
    assert trend_summary["trend_validation"]["interpretation"] == "construction_still_saturated"
    assert trend_summary["trend_signal"]["interpretation"] == "insufficient_data"


def test_trend_weight_shape_sensitivity_report_identifies_shape_as_primary_lever(monkeypatch) -> None:
    db_path = Path("data") / f"test_trend_weight_shape_{uuid4().hex}.db"
    monkeypatch.setenv("DB_PATH", str(db_path))
    settings = Settings()
    settings.ensure_directories()
    db = Database(settings.db_path)
    repo = SleeveRepository(db)
    details = {
        "signals_generated": 0,
        "targets_generated": 3,
        "cash": "25000",
        "gross_exposure": "50000",
        "drawdown": "0",
        "position_count": 2,
        "trend_cycle_diagnostics": {
            "current_weights": {"SPY": "0.35", "QQQ": "0.35"},
            "construction": {
                "rows": [
                    {
                        "symbol": "SPY",
                        "momentum_return": "0.12",
                        "realized_volatility": "0.08",
                        "raw_target_weight_before_scale": "0.35",
                        "final_target_weight": "0.35",
                        "capped_at_max_weight": True,
                    },
                    {
                        "symbol": "QQQ",
                        "momentum_return": "0.08",
                        "realized_volatility": "0.10",
                        "raw_target_weight_before_scale": "0.35",
                        "final_target_weight": "0.35",
                        "capped_at_max_weight": True,
                    },
                    {
                        "symbol": "TLT",
                        "momentum_return": "0.03",
                        "realized_volatility": "0.40",
                        "raw_target_weight_before_scale": "0",
                        "final_target_weight": "0",
                        "capped_at_max_weight": False,
                    },
                ]
            },
        },
    }
    for _ in range(3):
        repo.append_metrics_snapshot(
            sleeve_name="trend",
            run_mode="replay_paper",
            status="running",
            capital_assigned=Decimal("75000"),
            opportunities_seen=3,
            trades_attempted=0,
            trades_filled=0,
            open_positions=2,
            realized_pnl=Decimal("0"),
            unrealized_pnl=Decimal("0"),
            fees=None,
            avg_slippage_bps=None,
            fill_rate=None,
            trade_count=0,
            error_count=0,
            avg_expected_edge=Decimal("0.02"),
            avg_realized_edge=Decimal("0.019"),
            expected_realized_gap=Decimal("0.001"),
            no_trade_reasons={"no_rebalance_needed": 1},
            details=details,
        )
    report = _build_trend_weight_shape_sensitivity_report(
        sleeve_repo=repo,
        cycles_completed=3,
        target_vol=Decimal("0.10"),
        max_weight=Decimal("0.35"),
        max_gross_exposure=Decimal("1.0"),
        min_rebalance_fraction=Decimal("0.10"),
    )
    assert report["interpretation"] == "weight_shape_is_primary_construction_lever"
    modes = {item["mode"]: item for item in report["modes"]}
    assert modes["current"]["avg_capped_target_count"] == "2"
