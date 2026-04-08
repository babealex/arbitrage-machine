from __future__ import annotations

from dataclasses import asdict
from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path

from app.db import Database
from app.execution.orders import build_order_intent
from app.instruments.models import InstrumentRef
from app.persistence.convexity_repo import ConvexityRepository
from app.reporting import PaperTradingReporter
from app.strategies.convexity.lifecycle import ConvexityRunSummary
from app.strategies.convexity.models import ConvexTradeCandidate, ConvexTradeOutcome, ConvexityOpportunity


def test_convexity_report_block_exists_without_history(tmp_path: Path) -> None:
    db = Database(tmp_path / "report_empty.db")
    report = PaperTradingReporter(db, tmp_path / "report.json").write({})
    summary = report["convexity_engine_summary"]
    assert summary["enabled"] is True
    assert summary["live_execution_enabled"] is False
    assert summary["candidate_trades"] == 0
    assert summary["historical"]["total_trades"] == 0


def test_convexity_report_uses_persisted_history(tmp_path: Path) -> None:
    db = Database(tmp_path / "report_history.db")
    repo = ConvexityRepository(db)
    observed_at = datetime(2026, 3, 22, 12, 0, tzinfo=timezone.utc)
    opportunity = ConvexityOpportunity(
        symbol="QQQ",
        setup_type="earnings",
        event_time=observed_at,
        spot_price=Decimal("450"),
        expected_move_pct=Decimal("0.05"),
        implied_move_pct=Decimal("0.03"),
        realized_vol_pct=Decimal("0.04"),
        implied_vol_pct=Decimal("0.03"),
        volume_spike_ratio=Decimal("1.8"),
        breakout_score=Decimal("0.06"),
        metadata={"direction": "bullish"},
    )
    candidate = ConvexTradeCandidate(
        symbol="QQQ",
        setup_type="earnings",
        structure_type="long_call",
        expiry=date(2026, 4, 17),
        legs=[{"symbol": "QQQ_20260417C460", "expiry": date(2026, 4, 17), "strike": Decimal("460"), "option_type": "call", "quantity": 1, "premium": Decimal("1.50")}],
        debit=Decimal("150"),
        max_loss=Decimal("150"),
        max_profit_estimate=Decimal("900"),
        reward_to_risk=Decimal("6"),
        expected_value=Decimal("45"),
        expected_value_after_costs=Decimal("35"),
        convex_score=Decimal("10"),
        breakout_sensitivity=Decimal("0.6"),
        liquidity_score=Decimal("0.85"),
        confidence_score=Decimal("0.7"),
        metadata={"candidate_id": "qqq-1", "setup_type": "earnings"},
    )
    intent = build_order_intent(
        instrument=InstrumentRef(symbol="QQQ_20260417C460", venue="listed_option"),
        side="buy",
        quantity=1,
        order_type="limit",
        strategy_name="convexity",
        signal_ref="qqq-1",
        seed="qqq-1:0",
        limit_price=Decimal("1.50"),
        time_in_force="DAY",
    )
    outcome = ConvexTradeOutcome(
        candidate_id="qqq-1",
        symbol="QQQ",
        structure_type="long_call",
        entry_cost=Decimal("150"),
        max_loss=Decimal("150"),
        realized_pnl=Decimal("300"),
        realized_multiple=Decimal("2"),
        held_to_exit=False,
        exit_reason="paper_mark_exit",
        metadata={
            "order_intent_id": 1,
            "timestamp_opened": observed_at.isoformat(),
            "timestamp_closed": observed_at.isoformat(),
            "setup_type": "earnings",
            "expected_value_after_costs": "35",
        },
    )
    summary = ConvexityRunSummary(
        run_id="qqq-run",
        timestamp=observed_at.isoformat(),
        scanned_opportunities=1,
        candidate_trades=1,
        accepted_trades=1,
        rejected_for_liquidity=0,
        rejected_for_negative_ev=0,
        rejected_for_risk_budget=0,
        rejected_other=0,
        open_convexity_risk=Decimal("150"),
        avg_reward_to_risk=Decimal("6"),
        avg_expected_value_after_costs=Decimal("35"),
        metadata_json={
            "paper_execution_only": True,
            "input_underlyings": 2,
            "configured_macro_events": 1,
            "configured_earnings_events": 1,
            "recent_event_bias_rows": 1,
            "calendar_symbols_with_events": 2,
            "news_symbols_with_bias": 1,
        },
    )

    opp_ids = repo.persist_opportunities("qqq-run", [opportunity], timestamp=observed_at)
    repo.persist_candidates("qqq-run", [candidate], {"qqq-1": "accepted"}, {}, opportunity_id_map={"QQQ": opp_ids[0]}, timestamp=observed_at)
    repo.persist_order_intents("qqq-run", [intent], ["qqq-1"], candidate_lookup={"qqq-1": candidate}, timestamp=observed_at)
    repo.persist_trade_outcomes([outcome])
    repo.persist_run_summary(asdict(summary))

    report = PaperTradingReporter(db, tmp_path / "report_history.json").write({})
    block = report["convexity_engine_summary"]
    assert block["candidate_trades"] == 1
    assert block["accepted_trades"] == 1
    assert block["historical"]["total_trades"] == 1
    assert block["historical"]["max_winner_multiple"] == "2"
    assert block["runtime_inputs"]["configured_macro_events"] == 1
    assert block["selection_yield"] == "1.000000"
