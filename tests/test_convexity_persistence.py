from __future__ import annotations

from dataclasses import asdict
from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path

from app.db import Database
from app.execution.orders import build_order_intent
from app.instruments.models import InstrumentRef
from app.persistence.convexity_repo import ConvexityRepository
from app.strategies.convexity.lifecycle import ConvexityRunSummary
from app.strategies.convexity.models import ConvexTradeCandidate, ConvexTradeOutcome, ConvexityOpportunity


def test_convexity_repo_round_trip(tmp_path: Path) -> None:
    db = Database(tmp_path / "convexity.db")
    repo = ConvexityRepository(db)
    observed_at = datetime(2026, 3, 22, 12, 0, tzinfo=timezone.utc)
    opportunity = ConvexityOpportunity(
        symbol="SPY",
        setup_type="macro",
        event_time=observed_at,
        spot_price=Decimal("500"),
        expected_move_pct=Decimal("0.03"),
        implied_move_pct=Decimal("0.02"),
        realized_vol_pct=Decimal("0.025"),
        implied_vol_pct=Decimal("0.02"),
        volume_spike_ratio=Decimal("2.0"),
        breakout_score=Decimal("0.4"),
        metadata={"direction": "bullish"},
    )
    candidate = ConvexTradeCandidate(
        symbol="SPY",
        setup_type="macro",
        structure_type="long_call",
        expiry=date(2026, 4, 17),
        legs=[{"symbol": "SPY_20260417C510", "expiry": date(2026, 4, 17), "strike": Decimal("510"), "option_type": "call", "quantity": 1, "premium": Decimal("1.20")}],
        debit=Decimal("120"),
        max_loss=Decimal("120"),
        max_profit_estimate=Decimal("900"),
        reward_to_risk=Decimal("7.5"),
        expected_value=Decimal("35"),
        expected_value_after_costs=Decimal("25"),
        convex_score=Decimal("9.5"),
        breakout_sensitivity=Decimal("0.5"),
        liquidity_score=Decimal("0.9"),
        confidence_score=Decimal("0.7"),
        metadata={"candidate_id": "cand-1", "setup_type": "macro"},
    )
    intent = build_order_intent(
        instrument=InstrumentRef(symbol="SPY_20260417C510", venue="listed_option"),
        side="buy",
        quantity=1,
        order_type="limit",
        strategy_name="convexity",
        signal_ref="cand-1",
        limit_price=Decimal("1.20"),
        seed="cand-1:0",
        time_in_force="DAY",
    )
    outcome = ConvexTradeOutcome(
        candidate_id="cand-1",
        symbol="SPY",
        structure_type="long_call",
        entry_cost=Decimal("120"),
        max_loss=Decimal("120"),
        realized_pnl=Decimal("240"),
        realized_multiple=Decimal("2"),
        held_to_exit=False,
        exit_reason="paper_mark_exit",
        metadata={
            "order_intent_id": 1,
            "timestamp_opened": observed_at.isoformat(),
            "timestamp_closed": observed_at.isoformat(),
            "setup_type": "macro",
            "expected_value_after_costs": "25",
        },
    )
    summary = ConvexityRunSummary(
        run_id="run-1",
        timestamp=observed_at.isoformat(),
        scanned_opportunities=1,
        candidate_trades=1,
        accepted_trades=1,
        rejected_for_liquidity=0,
        rejected_for_negative_ev=0,
        rejected_for_risk_budget=0,
        rejected_other=0,
        open_convexity_risk=Decimal("120"),
        avg_reward_to_risk=Decimal("7.5"),
        avg_expected_value_after_costs=Decimal("25"),
        metadata_json={"paper_execution_only": True},
    )

    opportunity_ids = repo.persist_opportunities("run-1", [opportunity], timestamp=observed_at)
    repo.persist_candidates(
        "run-1",
        [candidate],
        {"cand-1": "accepted"},
        {},
        opportunity_id_map={"SPY": opportunity_ids[0]},
        timestamp=observed_at,
    )
    repo.persist_order_intents("run-1", [intent], ["cand-1"], candidate_lookup={"cand-1": candidate}, timestamp=observed_at)
    repo.persist_trade_outcomes([outcome])
    repo.persist_run_summary(asdict(summary))

    assert len(repo.load_recent_candidate_history()) == 1
    assert len(repo.load_recent_outcomes()) == 1
    latest = repo.load_latest_run_summary()
    assert latest is not None
    assert latest["accepted_trades"] == 1
    assert latest["avg_reward_to_risk_decimal"] == "7.5"
