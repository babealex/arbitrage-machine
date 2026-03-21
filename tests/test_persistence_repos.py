from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

from app.db import Database
from app.models import Signal, StrategyEvaluation, StrategyName
from app.persistence.contracts import (
    BeliefSnapshotRecord,
    CrossMarketSnapshotRecord,
    DisagreementSnapshotRecord,
    KalshiOrderRecord,
    MonotonicityObservationRecord,
    OutcomeTrackingRecord,
    OutcomeTrackingUpdate,
)
from app.persistence.belief_repo import BeliefRepository
from app.persistence.kalshi_repo import KalshiRepository
from app.persistence.reporting_repo import ReportingRepository
from app.execution.orders import build_order_intent


def test_kalshi_and_reporting_repositories_round_trip(tmp_path: Path) -> None:
    db = Database(tmp_path / "kalshi_repo.db")
    kalshi_repo = KalshiRepository(db)
    reporting_repo = ReportingRepository(db)

    kalshi_repo.persist_evaluation(
        StrategyEvaluation(
            strategy=StrategyName.CROSS_MARKET,
            ticker="FED-1",
            generated=True,
            reason="signal_generated",
            edge_before_fees=0.05,
            edge_after_fees=0.03,
            fee_estimate=0.01,
            quantity=2,
            metadata={"source": "test"},
        )
    )
    kalshi_repo.persist_signal(
        Signal(
            strategy=StrategyName.CROSS_MARKET,
            ticker="FED-1",
            action="buy_yes",
            probability_edge=0.03,
            expected_edge_bps=300,
            quantity=2,
            legs=[{"ticker": "FED-1", "action": "buy", "side": "yes", "price": 40, "tif": "IOC"}],
        )
    )
    intent = build_order_intent("FED-1", "yes", "buy", 2, 40, "IOC", "repo-test")
    kalshi_repo.persist_order(KalshiOrderRecord(intent=intent, status="created", metadata={"source": "test"}))

    assert kalshi_repo.max_evaluation_id() == 1
    assert kalshi_repo.max_signal_id() == 1
    kalshi_bundle = reporting_repo.load_kalshi_reporting_bundle()
    assert len(kalshi_bundle.evaluations) == 1
    assert len(kalshi_bundle.signals) == 1
    assert len(kalshi_bundle.orders) == 1
    assert len(kalshi_bundle.order_events) == 1


def test_belief_repository_due_outcomes_round_trip(tmp_path: Path) -> None:
    db = Database(tmp_path / "belief_repo.db")
    belief_repo = BeliefRepository(db)
    now = datetime(2026, 3, 20, 18, 0, 0, tzinfo=timezone.utc)

    belief_repo.persist_belief_snapshot(
        BeliefSnapshotRecord(
            timestamp_utc=now.isoformat(),
            family="KXECONSTATCPI",
            event_ticker="KXECONSTATCPI-26MAR",
            market_ticker="KXECONSTATCPI-26MAR-T1.0",
            source_type="kalshi",
            object_type="mean_belief",
            value=0.52,
            metadata_json={"source": "test"},
        )
    )
    belief_repo.persist_disagreement_snapshot(
        DisagreementSnapshotRecord(
            timestamp_utc=now.isoformat(),
            event_group="KXECONSTATCPI:KXECONSTATCPI-26MAR",
            kalshi_reference="KXECONSTATCPI-26MAR-T1.0",
            external_reference="SPY",
            disagreement_score=0.18,
            zscore=1.25,
            metadata_json={"source": "test"},
        )
    )
    row_id = belief_repo.persist_outcome_tracking(
        OutcomeTrackingRecord(
            timestamp_detected=now.isoformat(),
            event_ticker="KXECONSTATCPI-26MAR",
            family="KXECONSTATCPI",
            initial_value=0.52,
            disagreement_score=0.18,
            metadata_json={"source": "test"},
            due_at_30s=now.isoformat(),
        )
    )

    due_rows = belief_repo.due_outcomes(now.isoformat())
    assert len(due_rows) == 1
    assert int(due_rows[0]["id"]) == row_id

    belief_repo.update_outcome_tracking(
        row_id,
        OutcomeTrackingUpdate(
            metadata_json={"source": "test", "updated": True},
            due_at_30s=now.isoformat(),
            value_after_30s=0.55,
            max_favorable_move=0.03,
            max_adverse_move=-0.01,
            resolved_direction="up",
        ),
    )

    assert belief_repo.due_outcomes(now.isoformat()) == []


def test_kalshi_repository_typed_observation_and_cross_market_round_trip(tmp_path: Path) -> None:
    db = Database(tmp_path / "kalshi_observation_repo.db")
    kalshi_repo = KalshiRepository(db)
    reporting_repo = ReportingRepository(db)

    kalshi_repo.persist_monotonicity_observation(
        MonotonicityObservationRecord(
            signal_key="sig-1",
            ticker="PAIR-1",
            lower_ticker="LOW-1",
            higher_ticker="HIGH-1",
            product_family="KXECONSTATCPI",
            accepted_at=datetime(2026, 3, 20, 18, 0, 0, tzinfo=timezone.utc).isoformat(),
            edge_before_fees=0.08,
            edge_after_fees=0.05,
            lower_yes_ask_price=40,
            lower_yes_ask_size=100,
            higher_yes_bid_price=48,
            higher_yes_bid_size=120,
            latest_status="accepted",
            latest_observed_at=datetime(2026, 3, 20, 18, 0, 0, tzinfo=timezone.utc).isoformat(),
            metadata_json={"source": "test"},
        )
    )
    kalshi_repo.persist_cross_market_snapshot(
        CrossMarketSnapshotRecord(
            timestamp=datetime(2026, 3, 20, 18, 0, 5, tzinfo=timezone.utc).isoformat(),
            event_ticker="KXECONSTATCPI-26MAR",
            contract_pair="KXECONSTATCPI-26MAR-T1.1",
            kalshi_probability=0.52,
            kalshi_bid=51.0,
            kalshi_ask=53.0,
            kalshi_yes_bid_size=75,
            kalshi_yes_ask_size=80,
            spy_price=510.0,
            qqq_price=430.0,
            tlt_price=92.0,
            btc_price=80000.0,
            derived_risk_score=0.18,
            notes="test",
        )
    )

    kalshi_bundle = reporting_repo.load_kalshi_reporting_bundle()
    assert len(kalshi_bundle.observations) == 1
    assert kalshi_bundle.observations[0]["signal_key"] == "sig-1"
    assert len(kalshi_bundle.cross_market_snapshots) == 1
    assert kalshi_bundle.cross_market_snapshots[0]["event_ticker"] == "KXECONSTATCPI-26MAR"
