from __future__ import annotations

from app.db import Database
from app.persistence.contracts import (
    BeliefReportingBundle,
    EventReportingBundle,
    KalshiReportingBundle,
)


class ReportingRepository:
    def __init__(self, db: Database) -> None:
        self.db = db

    def latest_signal_id(self) -> int:
        return self._max_id("signals")

    def latest_strategy_evaluation_id(self) -> int:
        return self._max_id("strategy_evaluations")

    def load_kalshi_reporting_bundle(self) -> KalshiReportingBundle:
        return KalshiReportingBundle(
            evaluations=self.db.fetch_table_rows("strategy_evaluations"),
            signals=self.db.fetch_table_rows("signals"),
            orders=self.db.fetch_table_rows("orders"),
            order_events=self.db.fetch_table_rows("order_events"),
            fills=self.db.fetch_table_rows("fills"),
            execution_outcomes=self.db.fetch_table_rows("execution_outcomes"),
            observations=self.db.fetch_table_rows("monotonicity_signal_observations"),
            cross_market_snapshots=self.db.fetch_table_rows("cross_market_snapshots"),
            risk_events=self.db.fetch_table_rows("risk_events"),
        )

    def load_belief_reporting_bundle(self) -> BeliefReportingBundle:
        return BeliefReportingBundle(
            belief_sources=self.db.fetch_table_rows("belief_sources"),
            event_definitions=self.db.fetch_table_rows("event_definitions"),
            belief_snapshots=self.db.fetch_table_rows("belief_snapshots"),
            disagreement_snapshots=self.db.fetch_table_rows("disagreement_snapshots"),
            cross_market_disagreements=self.db.fetch_table_rows("cross_market_disagreements"),
            outcome_realizations=self.db.fetch_table_rows("outcome_realizations"),
            outcome_tracking=self.db.fetch_table_rows("outcome_tracking"),
        )

    def load_event_reporting_bundle(self) -> EventReportingBundle:
        return EventReportingBundle(
            news_events=self.db.fetch_table_rows("news_events"),
            classified_events=self.db.fetch_table_rows("classified_events"),
            trade_candidates=self.db.fetch_table_rows("trade_candidates"),
            entry_queue=self.db.fetch_table_rows("event_entry_queue"),
            positions=self.db.fetch_table_rows("event_positions"),
            trade_outcomes=self.db.fetch_table_rows("event_trade_outcomes"),
            paper_execution_audit=self.db.fetch_table_rows("event_paper_execution_audit"),
        )

    def _max_id(self, table: str) -> int:
        row = self.db.fetchone(f"SELECT MAX(id) AS max_id FROM {table}")
        return int(row["max_id"] or 0) if row else 0
