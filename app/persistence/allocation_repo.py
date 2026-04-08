from __future__ import annotations

from datetime import datetime

from app.allocation.models import AllocationDecision, AllocationRun
from app.allocation.outcomes import AllocationAdjustment, AllocationOutcomeRecord
from app.core.money import to_decimal
from app.db import Database


class AllocationRepository:
    def __init__(self, db: Database) -> None:
        self.db = db

    def persist_run(self, run: AllocationRun) -> None:
        self.db.execute(
            """
            INSERT OR REPLACE INTO allocation_runs(
                run_id, observed_at, portfolio_equity_decimal, daily_realized_pnl_decimal,
                daily_loss_stop_active, gross_notional_cap_decimal, allocated_capital_decimal,
                expected_portfolio_ev_decimal, realized_pnl_decimal, candidate_count,
                approved_count, rejected_count
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                run.run_id,
                run.observed_at.isoformat(),
                format(run.portfolio_equity, "f"),
                format(run.daily_realized_pnl, "f"),
                1 if run.daily_loss_stop_active else 0,
                format(run.gross_notional_cap, "f"),
                format(run.allocated_capital, "f"),
                format(run.expected_portfolio_ev, "f"),
                None if run.realized_pnl is None else format(run.realized_pnl, "f"),
                run.candidate_count,
                run.approved_count,
                run.rejected_count,
            ),
        )

    def persist_decisions(self, decisions: list[AllocationDecision]) -> None:
        if not decisions:
            return
        self.db.execute("DELETE FROM allocation_decisions WHERE run_id = ?", (decisions[0].run_id,))
        for decision in decisions:
            self.db.execute(
                """
                INSERT INTO allocation_decisions(
                    run_id, candidate_id, strategy_family, source_kind, sleeve, regime_name,
                    observed_at, score_decimal, approved, rejection_reason, target_notional_decimal,
                    max_loss_budget_decimal, expected_pnl_after_costs_decimal, cvar_95_loss_decimal,
                    uncertainty_penalty_decimal, liquidity_penalty_decimal, horizon_days, correlation_bucket,
                    execution_price_decimal, slippage_estimate_decimal, fill_status
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    decision.run_id,
                    decision.candidate_id,
                    decision.strategy_family,
                    decision.source_kind,
                    decision.sleeve,
                    decision.regime_name,
                    decision.observed_at.isoformat(),
                    format(decision.score, "f"),
                    1 if decision.approved else 0,
                    decision.rejection_reason,
                    format(decision.target_notional, "f"),
                    format(decision.max_loss_budget, "f"),
                    format(decision.expected_pnl_after_costs, "f"),
                    format(decision.cvar_95_loss, "f"),
                    format(decision.uncertainty_penalty, "f"),
                    format(decision.liquidity_penalty, "f"),
                    decision.horizon_days,
                    decision.correlation_bucket,
                    None if decision.execution_price is None else format(decision.execution_price, "f"),
                    None if decision.slippage_estimate is None else format(decision.slippage_estimate, "f"),
                    decision.fill_status,
                ),
            )

    def load_run(self, run_id: str) -> AllocationRun | None:
        row = self.db.fetchone("SELECT * FROM allocation_runs WHERE run_id = ?", (run_id,))
        if row is None:
            return None
        return AllocationRun(
            run_id=str(row["run_id"]),
            observed_at=datetime.fromisoformat(str(row["observed_at"])),
            portfolio_equity=to_decimal(row["portfolio_equity_decimal"]),
            daily_realized_pnl=to_decimal(row["daily_realized_pnl_decimal"]),
            daily_loss_stop_active=bool(row["daily_loss_stop_active"]),
            gross_notional_cap=to_decimal(row["gross_notional_cap_decimal"]),
            candidate_count=int(row["candidate_count"]),
            approved_count=int(row["approved_count"]),
            rejected_count=int(row["rejected_count"]),
            allocated_capital=to_decimal(row["allocated_capital_decimal"] or "0"),
            expected_portfolio_ev=to_decimal(row["expected_portfolio_ev_decimal"] or "0"),
            realized_pnl=None if row["realized_pnl_decimal"] is None else to_decimal(row["realized_pnl_decimal"]),
        )

    def load_latest_run(self) -> AllocationRun | None:
        row = self.db.fetchone(
            """
            SELECT *
            FROM allocation_runs
            ORDER BY observed_at DESC, run_id DESC
            LIMIT 1
            """
        )
        if row is None:
            return None
        return AllocationRun(
            run_id=str(row["run_id"]),
            observed_at=datetime.fromisoformat(str(row["observed_at"])),
            portfolio_equity=to_decimal(row["portfolio_equity_decimal"]),
            daily_realized_pnl=to_decimal(row["daily_realized_pnl_decimal"]),
            daily_loss_stop_active=bool(row["daily_loss_stop_active"]),
            gross_notional_cap=to_decimal(row["gross_notional_cap_decimal"]),
            candidate_count=int(row["candidate_count"]),
            approved_count=int(row["approved_count"]),
            rejected_count=int(row["rejected_count"]),
            allocated_capital=to_decimal(row["allocated_capital_decimal"] or "0"),
            expected_portfolio_ev=to_decimal(row["expected_portfolio_ev_decimal"] or "0"),
            realized_pnl=None if row["realized_pnl_decimal"] is None else to_decimal(row["realized_pnl_decimal"]),
        )

    def load_decisions(self, run_id: str) -> list[AllocationDecision]:
        rows = self.db.fetchall(
            """
            SELECT *
            FROM allocation_decisions
            WHERE run_id = ?
            ORDER BY approved DESC, CAST(score_decimal AS REAL) DESC, id ASC
            """,
            (run_id,),
        )
        return [
            AllocationDecision(
                run_id=str(row["run_id"]),
                candidate_id=str(row["candidate_id"]),
                strategy_family=str(row["strategy_family"]),
                source_kind=str(row["source_kind"]),
                sleeve=str(row["sleeve"]),
                regime_name=str(row["regime_name"]),
                observed_at=datetime.fromisoformat(str(row["observed_at"])),
                score=to_decimal(row["score_decimal"]),
                approved=bool(row["approved"]),
                rejection_reason=None if row["rejection_reason"] is None else str(row["rejection_reason"]),
                target_notional=to_decimal(row["target_notional_decimal"]),
                max_loss_budget=to_decimal(row["max_loss_budget_decimal"]),
                expected_pnl_after_costs=to_decimal(row["expected_pnl_after_costs_decimal"]),
                cvar_95_loss=to_decimal(row["cvar_95_loss_decimal"]),
                uncertainty_penalty=to_decimal(row["uncertainty_penalty_decimal"]),
                liquidity_penalty=to_decimal(row["liquidity_penalty_decimal"]),
                horizon_days=int(row["horizon_days"]),
                correlation_bucket=str(row["correlation_bucket"]),
                execution_price=None if row["execution_price_decimal"] is None else to_decimal(row["execution_price_decimal"]),
                slippage_estimate=None if row["slippage_estimate_decimal"] is None else to_decimal(row["slippage_estimate_decimal"]),
                fill_status=None if row["fill_status"] is None else str(row["fill_status"]),
            )
            for row in rows
        ]

    def update_decision_execution(
        self,
        *,
        run_id: str,
        candidate_id: str,
        execution_price: str | None,
        slippage_estimate: str | None,
        fill_status: str | None,
    ) -> None:
        self.db.execute(
            """
            UPDATE allocation_decisions
            SET execution_price_decimal = ?,
                slippage_estimate_decimal = ?,
                fill_status = ?
            WHERE run_id = ? AND candidate_id = ?
            """,
            (execution_price, slippage_estimate, fill_status, run_id, candidate_id),
        )

    def persist_outcomes(self, outcomes: list[AllocationOutcomeRecord]) -> None:
        for outcome in outcomes:
            self.db.execute(
                """
                DELETE FROM allocation_outcomes
                WHERE candidate_id = ? AND allocation_run_id = ? AND outcome_horizon_days = ?
                """,
                (outcome.candidate_id, outcome.allocation_run_id, outcome.outcome_horizon_days),
            )
            self.db.execute(
                """
                INSERT INTO allocation_outcomes(
                    candidate_id, allocation_run_id, observed_at, outcome_horizon_days,
                    realized_pnl_decimal, realized_return_bps_decimal, realized_max_drawdown_decimal,
                    realized_vol_or_risk_proxy_decimal, expected_pnl_at_decision_decimal,
                    expected_cvar_at_decision_decimal, uncertainty_penalty_at_decision_decimal,
                    sleeve, correlation_bucket, regime_name, approved
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    outcome.candidate_id,
                    outcome.allocation_run_id,
                    outcome.observed_at.isoformat(),
                    outcome.outcome_horizon_days,
                    format(outcome.realized_pnl, "f"),
                    None if outcome.realized_return_bps is None else format(outcome.realized_return_bps, "f"),
                    None if outcome.realized_max_drawdown is None else format(outcome.realized_max_drawdown, "f"),
                    None if outcome.realized_vol_or_risk_proxy is None else format(outcome.realized_vol_or_risk_proxy, "f"),
                    format(outcome.expected_pnl_at_decision, "f"),
                    format(outcome.expected_cvar_at_decision, "f"),
                    format(outcome.uncertainty_penalty_at_decision, "f"),
                    outcome.sleeve,
                    outcome.correlation_bucket,
                    outcome.regime_name,
                    1 if outcome.approved else 0,
                ),
            )

    def load_outcomes_for_run(self, run_id: str) -> list[AllocationOutcomeRecord]:
        rows = self.db.fetchall(
            """
            SELECT *
            FROM allocation_outcomes
            WHERE allocation_run_id = ?
            ORDER BY observed_at ASC, id ASC
            """,
            (run_id,),
        )
        return [
            AllocationOutcomeRecord(
                candidate_id=str(row["candidate_id"]),
                allocation_run_id=str(row["allocation_run_id"]),
                observed_at=datetime.fromisoformat(str(row["observed_at"])),
                outcome_horizon_days=int(row["outcome_horizon_days"]),
                realized_pnl=to_decimal(row["realized_pnl_decimal"]),
                realized_return_bps=None if row["realized_return_bps_decimal"] is None else to_decimal(row["realized_return_bps_decimal"]),
                realized_max_drawdown=None if row["realized_max_drawdown_decimal"] is None else to_decimal(row["realized_max_drawdown_decimal"]),
                realized_vol_or_risk_proxy=None if row["realized_vol_or_risk_proxy_decimal"] is None else to_decimal(row["realized_vol_or_risk_proxy_decimal"]),
                expected_pnl_at_decision=to_decimal(row["expected_pnl_at_decision_decimal"]),
                expected_cvar_at_decision=to_decimal(row["expected_cvar_at_decision_decimal"]),
                uncertainty_penalty_at_decision=to_decimal(row["uncertainty_penalty_at_decision_decimal"]),
                sleeve=str(row["sleeve"]),
                correlation_bucket=str(row["correlation_bucket"]),
                regime_name=str(row["regime_name"]),
                approved=bool(row["approved"]),
            )
            for row in rows
        ]

    def load_outcomes(self) -> list[AllocationOutcomeRecord]:
        rows = self.db.fetchall("SELECT * FROM allocation_outcomes ORDER BY observed_at ASC, id ASC")
        return [
            AllocationOutcomeRecord(
                candidate_id=str(row["candidate_id"]),
                allocation_run_id=str(row["allocation_run_id"]),
                observed_at=datetime.fromisoformat(str(row["observed_at"])),
                outcome_horizon_days=int(row["outcome_horizon_days"]),
                realized_pnl=to_decimal(row["realized_pnl_decimal"]),
                realized_return_bps=None if row["realized_return_bps_decimal"] is None else to_decimal(row["realized_return_bps_decimal"]),
                realized_max_drawdown=None if row["realized_max_drawdown_decimal"] is None else to_decimal(row["realized_max_drawdown_decimal"]),
                realized_vol_or_risk_proxy=None if row["realized_vol_or_risk_proxy_decimal"] is None else to_decimal(row["realized_vol_or_risk_proxy_decimal"]),
                expected_pnl_at_decision=to_decimal(row["expected_pnl_at_decision_decimal"]),
                expected_cvar_at_decision=to_decimal(row["expected_cvar_at_decision_decimal"]),
                uncertainty_penalty_at_decision=to_decimal(row["uncertainty_penalty_at_decision_decimal"]),
                sleeve=str(row["sleeve"]),
                correlation_bucket=str(row["correlation_bucket"]),
                regime_name=str(row["regime_name"]),
                approved=bool(row["approved"]),
            )
            for row in rows
        ]

    def persist_adjustments(self, adjustments: list[AllocationAdjustment]) -> None:
        if not adjustments:
            return
        observed_at = adjustments[0].observed_at.isoformat()
        self.db.execute("DELETE FROM allocation_adjustments WHERE observed_at = ?", (observed_at,))
        for adjustment in adjustments:
            self.db.execute(
                """
                INSERT INTO allocation_adjustments(
                    scope_type, scope_key, observed_at, sample_size,
                    pnl_bias_multiplier_decimal, tail_risk_multiplier_decimal,
                    uncertainty_multiplier_adjustment_decimal
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    adjustment.scope_type,
                    adjustment.scope_key,
                    adjustment.observed_at.isoformat(),
                    adjustment.sample_size,
                    format(adjustment.pnl_bias_multiplier, "f"),
                    format(adjustment.tail_risk_multiplier, "f"),
                    format(adjustment.uncertainty_multiplier_adjustment, "f"),
                ),
            )

    def load_latest_adjustments(self) -> list[AllocationAdjustment]:
        latest = self.db.fetchone("SELECT MAX(observed_at) AS observed_at FROM allocation_adjustments")
        if latest is None or latest["observed_at"] is None:
            return []
        rows = self.db.fetchall(
            "SELECT * FROM allocation_adjustments WHERE observed_at = ? ORDER BY scope_type ASC, scope_key ASC",
            (str(latest["observed_at"]),),
        )
        return [
            AllocationAdjustment(
                scope_type=str(row["scope_type"]),
                scope_key=str(row["scope_key"]),
                observed_at=datetime.fromisoformat(str(row["observed_at"])),
                sample_size=int(row["sample_size"]),
                pnl_bias_multiplier=to_decimal(row["pnl_bias_multiplier_decimal"]),
                tail_risk_multiplier=to_decimal(row["tail_risk_multiplier_decimal"]),
                uncertainty_multiplier_adjustment=to_decimal(row["uncertainty_multiplier_adjustment_decimal"]),
            )
            for row in rows
        ]
