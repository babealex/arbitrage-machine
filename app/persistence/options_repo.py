from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal

from app.core.money import to_decimal
from app.db import Database
from app.options.models import (
    ChainSurfaceFeatures,
    ExpirySurfaceFeatures,
    OptionChainSnapshot,
    OptionContract,
    OptionQuote,
    OptionStateVector,
)
from app.options.selection import StructureSelectionRecord
from app.options.scenario_engine import StructureScenarioSummary
from app.options.optimizer import StructureScore
from app.options.outcomes import OptionOutcomeEvaluation


class OptionsRepository:
    def __init__(self, db: Database) -> None:
        self.db = db

    def persist_chain_snapshot(self, snapshot: OptionChainSnapshot) -> None:
        self.db.execute(
            """
            DELETE FROM option_chain_quotes
            WHERE underlying = ? AND observed_at = ? AND source = ?
            """,
            (snapshot.underlying, snapshot.observed_at.isoformat(), snapshot.source),
        )
        self.db.execute(
            """
            INSERT OR REPLACE INTO option_chain_snapshots(
                underlying, observed_at, source, spot_price_decimal, forward_price_decimal,
                risk_free_rate_decimal, dividend_yield_decimal, realized_vol_20d_decimal
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                snapshot.underlying,
                snapshot.observed_at.isoformat(),
                snapshot.source,
                self._decimal_text(snapshot.spot_price),
                self._decimal_text(snapshot.forward_price if snapshot.forward_price is not None else snapshot.spot_price),
                self._decimal_text(snapshot.risk_free_rate),
                self._decimal_text(snapshot.dividend_yield),
                self._nullable_decimal(snapshot.realized_vol_20d),
            ),
        )
        for quote in snapshot.quotes:
            self.db.execute(
                """
                INSERT INTO option_chain_quotes(
                    underlying, observed_at, source, venue, expiration_date, option_type, strike_decimal,
                    bid_decimal, ask_decimal, last_decimal, iv_decimal, delta_decimal, gamma_decimal,
                    vega_decimal, theta_decimal, bid_size, ask_size, open_interest, volume
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    snapshot.underlying,
                    snapshot.observed_at.isoformat(),
                    snapshot.source,
                    quote.contract.venue,
                    quote.contract.expiration_date.isoformat(),
                    quote.contract.option_type,
                    self._decimal_text(quote.contract.strike),
                    self._nullable_decimal(quote.bid),
                    self._nullable_decimal(quote.ask),
                    self._nullable_decimal(quote.last),
                    self._nullable_decimal(quote.implied_volatility),
                    self._nullable_decimal(quote.delta),
                    self._nullable_decimal(quote.gamma),
                    self._nullable_decimal(quote.vega),
                    self._nullable_decimal(quote.theta),
                    quote.bid_size,
                    quote.ask_size,
                    quote.open_interest,
                    quote.volume,
                ),
            )

    def load_chain_snapshot(self, *, underlying: str, observed_at: datetime) -> OptionChainSnapshot:
        header = self.db.fetchone(
            """
            SELECT *
            FROM option_chain_snapshots
            WHERE underlying = ? AND observed_at = ?
            LIMIT 1
            """,
            (underlying, observed_at.isoformat()),
        )
        rows = self.db.fetchall(
            """
            SELECT *
            FROM option_chain_quotes
            WHERE underlying = ? AND observed_at = ?
            ORDER BY expiration_date ASC, option_type ASC, strike_decimal ASC
            """,
            (underlying, observed_at.isoformat()),
        )
        quotes = [
            OptionQuote(
                contract=OptionContract(
                    underlying=str(row["underlying"]),
                    expiration_date=date.fromisoformat(str(row["expiration_date"])),
                    strike=to_decimal(row["strike_decimal"]),
                    option_type=str(row["option_type"]),
                    venue=str(row["venue"]),
                ),
                observed_at=datetime.fromisoformat(str(row["observed_at"])),
                source=str(row["source"]),
                bid=None if row["bid_decimal"] is None else to_decimal(row["bid_decimal"]),
                ask=None if row["ask_decimal"] is None else to_decimal(row["ask_decimal"]),
                last=None if row["last_decimal"] is None else to_decimal(row["last_decimal"]),
                implied_volatility=None if row["iv_decimal"] is None else to_decimal(row["iv_decimal"]),
                delta=None if row["delta_decimal"] is None else to_decimal(row["delta_decimal"]),
                gamma=None if row["gamma_decimal"] is None else to_decimal(row["gamma_decimal"]),
                vega=None if row["vega_decimal"] is None else to_decimal(row["vega_decimal"]),
                theta=None if row["theta_decimal"] is None else to_decimal(row["theta_decimal"]),
                bid_size=None if row["bid_size"] is None else int(row["bid_size"]),
                ask_size=None if row["ask_size"] is None else int(row["ask_size"]),
                open_interest=None if row["open_interest"] is None else int(row["open_interest"]),
                volume=None if row["volume"] is None else int(row["volume"]),
            )
            for row in rows
        ]
        spot_price = to_decimal(header["spot_price_decimal"]) if header is not None else _infer_spot_price(quotes)
        return OptionChainSnapshot(
            underlying=underlying,
            observed_at=observed_at,
            source=str(header["source"]) if header is not None else (str(rows[0]["source"]) if rows else "unknown"),
            spot_price=spot_price,
            forward_price=None if header is None else to_decimal(header["forward_price_decimal"]),
            risk_free_rate=Decimal("0") if header is None else to_decimal(header["risk_free_rate_decimal"]),
            dividend_yield=Decimal("0") if header is None else to_decimal(header["dividend_yield_decimal"]),
            realized_vol_20d=None if header is None or header["realized_vol_20d_decimal"] is None else to_decimal(header["realized_vol_20d_decimal"]),
            quotes=quotes,
        )

    def load_latest_chain_snapshot(self, *, underlying: str) -> OptionChainSnapshot | None:
        row = self.db.fetchone(
            """
            SELECT observed_at
            FROM option_chain_snapshots
            WHERE underlying = ?
            ORDER BY observed_at DESC
            LIMIT 1
            """,
            (underlying,),
        )
        if row is None:
            return None
        return self.load_chain_snapshot(underlying=underlying, observed_at=datetime.fromisoformat(str(row["observed_at"])))

    def load_latest_chain_underlyings(self) -> list[str]:
        rows = self.db.fetchall(
            """
            SELECT DISTINCT underlying
            FROM option_chain_snapshots
            ORDER BY underlying ASC
            """
        )
        return [str(row["underlying"]) for row in rows]

    def persist_expiry_surface_features(self, items: list[ExpirySurfaceFeatures]) -> None:
        if items:
            first = items[0]
            self.db.execute(
                """
                DELETE FROM option_expiry_surface_features
                WHERE underlying = ? AND observed_at = ? AND source = ?
                """,
                (first.underlying, first.observed_at.isoformat(), first.source),
            )
        for item in items:
            self.db.execute(
                """
                INSERT INTO option_expiry_surface_features(
                    underlying, observed_at, source, expiration_date, days_to_expiry,
                    atm_iv_decimal, atm_total_variance_decimal, put_skew_95_decimal,
                    call_skew_105_decimal, quote_count
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    item.underlying,
                    item.observed_at.isoformat(),
                    item.source,
                    item.expiration_date.isoformat(),
                    item.days_to_expiry,
                    self._nullable_decimal(item.atm_iv),
                    self._nullable_decimal(item.atm_total_variance),
                    self._nullable_decimal(item.put_skew_95),
                    self._nullable_decimal(item.call_skew_105),
                    item.quote_count,
                ),
            )

    def persist_chain_surface_features(self, item: ChainSurfaceFeatures) -> None:
        self.db.execute(
            """
            DELETE FROM option_chain_surface_features
            WHERE underlying = ? AND observed_at = ? AND source = ?
            """,
            (item.underlying, item.observed_at.isoformat(), item.source),
        )
        self.db.execute(
            """
            INSERT INTO option_chain_surface_features(
                underlying, observed_at, source, spot_price_decimal, expiries_count,
                atm_iv_near_decimal, atm_iv_next_decimal, atm_total_variance_near_decimal,
                atm_total_variance_next_decimal, atm_iv_term_slope_decimal, event_premium_proxy_decimal
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                item.underlying,
                item.observed_at.isoformat(),
                item.source,
                self._decimal_text(item.spot_price),
                item.expiries_count,
                self._nullable_decimal(item.atm_iv_near),
                self._nullable_decimal(item.atm_iv_next),
                self._nullable_decimal(item.atm_total_variance_near),
                self._nullable_decimal(item.atm_total_variance_next),
                self._nullable_decimal(item.atm_iv_term_slope),
                self._nullable_decimal(item.event_premium_proxy),
            ),
        )

    def persist_state_vector(self, item: OptionStateVector) -> None:
        self.db.execute(
            """
            DELETE FROM option_state_vectors
            WHERE underlying = ? AND observed_at = ? AND source = ?
            """,
            (item.underlying, item.observed_at.isoformat(), item.source),
        )
        self.db.execute(
            """
            INSERT INTO option_state_vectors(
                underlying, observed_at, source, spot_price_decimal, forward_price_decimal,
                realized_vol_20d_decimal, atm_iv_near_decimal, atm_iv_next_decimal,
                atm_iv_term_slope_decimal, event_premium_proxy_decimal, vrp_proxy_near_decimal,
                total_variance_term_monotonic
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                item.underlying,
                item.observed_at.isoformat(),
                item.source,
                self._decimal_text(item.spot_price),
                self._decimal_text(item.forward_price),
                self._nullable_decimal(item.realized_vol_20d),
                self._nullable_decimal(item.atm_iv_near),
                self._nullable_decimal(item.atm_iv_next),
                self._nullable_decimal(item.atm_iv_term_slope),
                self._nullable_decimal(item.event_premium_proxy),
                self._nullable_decimal(item.vrp_proxy_near),
                1 if item.total_variance_term_monotonic else 0,
            ),
        )

    def load_expiry_surface_features(self, *, underlying: str, observed_at: datetime) -> list[ExpirySurfaceFeatures]:
        rows = self.db.fetchall(
            """
            SELECT *
            FROM option_expiry_surface_features
            WHERE underlying = ? AND observed_at = ?
            ORDER BY expiration_date ASC, id ASC
            """,
            (underlying, observed_at.isoformat()),
        )
        return [
            ExpirySurfaceFeatures(
                underlying=str(row["underlying"]),
                observed_at=datetime.fromisoformat(str(row["observed_at"])),
                source=str(row["source"]),
                expiration_date=date.fromisoformat(str(row["expiration_date"])),
                days_to_expiry=int(row["days_to_expiry"]),
                atm_iv=None if row["atm_iv_decimal"] is None else to_decimal(row["atm_iv_decimal"]),
                atm_total_variance=None if row["atm_total_variance_decimal"] is None else to_decimal(row["atm_total_variance_decimal"]),
                put_skew_95=None if row["put_skew_95_decimal"] is None else to_decimal(row["put_skew_95_decimal"]),
                call_skew_105=None if row["call_skew_105_decimal"] is None else to_decimal(row["call_skew_105_decimal"]),
                quote_count=int(row["quote_count"]),
            )
            for row in rows
        ]

    def load_chain_surface_features(self, *, underlying: str, observed_at: datetime) -> ChainSurfaceFeatures | None:
        row = self.db.fetchone(
            """
            SELECT *
            FROM option_chain_surface_features
            WHERE underlying = ? AND observed_at = ?
            ORDER BY id DESC
            LIMIT 1
            """,
            (underlying, observed_at.isoformat()),
        )
        if row is None:
            return None
        return ChainSurfaceFeatures(
            underlying=str(row["underlying"]),
            observed_at=datetime.fromisoformat(str(row["observed_at"])),
            source=str(row["source"]),
            spot_price=to_decimal(row["spot_price_decimal"]),
            expiries_count=int(row["expiries_count"]),
            atm_iv_near=None if row["atm_iv_near_decimal"] is None else to_decimal(row["atm_iv_near_decimal"]),
            atm_iv_next=None if row["atm_iv_next_decimal"] is None else to_decimal(row["atm_iv_next_decimal"]),
            atm_total_variance_near=None if row["atm_total_variance_near_decimal"] is None else to_decimal(row["atm_total_variance_near_decimal"]),
            atm_total_variance_next=None if row["atm_total_variance_next_decimal"] is None else to_decimal(row["atm_total_variance_next_decimal"]),
            atm_iv_term_slope=None if row["atm_iv_term_slope_decimal"] is None else to_decimal(row["atm_iv_term_slope_decimal"]),
            event_premium_proxy=None if row["event_premium_proxy_decimal"] is None else to_decimal(row["event_premium_proxy_decimal"]),
        )

    def load_state_vector(self, *, underlying: str, observed_at: datetime) -> OptionStateVector | None:
        row = self.db.fetchone(
            """
            SELECT *
            FROM option_state_vectors
            WHERE underlying = ? AND observed_at = ?
            ORDER BY id DESC
            LIMIT 1
            """,
            (underlying, observed_at.isoformat()),
        )
        if row is None:
            return None
        return OptionStateVector(
            underlying=str(row["underlying"]),
            observed_at=datetime.fromisoformat(str(row["observed_at"])),
            source=str(row["source"]),
            spot_price=to_decimal(row["spot_price_decimal"]),
            forward_price=to_decimal(row["forward_price_decimal"]),
            realized_vol_20d=None if row["realized_vol_20d_decimal"] is None else to_decimal(row["realized_vol_20d_decimal"]),
            atm_iv_near=None if row["atm_iv_near_decimal"] is None else to_decimal(row["atm_iv_near_decimal"]),
            atm_iv_next=None if row["atm_iv_next_decimal"] is None else to_decimal(row["atm_iv_next_decimal"]),
            atm_iv_term_slope=None if row["atm_iv_term_slope_decimal"] is None else to_decimal(row["atm_iv_term_slope_decimal"]),
            event_premium_proxy=None if row["event_premium_proxy_decimal"] is None else to_decimal(row["event_premium_proxy_decimal"]),
            vrp_proxy_near=None if row["vrp_proxy_near_decimal"] is None else to_decimal(row["vrp_proxy_near_decimal"]),
            total_variance_term_monotonic=bool(row["total_variance_term_monotonic"]),
        )

    def persist_structure_scenario_summary(self, item: StructureScenarioSummary) -> None:
        self.db.execute(
            """
            DELETE FROM option_structure_scenario_summaries
            WHERE underlying = ? AND observed_at = ? AND source = ? AND structure_name = ?
            """,
            (item.underlying, item.observed_at.isoformat(), item.source, item.structure_name),
        )
        self.db.execute(
            """
            INSERT INTO option_structure_scenario_summaries(
                underlying, observed_at, source, structure_name, horizon_days, path_count, seed,
                expected_pnl_decimal, probability_of_profit_decimal, cvar_95_loss_decimal,
                expected_max_drawdown_decimal, stop_out_probability_decimal, predicted_daily_vol_decimal,
                expected_entry_cost_decimal, stress_expected_pnl_decimal, model_uncertainty_penalty_decimal
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                item.underlying,
                item.observed_at.isoformat(),
                item.source,
                item.structure_name,
                item.horizon_days,
                item.path_count,
                item.seed,
                self._decimal_text(item.expected_pnl),
                self._decimal_text(item.probability_of_profit),
                self._decimal_text(item.cvar_95_loss),
                self._decimal_text(item.expected_max_drawdown),
                self._decimal_text(item.stop_out_probability),
                self._decimal_text(item.predicted_daily_vol),
                self._decimal_text(item.expected_entry_cost),
                self._nullable_decimal(item.stress_expected_pnl),
                self._nullable_decimal(item.model_uncertainty_penalty),
            ),
        )

    def load_structure_scenario_summary(
        self,
        *,
        underlying: str,
        observed_at: datetime,
        structure_name: str,
    ) -> StructureScenarioSummary | None:
        row = self.db.fetchone(
            """
            SELECT *
            FROM option_structure_scenario_summaries
            WHERE underlying = ? AND observed_at = ? AND structure_name = ?
            ORDER BY id DESC
            LIMIT 1
            """,
            (underlying, observed_at.isoformat(), structure_name),
        )
        if row is None:
            return None
        return StructureScenarioSummary(
            underlying=str(row["underlying"]),
            observed_at=datetime.fromisoformat(str(row["observed_at"])),
            source=str(row["source"]),
            structure_name=str(row["structure_name"]),
            horizon_days=int(row["horizon_days"]),
            path_count=int(row["path_count"]),
            seed=int(row["seed"]),
            expected_pnl=to_decimal(row["expected_pnl_decimal"]),
            probability_of_profit=to_decimal(row["probability_of_profit_decimal"]),
            cvar_95_loss=to_decimal(row["cvar_95_loss_decimal"]),
            expected_max_drawdown=to_decimal(row["expected_max_drawdown_decimal"]),
            stop_out_probability=to_decimal(row["stop_out_probability_decimal"]),
            predicted_daily_vol=to_decimal(row["predicted_daily_vol_decimal"]),
            expected_entry_cost=to_decimal(row["expected_entry_cost_decimal"]),
            stress_expected_pnl=None if row["stress_expected_pnl_decimal"] is None else to_decimal(row["stress_expected_pnl_decimal"]),
            model_uncertainty_penalty=None if row["model_uncertainty_penalty_decimal"] is None else to_decimal(row["model_uncertainty_penalty_decimal"]),
        )

    def persist_structure_selection(self, item: StructureSelectionRecord) -> None:
        self.db.execute(
            """
            DELETE FROM option_structure_selections
            WHERE underlying = ? AND observed_at = ? AND source = ?
            """,
            (item.underlying, item.observed_at.isoformat(), item.source),
        )
        self.db.execute(
            """
            INSERT INTO option_structure_selections(
                underlying, observed_at, source, regime_name, due_at_1d, due_at_3d, due_at_expiry, candidate_count, selected_structure_name, objective_value_decimal,
                horizon_days, path_count, seed, annual_drift_decimal, annual_volatility_decimal,
                event_day, event_jump_mean_decimal, event_jump_stddev_decimal,
                iv_mean_reversion_decimal, iv_regime_level_decimal, iv_volatility_decimal,
                event_crush_decimal, exit_spread_fraction_decimal, slippage_bps_decimal,
                stop_out_loss_decimal, tail_penalty_decimal, uncertainty_penalty_decimal,
                realized_return_decimal, realized_vol_decimal, realized_iv_change_decimal, realized_pnl_decimal,
                max_drawdown_realized_decimal, prediction_error_decimal, move_error_decimal, vol_error_decimal, iv_error_decimal
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                item.underlying,
                item.observed_at.isoformat(),
                item.source,
                item.regime_name,
                item.due_at_1d.isoformat(),
                item.due_at_3d.isoformat(),
                item.due_at_expiry.isoformat(),
                item.candidate_count,
                item.selected_structure_name,
                self._decimal_text(item.objective_value),
                item.horizon_days,
                item.path_count,
                item.seed,
                self._decimal_text(item.annual_drift),
                self._nullable_decimal(item.annual_volatility),
                item.event_day,
                self._decimal_text(item.event_jump_mean),
                self._decimal_text(item.event_jump_stddev),
                self._decimal_text(item.iv_mean_reversion),
                self._nullable_decimal(item.iv_regime_level),
                self._decimal_text(item.iv_volatility),
                self._decimal_text(item.event_crush),
                self._decimal_text(item.exit_spread_fraction),
                self._decimal_text(item.slippage_bps),
                self._nullable_decimal(item.stop_out_loss),
                self._decimal_text(item.tail_penalty),
                self._decimal_text(item.uncertainty_penalty),
                self._nullable_decimal(item.realized_return),
                self._nullable_decimal(item.realized_vol),
                self._nullable_decimal(item.realized_iv_change),
                self._nullable_decimal(item.realized_pnl),
                self._nullable_decimal(item.max_drawdown_realized),
                self._nullable_decimal(item.prediction_error),
                self._nullable_decimal(item.move_error),
                self._nullable_decimal(item.vol_error),
                self._nullable_decimal(item.iv_error),
            ),
        )

    def load_structure_selection(self, *, underlying: str, observed_at: datetime) -> StructureSelectionRecord | None:
        row = self.db.fetchone(
            """
            SELECT *
            FROM option_structure_selections
            WHERE underlying = ? AND observed_at = ?
            ORDER BY id DESC
            LIMIT 1
            """,
            (underlying, observed_at.isoformat()),
        )
        if row is None:
            return None
        return StructureSelectionRecord(
            underlying=str(row["underlying"]),
            observed_at=datetime.fromisoformat(str(row["observed_at"])),
            source=str(row["source"]),
            regime_name=str(row["regime_name"]),
            due_at_1d=datetime.fromisoformat(str(row["due_at_1d"])),
            due_at_3d=datetime.fromisoformat(str(row["due_at_3d"])),
            due_at_expiry=datetime.fromisoformat(str(row["due_at_expiry"])),
            candidate_count=int(row["candidate_count"]),
            selected_structure_name=str(row["selected_structure_name"]),
            objective_value=to_decimal(row["objective_value_decimal"]),
            horizon_days=int(row["horizon_days"]),
            path_count=int(row["path_count"]),
            seed=int(row["seed"]),
            annual_drift=to_decimal(row["annual_drift_decimal"]),
            annual_volatility=None if row["annual_volatility_decimal"] is None else to_decimal(row["annual_volatility_decimal"]),
            event_day=None if row["event_day"] is None else int(row["event_day"]),
            event_jump_mean=to_decimal(row["event_jump_mean_decimal"]),
            event_jump_stddev=to_decimal(row["event_jump_stddev_decimal"]),
            iv_mean_reversion=to_decimal(row["iv_mean_reversion_decimal"]),
            iv_regime_level=None if row["iv_regime_level_decimal"] is None else to_decimal(row["iv_regime_level_decimal"]),
            iv_volatility=to_decimal(row["iv_volatility_decimal"]),
            event_crush=to_decimal(row["event_crush_decimal"]),
            exit_spread_fraction=to_decimal(row["exit_spread_fraction_decimal"]),
            slippage_bps=to_decimal(row["slippage_bps_decimal"]),
            stop_out_loss=None if row["stop_out_loss_decimal"] is None else to_decimal(row["stop_out_loss_decimal"]),
            tail_penalty=to_decimal(row["tail_penalty_decimal"]),
            uncertainty_penalty=to_decimal(row["uncertainty_penalty_decimal"]),
            realized_return=None if row["realized_return_decimal"] is None else to_decimal(row["realized_return_decimal"]),
            realized_vol=None if row["realized_vol_decimal"] is None else to_decimal(row["realized_vol_decimal"]),
            realized_iv_change=None if row["realized_iv_change_decimal"] is None else to_decimal(row["realized_iv_change_decimal"]),
            realized_pnl=None if row["realized_pnl_decimal"] is None else to_decimal(row["realized_pnl_decimal"]),
            max_drawdown_realized=None if row["max_drawdown_realized_decimal"] is None else to_decimal(row["max_drawdown_realized_decimal"]),
            prediction_error=None if row["prediction_error_decimal"] is None else to_decimal(row["prediction_error_decimal"]),
            move_error=None if row["move_error_decimal"] is None else to_decimal(row["move_error_decimal"]),
            vol_error=None if row["vol_error_decimal"] is None else to_decimal(row["vol_error_decimal"]),
            iv_error=None if row["iv_error_decimal"] is None else to_decimal(row["iv_error_decimal"]),
        )

    def persist_selection_outcome(self, item: OptionOutcomeEvaluation) -> None:
        self.db.execute(
            """
            DELETE FROM option_selection_outcomes
            WHERE underlying = ? AND decision_observed_at = ? AND evaluation_horizon = ?
            """,
            (item.underlying, item.decision_observed_at.isoformat(), item.evaluation_horizon),
        )
        self.db.execute(
            """
            INSERT INTO option_selection_outcomes(
                underlying, decision_observed_at, evaluation_horizon, evaluated_at, selected_structure_name,
                expected_pnl_decimal, realized_return_decimal, realized_vol_decimal, realized_iv_change_decimal,
                realized_pnl_decimal, max_drawdown_realized_decimal, prediction_error_decimal,
                move_error_decimal, vol_error_decimal, iv_error_decimal
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                item.underlying,
                item.decision_observed_at.isoformat(),
                item.evaluation_horizon,
                item.evaluated_at.isoformat(),
                item.selected_structure_name,
                self._decimal_text(item.expected_pnl),
                self._decimal_text(item.realized_return),
                self._nullable_decimal(item.realized_vol),
                self._nullable_decimal(item.realized_iv_change),
                self._nullable_decimal(item.realized_pnl),
                self._nullable_decimal(item.max_drawdown_realized),
                self._nullable_decimal(item.predicted_pnl_error),
                self._nullable_decimal(item.move_error),
                self._nullable_decimal(item.vol_error),
                self._nullable_decimal(item.iv_error),
            ),
        )
        self.db.execute(
            """
            UPDATE option_structure_selections
            SET realized_return_decimal = ?,
                realized_vol_decimal = ?,
                realized_iv_change_decimal = ?,
                realized_pnl_decimal = ?,
                max_drawdown_realized_decimal = ?,
                prediction_error_decimal = ?,
                move_error_decimal = ?,
                vol_error_decimal = ?,
                iv_error_decimal = ?
            WHERE underlying = ? AND observed_at = ?
            """,
            (
                self._decimal_text(item.realized_return),
                self._nullable_decimal(item.realized_vol),
                self._nullable_decimal(item.realized_iv_change),
                self._nullable_decimal(item.realized_pnl),
                self._nullable_decimal(item.max_drawdown_realized),
                self._nullable_decimal(item.predicted_pnl_error),
                self._nullable_decimal(item.move_error),
                self._nullable_decimal(item.vol_error),
                self._nullable_decimal(item.iv_error),
                item.underlying,
                item.decision_observed_at.isoformat(),
            ),
        )

    def load_selection_outcomes(self, *, underlying: str, decision_observed_at: datetime) -> list[OptionOutcomeEvaluation]:
        rows = self.db.fetchall(
            """
            SELECT *
            FROM option_selection_outcomes
            WHERE underlying = ? AND decision_observed_at = ?
            ORDER BY evaluated_at ASC, id ASC
            """,
            (underlying, decision_observed_at.isoformat()),
        )
        return [
            OptionOutcomeEvaluation(
                underlying=str(row["underlying"]),
                decision_observed_at=datetime.fromisoformat(str(row["decision_observed_at"])),
                evaluation_horizon=str(row["evaluation_horizon"]),
                evaluated_at=datetime.fromisoformat(str(row["evaluated_at"])),
                selected_structure_name=str(row["selected_structure_name"]),
                expected_pnl=to_decimal(row["expected_pnl_decimal"]),
                realized_return=to_decimal(row["realized_return_decimal"]),
                realized_vol=None if row["realized_vol_decimal"] is None else to_decimal(row["realized_vol_decimal"]),
                realized_iv_change=None if row["realized_iv_change_decimal"] is None else to_decimal(row["realized_iv_change_decimal"]),
                realized_pnl=None if row["realized_pnl_decimal"] is None else to_decimal(row["realized_pnl_decimal"]),
                max_drawdown_realized=None if row["max_drawdown_realized_decimal"] is None else to_decimal(row["max_drawdown_realized_decimal"]),
                predicted_pnl_error=None if row["prediction_error_decimal"] is None else to_decimal(row["prediction_error_decimal"]),
                move_error=None if row["move_error_decimal"] is None else to_decimal(row["move_error_decimal"]),
                vol_error=None if row["vol_error_decimal"] is None else to_decimal(row["vol_error_decimal"]),
                iv_error=None if row["iv_error_decimal"] is None else to_decimal(row["iv_error_decimal"]),
            )
            for row in rows
        ]

    def load_all_structure_selections(self) -> list[StructureSelectionRecord]:
        rows = self.db.fetchall(
            """
            SELECT *
            FROM option_structure_selections
            ORDER BY observed_at ASC, id ASC
            """
        )
        return [
            StructureSelectionRecord(
                underlying=str(row["underlying"]),
                observed_at=datetime.fromisoformat(str(row["observed_at"])),
                source=str(row["source"]),
                regime_name=str(row["regime_name"]),
                due_at_1d=datetime.fromisoformat(str(row["due_at_1d"])),
                due_at_3d=datetime.fromisoformat(str(row["due_at_3d"])),
                due_at_expiry=datetime.fromisoformat(str(row["due_at_expiry"])),
                candidate_count=int(row["candidate_count"]),
                selected_structure_name=str(row["selected_structure_name"]),
                objective_value=to_decimal(row["objective_value_decimal"]),
                horizon_days=int(row["horizon_days"]),
                path_count=int(row["path_count"]),
                seed=int(row["seed"]),
                annual_drift=to_decimal(row["annual_drift_decimal"]),
                annual_volatility=None if row["annual_volatility_decimal"] is None else to_decimal(row["annual_volatility_decimal"]),
                event_day=None if row["event_day"] is None else int(row["event_day"]),
                event_jump_mean=to_decimal(row["event_jump_mean_decimal"]),
                event_jump_stddev=to_decimal(row["event_jump_stddev_decimal"]),
                iv_mean_reversion=to_decimal(row["iv_mean_reversion_decimal"]),
                iv_regime_level=None if row["iv_regime_level_decimal"] is None else to_decimal(row["iv_regime_level_decimal"]),
                iv_volatility=to_decimal(row["iv_volatility_decimal"]),
                event_crush=to_decimal(row["event_crush_decimal"]),
                exit_spread_fraction=to_decimal(row["exit_spread_fraction_decimal"]),
                slippage_bps=to_decimal(row["slippage_bps_decimal"]),
                stop_out_loss=None if row["stop_out_loss_decimal"] is None else to_decimal(row["stop_out_loss_decimal"]),
                tail_penalty=to_decimal(row["tail_penalty_decimal"]),
                uncertainty_penalty=to_decimal(row["uncertainty_penalty_decimal"]),
                realized_return=None if row["realized_return_decimal"] is None else to_decimal(row["realized_return_decimal"]),
                realized_vol=None if row["realized_vol_decimal"] is None else to_decimal(row["realized_vol_decimal"]),
                realized_iv_change=None if row["realized_iv_change_decimal"] is None else to_decimal(row["realized_iv_change_decimal"]),
                realized_pnl=None if row["realized_pnl_decimal"] is None else to_decimal(row["realized_pnl_decimal"]),
                max_drawdown_realized=None if row["max_drawdown_realized_decimal"] is None else to_decimal(row["max_drawdown_realized_decimal"]),
                prediction_error=None if row["prediction_error_decimal"] is None else to_decimal(row["prediction_error_decimal"]),
                move_error=None if row["move_error_decimal"] is None else to_decimal(row["move_error_decimal"]),
                vol_error=None if row["vol_error_decimal"] is None else to_decimal(row["vol_error_decimal"]),
                iv_error=None if row["iv_error_decimal"] is None else to_decimal(row["iv_error_decimal"]),
            )
            for row in rows
        ]

    def load_all_selection_outcomes(self) -> list[OptionOutcomeEvaluation]:
        rows = self.db.fetchall(
            """
            SELECT *
            FROM option_selection_outcomes
            ORDER BY decision_observed_at ASC, evaluated_at ASC, id ASC
            """
        )
        return [
            OptionOutcomeEvaluation(
                underlying=str(row["underlying"]),
                decision_observed_at=datetime.fromisoformat(str(row["decision_observed_at"])),
                evaluation_horizon=str(row["evaluation_horizon"]),
                evaluated_at=datetime.fromisoformat(str(row["evaluated_at"])),
                selected_structure_name=str(row["selected_structure_name"]),
                expected_pnl=to_decimal(row["expected_pnl_decimal"]),
                realized_return=to_decimal(row["realized_return_decimal"]),
                realized_vol=None if row["realized_vol_decimal"] is None else to_decimal(row["realized_vol_decimal"]),
                realized_iv_change=None if row["realized_iv_change_decimal"] is None else to_decimal(row["realized_iv_change_decimal"]),
                realized_pnl=None if row["realized_pnl_decimal"] is None else to_decimal(row["realized_pnl_decimal"]),
                max_drawdown_realized=None if row["max_drawdown_realized_decimal"] is None else to_decimal(row["max_drawdown_realized_decimal"]),
                predicted_pnl_error=None if row["prediction_error_decimal"] is None else to_decimal(row["prediction_error_decimal"]),
                move_error=None if row["move_error_decimal"] is None else to_decimal(row["move_error_decimal"]),
                vol_error=None if row["vol_error_decimal"] is None else to_decimal(row["vol_error_decimal"]),
                iv_error=None if row["iv_error_decimal"] is None else to_decimal(row["iv_error_decimal"]),
            )
            for row in rows
        ]

    def persist_candidate_scores(
        self,
        *,
        underlying: str,
        observed_at: datetime,
        source: str,
        regime_name: str,
        scores: list[StructureScore],
    ) -> None:
        self.db.execute(
            """
            DELETE FROM option_candidate_scores
            WHERE underlying = ? AND observed_at = ? AND source = ?
            """,
            (underlying, observed_at.isoformat(), source),
        )
        for rank_index, score in enumerate(scores, start=1):
            self.db.execute(
                """
                INSERT INTO option_candidate_scores(
                    underlying, observed_at, source, regime_name, structure_name, rank_index,
                    objective_value_decimal, expected_pnl_decimal, probability_of_profit_decimal,
                    cvar_95_loss_decimal, expected_max_drawdown_decimal, stop_out_probability_decimal,
                    predicted_daily_vol_decimal, expected_entry_cost_decimal, stress_expected_pnl_decimal,
                    model_uncertainty_penalty_decimal
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    underlying,
                    observed_at.isoformat(),
                    source,
                    regime_name,
                    score.structure_name,
                    rank_index,
                    self._decimal_text(score.objective_value),
                    self._decimal_text(score.summary.expected_pnl),
                    self._decimal_text(score.summary.probability_of_profit),
                    self._decimal_text(score.summary.cvar_95_loss),
                    self._decimal_text(score.summary.expected_max_drawdown),
                    self._decimal_text(score.summary.stop_out_probability),
                    self._decimal_text(score.summary.predicted_daily_vol),
                    self._decimal_text(score.summary.expected_entry_cost),
                    self._nullable_decimal(score.summary.stress_expected_pnl),
                    self._nullable_decimal(score.summary.model_uncertainty_penalty),
                ),
            )

    def load_candidate_scores(self, *, underlying: str, observed_at: datetime) -> list[StructureScore]:
        rows = self.db.fetchall(
            """
            SELECT *
            FROM option_candidate_scores
            WHERE underlying = ? AND observed_at = ?
            ORDER BY rank_index ASC, id ASC
            """,
            (underlying, observed_at.isoformat()),
        )
        return [
            StructureScore(
                structure_name=str(row["structure_name"]),
                objective_value=to_decimal(row["objective_value_decimal"]),
                summary=StructureScenarioSummary(
                    underlying=str(row["underlying"]),
                    observed_at=datetime.fromisoformat(str(row["observed_at"])),
                    source=str(row["source"]),
                    structure_name=str(row["structure_name"]),
                    horizon_days=0,
                    path_count=0,
                    seed=0,
                    expected_pnl=to_decimal(row["expected_pnl_decimal"]),
                    probability_of_profit=to_decimal(row["probability_of_profit_decimal"]),
                    cvar_95_loss=to_decimal(row["cvar_95_loss_decimal"]),
                    expected_max_drawdown=to_decimal(row["expected_max_drawdown_decimal"]),
                    stop_out_probability=to_decimal(row["stop_out_probability_decimal"]),
                    predicted_daily_vol=to_decimal(row["predicted_daily_vol_decimal"]),
                    expected_entry_cost=to_decimal(row["expected_entry_cost_decimal"]),
                    stress_expected_pnl=None if row["stress_expected_pnl_decimal"] is None else to_decimal(row["stress_expected_pnl_decimal"]),
                    model_uncertainty_penalty=None if row["model_uncertainty_penalty_decimal"] is None else to_decimal(row["model_uncertainty_penalty_decimal"]),
                ),
            )
            for row in rows
        ]

    @staticmethod
    def _decimal_text(value: Decimal | str | int | float) -> str:
        return format(to_decimal(value), "f")

    @classmethod
    def _nullable_decimal(cls, value: Decimal | None) -> str | None:
        return None if value is None else cls._decimal_text(value)


def _infer_spot_price(quotes: list[OptionQuote]) -> Decimal:
    if not quotes:
        return Decimal("0")
    strikes = sorted(quote.contract.strike for quote in quotes)
    midpoint = len(strikes) // 2
    return strikes[midpoint]
