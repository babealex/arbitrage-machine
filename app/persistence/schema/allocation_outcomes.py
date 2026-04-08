ALLOCATION_OUTCOMES_SCHEMA = """
CREATE TABLE IF NOT EXISTS allocation_outcomes (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    candidate_id TEXT NOT NULL,
    allocation_run_id TEXT NOT NULL,
    observed_at TEXT NOT NULL,
    outcome_horizon_days INTEGER NOT NULL,
    realized_pnl_decimal TEXT NOT NULL,
    realized_return_bps_decimal TEXT,
    realized_max_drawdown_decimal TEXT,
    realized_vol_or_risk_proxy_decimal TEXT,
    expected_pnl_at_decision_decimal TEXT NOT NULL,
    expected_cvar_at_decision_decimal TEXT NOT NULL,
    uncertainty_penalty_at_decision_decimal TEXT NOT NULL,
    sleeve TEXT NOT NULL,
    correlation_bucket TEXT NOT NULL,
    regime_name TEXT NOT NULL,
    approved INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_allocation_outcomes_candidate
ON allocation_outcomes(candidate_id, outcome_horizon_days);

CREATE TABLE IF NOT EXISTS allocation_adjustments (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    scope_type TEXT NOT NULL,
    scope_key TEXT NOT NULL,
    observed_at TEXT NOT NULL,
    sample_size INTEGER NOT NULL,
    pnl_bias_multiplier_decimal TEXT NOT NULL,
    tail_risk_multiplier_decimal TEXT NOT NULL,
    uncertainty_multiplier_adjustment_decimal TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_allocation_adjustments_scope
ON allocation_adjustments(scope_type, scope_key, observed_at);
"""
