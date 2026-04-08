ALLOCATION_SCHEMA = """
CREATE TABLE IF NOT EXISTS allocation_runs (
    run_id TEXT PRIMARY KEY,
    observed_at TEXT NOT NULL,
    portfolio_equity_decimal TEXT NOT NULL,
    daily_realized_pnl_decimal TEXT NOT NULL,
    daily_loss_stop_active INTEGER NOT NULL,
    gross_notional_cap_decimal TEXT NOT NULL,
    allocated_capital_decimal TEXT NOT NULL DEFAULT '0',
    expected_portfolio_ev_decimal TEXT NOT NULL DEFAULT '0',
    realized_pnl_decimal TEXT,
    candidate_count INTEGER NOT NULL,
    approved_count INTEGER NOT NULL,
    rejected_count INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS allocation_decisions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id TEXT NOT NULL,
    candidate_id TEXT NOT NULL,
    strategy_family TEXT NOT NULL,
    source_kind TEXT NOT NULL,
    sleeve TEXT NOT NULL,
    regime_name TEXT NOT NULL,
    observed_at TEXT NOT NULL,
    score_decimal TEXT NOT NULL,
    approved INTEGER NOT NULL,
    rejection_reason TEXT,
    target_notional_decimal TEXT NOT NULL,
    max_loss_budget_decimal TEXT NOT NULL,
    expected_pnl_after_costs_decimal TEXT NOT NULL,
    cvar_95_loss_decimal TEXT NOT NULL,
    uncertainty_penalty_decimal TEXT NOT NULL,
    liquidity_penalty_decimal TEXT NOT NULL,
    horizon_days INTEGER NOT NULL,
    correlation_bucket TEXT NOT NULL,
    execution_price_decimal TEXT,
    slippage_estimate_decimal TEXT,
    fill_status TEXT
);
CREATE INDEX IF NOT EXISTS idx_allocation_decisions_run
ON allocation_decisions(run_id, approved, sleeve, strategy_family);
"""
