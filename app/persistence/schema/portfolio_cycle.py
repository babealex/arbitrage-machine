PORTFOLIO_CYCLE_SCHEMA = """
CREATE TABLE IF NOT EXISTS portfolio_cycle_snapshots (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id TEXT NOT NULL UNIQUE,
    observed_at TEXT NOT NULL,
    candidate_count INTEGER NOT NULL,
    approved_count INTEGER NOT NULL,
    rejected_count INTEGER NOT NULL,
    allocated_capital_decimal TEXT NOT NULL,
    expected_portfolio_ev_decimal TEXT NOT NULL,
    strategy_counts_json TEXT NOT NULL,
    rejection_counts_json TEXT NOT NULL,
    readiness_json TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_portfolio_cycle_snapshots_observed
ON portfolio_cycle_snapshots(observed_at DESC, id DESC);
"""
