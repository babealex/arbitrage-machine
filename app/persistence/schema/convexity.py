CONVEXITY_SCHEMA = """
CREATE TABLE IF NOT EXISTS convexity_opportunity_snapshots (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id TEXT NOT NULL,
    timestamp TEXT NOT NULL,
    symbol TEXT NOT NULL,
    setup_type TEXT NOT NULL,
    event_time TEXT,
    spot_price_decimal TEXT NOT NULL,
    expected_move_pct_decimal TEXT NOT NULL,
    implied_move_pct_decimal TEXT NOT NULL,
    move_edge_pct_decimal TEXT NOT NULL,
    realized_vol_pct_decimal TEXT NOT NULL,
    implied_vol_pct_decimal TEXT NOT NULL,
    volume_spike_ratio_decimal TEXT,
    breakout_score_decimal TEXT,
    metadata_json TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS convexity_trade_candidates (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id TEXT NOT NULL,
    opportunity_id INTEGER,
    candidate_id TEXT NOT NULL,
    timestamp TEXT NOT NULL,
    symbol TEXT NOT NULL,
    setup_type TEXT NOT NULL,
    structure_type TEXT NOT NULL,
    expiry TEXT NOT NULL,
    legs_json TEXT NOT NULL,
    debit_decimal TEXT NOT NULL,
    max_loss_decimal TEXT NOT NULL,
    max_profit_estimate_decimal TEXT,
    reward_to_risk_decimal TEXT NOT NULL,
    expected_value_decimal TEXT NOT NULL,
    expected_value_after_costs_decimal TEXT NOT NULL,
    convex_score_decimal TEXT NOT NULL,
    breakout_sensitivity_decimal TEXT NOT NULL,
    liquidity_score_decimal TEXT NOT NULL,
    confidence_score_decimal TEXT NOT NULL,
    status TEXT NOT NULL,
    rejection_reason TEXT,
    metadata_json TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_convexity_candidates_run ON convexity_trade_candidates(run_id, status);

CREATE TABLE IF NOT EXISTS convexity_order_intents (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id TEXT NOT NULL,
    candidate_id TEXT NOT NULL,
    timestamp TEXT NOT NULL,
    symbol TEXT NOT NULL,
    structure_type TEXT NOT NULL,
    order_intent_json TEXT NOT NULL,
    risk_amount_decimal TEXT NOT NULL,
    limit_debit_decimal TEXT NOT NULL,
    status TEXT NOT NULL,
    metadata_json TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS convexity_trade_outcomes (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    candidate_id TEXT NOT NULL,
    order_intent_id INTEGER,
    timestamp_opened TEXT NOT NULL,
    timestamp_closed TEXT,
    symbol TEXT NOT NULL,
    structure_type TEXT NOT NULL,
    entry_cost_decimal TEXT NOT NULL,
    max_loss_decimal TEXT NOT NULL,
    realized_pnl_decimal TEXT NOT NULL,
    realized_multiple_decimal TEXT NOT NULL,
    exit_reason TEXT NOT NULL,
    held_to_exit INTEGER NOT NULL,
    metadata_json TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS convexity_run_summaries (
    run_id TEXT PRIMARY KEY,
    timestamp TEXT NOT NULL,
    scanned_opportunities INTEGER NOT NULL,
    candidate_trades INTEGER NOT NULL,
    accepted_trades INTEGER NOT NULL,
    rejected_for_liquidity INTEGER NOT NULL,
    rejected_for_negative_ev INTEGER NOT NULL,
    rejected_for_risk_budget INTEGER NOT NULL,
    rejected_other INTEGER NOT NULL,
    open_convexity_risk_decimal TEXT NOT NULL,
    avg_reward_to_risk_decimal TEXT NOT NULL,
    avg_expected_value_after_costs_decimal TEXT NOT NULL,
    metadata_json TEXT NOT NULL
);
"""
