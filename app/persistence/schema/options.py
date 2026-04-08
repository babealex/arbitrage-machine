OPTIONS_SCHEMA = """
CREATE TABLE IF NOT EXISTS option_chain_snapshots (
    underlying TEXT NOT NULL,
    observed_at TEXT NOT NULL,
    source TEXT NOT NULL,
    spot_price_decimal TEXT NOT NULL,
    forward_price_decimal TEXT NOT NULL,
    risk_free_rate_decimal TEXT NOT NULL,
    dividend_yield_decimal TEXT NOT NULL,
    realized_vol_20d_decimal,
    PRIMARY KEY(underlying, observed_at, source)
);

CREATE TABLE IF NOT EXISTS option_chain_quotes (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    underlying TEXT NOT NULL,
    observed_at TEXT NOT NULL,
    source TEXT NOT NULL,
    venue TEXT NOT NULL,
    expiration_date TEXT NOT NULL,
    option_type TEXT NOT NULL,
    strike_decimal TEXT NOT NULL,
    bid_decimal TEXT,
    ask_decimal TEXT,
    last_decimal TEXT,
    iv_decimal TEXT,
    delta_decimal TEXT,
    gamma_decimal TEXT,
    vega_decimal TEXT,
    theta_decimal TEXT,
    bid_size INTEGER,
    ask_size INTEGER,
    open_interest INTEGER,
    volume INTEGER
);
CREATE INDEX IF NOT EXISTS idx_option_chain_quotes_lookup
ON option_chain_quotes(underlying, observed_at, expiration_date, option_type, strike_decimal);

CREATE TABLE IF NOT EXISTS option_expiry_surface_features (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    underlying TEXT NOT NULL,
    observed_at TEXT NOT NULL,
    source TEXT NOT NULL,
    expiration_date TEXT NOT NULL,
    days_to_expiry INTEGER NOT NULL,
    atm_iv_decimal TEXT,
    atm_total_variance_decimal TEXT,
    put_skew_95_decimal TEXT,
    call_skew_105_decimal TEXT,
    quote_count INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS option_chain_surface_features (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    underlying TEXT NOT NULL,
    observed_at TEXT NOT NULL,
    source TEXT NOT NULL,
    spot_price_decimal TEXT NOT NULL,
    expiries_count INTEGER NOT NULL,
    atm_iv_near_decimal TEXT,
    atm_iv_next_decimal TEXT,
    atm_total_variance_near_decimal TEXT,
    atm_total_variance_next_decimal TEXT,
    atm_iv_term_slope_decimal TEXT,
    event_premium_proxy_decimal TEXT
);

CREATE TABLE IF NOT EXISTS option_state_vectors (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    underlying TEXT NOT NULL,
    observed_at TEXT NOT NULL,
    source TEXT NOT NULL,
    spot_price_decimal TEXT NOT NULL,
    forward_price_decimal TEXT NOT NULL,
    realized_vol_20d_decimal,
    atm_iv_near_decimal,
    atm_iv_next_decimal,
    atm_iv_term_slope_decimal,
    event_premium_proxy_decimal,
    vrp_proxy_near_decimal,
    total_variance_term_monotonic INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS option_structure_scenario_summaries (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    underlying TEXT NOT NULL,
    observed_at TEXT NOT NULL,
    source TEXT NOT NULL,
    structure_name TEXT NOT NULL,
    horizon_days INTEGER NOT NULL,
    path_count INTEGER NOT NULL,
    seed INTEGER NOT NULL,
    expected_pnl_decimal TEXT NOT NULL,
    probability_of_profit_decimal TEXT NOT NULL,
    cvar_95_loss_decimal TEXT NOT NULL,
    expected_max_drawdown_decimal TEXT NOT NULL,
    stop_out_probability_decimal TEXT NOT NULL,
    predicted_daily_vol_decimal TEXT NOT NULL,
    expected_entry_cost_decimal TEXT NOT NULL,
    stress_expected_pnl_decimal,
    model_uncertainty_penalty_decimal
);

CREATE TABLE IF NOT EXISTS option_candidate_scores (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    underlying TEXT NOT NULL,
    observed_at TEXT NOT NULL,
    source TEXT NOT NULL,
    regime_name TEXT NOT NULL,
    structure_name TEXT NOT NULL,
    rank_index INTEGER NOT NULL,
    objective_value_decimal TEXT NOT NULL,
    expected_pnl_decimal TEXT NOT NULL,
    probability_of_profit_decimal TEXT NOT NULL,
    cvar_95_loss_decimal TEXT NOT NULL,
    expected_max_drawdown_decimal TEXT NOT NULL,
    stop_out_probability_decimal TEXT NOT NULL,
    predicted_daily_vol_decimal TEXT NOT NULL,
    expected_entry_cost_decimal TEXT NOT NULL,
    stress_expected_pnl_decimal,
    model_uncertainty_penalty_decimal
);

CREATE TABLE IF NOT EXISTS option_structure_selections (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    underlying TEXT NOT NULL,
    observed_at TEXT NOT NULL,
    source TEXT NOT NULL,
    regime_name TEXT NOT NULL,
    due_at_1d TEXT NOT NULL,
    due_at_3d TEXT NOT NULL,
    due_at_expiry TEXT NOT NULL,
    candidate_count INTEGER NOT NULL,
    selected_structure_name TEXT NOT NULL,
    objective_value_decimal TEXT NOT NULL,
    horizon_days INTEGER NOT NULL,
    path_count INTEGER NOT NULL,
    seed INTEGER NOT NULL,
    annual_drift_decimal TEXT NOT NULL,
    annual_volatility_decimal TEXT,
    event_day INTEGER,
    event_jump_mean_decimal TEXT NOT NULL,
    event_jump_stddev_decimal TEXT NOT NULL,
    iv_mean_reversion_decimal TEXT NOT NULL,
    iv_regime_level_decimal TEXT,
    iv_volatility_decimal TEXT NOT NULL,
    event_crush_decimal TEXT NOT NULL,
    exit_spread_fraction_decimal TEXT NOT NULL,
    slippage_bps_decimal TEXT NOT NULL,
    stop_out_loss_decimal TEXT,
    tail_penalty_decimal TEXT NOT NULL,
    uncertainty_penalty_decimal TEXT NOT NULL,
    realized_return_decimal TEXT,
    realized_vol_decimal TEXT,
    realized_iv_change_decimal TEXT,
    realized_pnl_decimal TEXT,
    max_drawdown_realized_decimal TEXT,
    prediction_error_decimal TEXT,
    move_error_decimal TEXT,
    vol_error_decimal TEXT,
    iv_error_decimal TEXT
);

CREATE TABLE IF NOT EXISTS option_selection_outcomes (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    underlying TEXT NOT NULL,
    decision_observed_at TEXT NOT NULL,
    evaluation_horizon TEXT NOT NULL,
    evaluated_at TEXT NOT NULL,
    selected_structure_name TEXT NOT NULL,
    expected_pnl_decimal TEXT NOT NULL,
    realized_return_decimal TEXT NOT NULL,
    realized_vol_decimal TEXT,
    realized_iv_change_decimal TEXT,
    realized_pnl_decimal TEXT,
    max_drawdown_realized_decimal TEXT,
    prediction_error_decimal TEXT,
    move_error_decimal TEXT,
    vol_error_decimal TEXT,
    iv_error_decimal TEXT
);
"""
