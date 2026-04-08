CORE_SCHEMA = """
CREATE TABLE IF NOT EXISTS markets (
    ticker TEXT PRIMARY KEY,
    event_ticker TEXT NOT NULL,
    title TEXT NOT NULL,
    subtitle TEXT,
    status TEXT NOT NULL,
    strike REAL,
    metadata TEXT,
    updated_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS ladders (
    event_ticker TEXT NOT NULL,
    ladder_key TEXT NOT NULL,
    members TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    PRIMARY KEY (event_ticker, ladder_key)
);
CREATE TABLE IF NOT EXISTS risk_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    state TEXT NOT NULL,
    reason TEXT NOT NULL,
    details TEXT NOT NULL,
    occurred_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS market_state_inputs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    venue TEXT NOT NULL,
    symbol TEXT NOT NULL,
    contract_side TEXT,
    observed_at TEXT NOT NULL,
    reference_price_cents INTEGER NOT NULL,
    order_side TEXT,
    best_bid_cents INTEGER,
    best_ask_cents INTEGER,
    bid_size INTEGER,
    ask_size INTEGER,
    source TEXT NOT NULL,
    provenance TEXT NOT NULL,
    kind TEXT NOT NULL,
    order_id TEXT
);
CREATE TABLE IF NOT EXISTS sleeve_runtime_status (
    sleeve_name TEXT PRIMARY KEY,
    maturity TEXT NOT NULL,
    run_mode TEXT NOT NULL,
    status TEXT NOT NULL,
    enabled INTEGER NOT NULL,
    paper_capable INTEGER NOT NULL,
    live_capable INTEGER NOT NULL,
    capital_weight_decimal TEXT NOT NULL,
    capital_assigned_decimal TEXT NOT NULL,
    opportunities_seen INTEGER NOT NULL,
    trades_attempted INTEGER NOT NULL,
    trades_filled INTEGER NOT NULL,
    open_positions INTEGER NOT NULL,
    realized_pnl_decimal TEXT,
    unrealized_pnl_decimal TEXT,
    fees_decimal TEXT,
    avg_slippage_bps_decimal TEXT,
    fill_rate_decimal TEXT,
    trade_count INTEGER NOT NULL,
    error_count INTEGER NOT NULL,
    avg_expected_edge_decimal TEXT,
    avg_realized_edge_decimal TEXT,
    expected_realized_gap_decimal TEXT,
    no_trade_reasons TEXT NOT NULL,
    details TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS sleeve_metrics_snapshots (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    sleeve_name TEXT NOT NULL,
    run_mode TEXT NOT NULL,
    status TEXT NOT NULL,
    capital_assigned_decimal TEXT NOT NULL,
    opportunities_seen INTEGER NOT NULL,
    trades_attempted INTEGER NOT NULL,
    trades_filled INTEGER NOT NULL,
    open_positions INTEGER NOT NULL,
    realized_pnl_decimal TEXT,
    unrealized_pnl_decimal TEXT,
    fees_decimal TEXT,
    avg_slippage_bps_decimal TEXT,
    fill_rate_decimal TEXT,
    trade_count INTEGER NOT NULL,
    error_count INTEGER NOT NULL,
    avg_expected_edge_decimal TEXT,
    avg_realized_edge_decimal TEXT,
    expected_realized_gap_decimal TEXT,
    no_trade_reasons TEXT NOT NULL,
    details TEXT NOT NULL,
    created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS sleeve_trade_ledger (
    trade_id TEXT PRIMARY KEY,
    sleeve_name TEXT NOT NULL,
    timestamp TEXT NOT NULL,
    instrument_or_market TEXT NOT NULL,
    signal_type TEXT,
    strategy_name TEXT,
    venue TEXT,
    expected_edge_decimal TEXT,
    expected_edge_after_costs_decimal TEXT,
    bid_decimal TEXT,
    ask_decimal TEXT,
    decision_price_reference_decimal TEXT,
    order_type TEXT,
    intended_size_decimal TEXT,
    filled_size_decimal TEXT,
    fill_price_decimal TEXT,
    fill_status TEXT,
    fees_decimal TEXT,
    slippage_estimate_bps_decimal TEXT,
    realized_edge_after_costs_decimal TEXT,
    realized_pnl_decimal TEXT,
    notes_or_error_code TEXT,
    order_id TEXT
);
CREATE TABLE IF NOT EXISTS sleeve_error_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    sleeve_name TEXT NOT NULL,
    error_code TEXT NOT NULL,
    message TEXT NOT NULL,
    details TEXT NOT NULL,
    created_at TEXT NOT NULL
);
"""
