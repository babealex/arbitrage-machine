TREND_BROKER_SCHEMA = """
CREATE TABLE IF NOT EXISTS trend_broker_readiness_checks (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    broker_name TEXT NOT NULL,
    checked_at TEXT NOT NULL,
    is_ready INTEGER NOT NULL,
    reasons_json TEXT NOT NULL,
    last_successful_auth_at TEXT,
    last_market_data_check_at TEXT,
    last_reconciliation_check_at TEXT
);
CREATE TABLE IF NOT EXISTS trend_broker_session_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    broker_name TEXT NOT NULL,
    event_type TEXT NOT NULL,
    status TEXT NOT NULL,
    reason TEXT,
    occurred_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS trend_broker_order_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    broker_name TEXT NOT NULL,
    broker_order_id TEXT,
    client_order_id TEXT NOT NULL,
    symbol TEXT NOT NULL,
    side TEXT NOT NULL,
    quantity_decimal TEXT NOT NULL,
    order_type TEXT NOT NULL,
    limit_price_decimal TEXT,
    time_in_force TEXT NOT NULL,
    status TEXT NOT NULL,
    reject_reason TEXT,
    event_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS trend_broker_fills (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    broker_name TEXT NOT NULL,
    broker_fill_id TEXT NOT NULL,
    broker_order_id TEXT NOT NULL,
    client_order_id TEXT NOT NULL,
    symbol TEXT NOT NULL,
    side TEXT NOT NULL,
    quantity_decimal TEXT NOT NULL,
    price_decimal TEXT NOT NULL,
    filled_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS trend_broker_reconciliation_runs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    broker_name TEXT NOT NULL,
    checked_at TEXT NOT NULL,
    is_consistent INTEGER NOT NULL,
    position_diff_count INTEGER NOT NULL,
    open_order_diff_count INTEGER NOT NULL,
    cash_diff_decimal TEXT,
    reasons_json TEXT NOT NULL
);
"""
