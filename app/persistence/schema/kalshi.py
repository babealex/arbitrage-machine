KALSHI_SCHEMA = """
CREATE TABLE IF NOT EXISTS signals (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    strategy TEXT NOT NULL,
    ticker TEXT NOT NULL,
    action TEXT NOT NULL,
    probability_edge REAL NOT NULL,
    expected_edge_bps REAL NOT NULL,
    quantity INTEGER NOT NULL,
    legs TEXT NOT NULL,
    metadata TEXT NOT NULL,
    created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS strategy_evaluations (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    strategy TEXT NOT NULL,
    ticker TEXT NOT NULL,
    generated INTEGER NOT NULL,
    reason TEXT NOT NULL,
    edge_before_fees REAL NOT NULL,
    edge_after_fees REAL NOT NULL,
    fee_estimate REAL NOT NULL,
    quantity INTEGER NOT NULL,
    metadata TEXT NOT NULL,
    created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS orders (
    client_order_id TEXT PRIMARY KEY,
    ticker TEXT NOT NULL,
    side TEXT NOT NULL,
    action TEXT NOT NULL,
    quantity INTEGER NOT NULL,
    price INTEGER NOT NULL,
    tif TEXT NOT NULL,
    status TEXT NOT NULL,
    metadata TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT
);
CREATE TABLE IF NOT EXISTS order_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    client_order_id TEXT NOT NULL,
    ticker TEXT NOT NULL,
    side TEXT NOT NULL,
    action TEXT NOT NULL,
    quantity INTEGER NOT NULL,
    price INTEGER NOT NULL,
    tif TEXT NOT NULL,
    status TEXT NOT NULL,
    metadata TEXT NOT NULL,
    created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS fills (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    order_id TEXT NOT NULL,
    ticker TEXT NOT NULL,
    quantity INTEGER NOT NULL,
    price INTEGER NOT NULL,
    side TEXT NOT NULL,
    created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS positions (
    ticker TEXT PRIMARY KEY,
    yes_contracts INTEGER NOT NULL,
    no_contracts INTEGER NOT NULL,
    mark_price REAL NOT NULL,
    updated_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS pnl_snapshots (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    equity REAL NOT NULL,
    cash REAL NOT NULL,
    daily_pnl REAL NOT NULL,
    payload TEXT NOT NULL,
    created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS monotonicity_signal_observations (
    signal_key TEXT PRIMARY KEY,
    ticker TEXT NOT NULL,
    lower_ticker TEXT NOT NULL,
    higher_ticker TEXT NOT NULL,
    product_family TEXT NOT NULL,
    accepted_at TEXT NOT NULL,
    edge_before_fees REAL NOT NULL,
    edge_after_fees REAL NOT NULL,
    lower_yes_ask_price REAL,
    lower_yes_ask_size INTEGER NOT NULL,
    higher_yes_bid_price REAL,
    higher_yes_bid_size INTEGER NOT NULL,
    next_refresh_status TEXT,
    next_refresh_observed_at TEXT,
    time_to_disappear_seconds REAL,
    latest_status TEXT,
    latest_observed_at TEXT,
    metadata TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS cross_market_snapshots (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp TEXT NOT NULL,
    event_ticker TEXT NOT NULL,
    contract_pair TEXT,
    kalshi_probability REAL NOT NULL,
    kalshi_bid REAL NOT NULL,
    kalshi_ask REAL NOT NULL,
    kalshi_yes_bid_size INTEGER NOT NULL,
    kalshi_yes_ask_size INTEGER NOT NULL,
    spy_price REAL,
    qqq_price REAL,
    tlt_price REAL,
    btc_price REAL,
    derived_risk_score REAL NOT NULL,
    notes TEXT NOT NULL
);
"""
