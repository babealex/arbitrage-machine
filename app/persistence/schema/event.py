EVENT_SCHEMA = """
CREATE TABLE IF NOT EXISTS news_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    external_id TEXT NOT NULL UNIQUE,
    timestamp_utc TEXT NOT NULL,
    source TEXT NOT NULL,
    headline TEXT NOT NULL,
    body TEXT,
    url TEXT,
    metadata_json TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS classified_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    news_event_id INTEGER NOT NULL,
    event_type TEXT NOT NULL,
    region TEXT NOT NULL,
    severity REAL NOT NULL,
    directional_bias TEXT NOT NULL,
    assets_affected_json TEXT NOT NULL,
    metadata_json TEXT NOT NULL,
    created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS trade_candidates (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    classified_event_id INTEGER NOT NULL,
    symbol TEXT NOT NULL,
    side TEXT NOT NULL,
    event_type TEXT NOT NULL,
    status TEXT NOT NULL,
    confidence REAL NOT NULL,
    confirmation_score REAL NOT NULL,
    metadata_json TEXT NOT NULL,
    created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS event_entry_queue (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    trade_candidate_id INTEGER NOT NULL,
    due_at TEXT NOT NULL,
    symbol TEXT NOT NULL,
    side TEXT NOT NULL,
    quantity INTEGER NOT NULL,
    tranche_index INTEGER NOT NULL,
    total_tranches INTEGER NOT NULL,
    status TEXT NOT NULL,
    metadata_json TEXT NOT NULL,
    created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS event_positions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    trade_candidate_id INTEGER NOT NULL,
    news_event_id INTEGER NOT NULL,
    symbol TEXT NOT NULL,
    side TEXT NOT NULL,
    quantity INTEGER NOT NULL,
    entry_price REAL NOT NULL,
    stop_price REAL NOT NULL,
    opened_at TEXT NOT NULL,
    planned_exit_at TEXT NOT NULL,
    status TEXT NOT NULL,
    metadata_json TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS event_trade_outcomes (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    position_id INTEGER NOT NULL,
    symbol TEXT NOT NULL,
    entry_price REAL NOT NULL,
    exit_price REAL NOT NULL,
    return_pct REAL NOT NULL,
    holding_seconds REAL NOT NULL,
    exit_reason TEXT NOT NULL,
    metadata_json TEXT NOT NULL,
    closed_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS event_paper_execution_audit (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    queue_id INTEGER,
    position_id INTEGER,
    symbol TEXT NOT NULL,
    side TEXT NOT NULL,
    requested_quantity INTEGER NOT NULL,
    filled_quantity INTEGER NOT NULL,
    remaining_quantity INTEGER NOT NULL,
    order_style TEXT NOT NULL,
    status TEXT NOT NULL,
    decision_state TEXT NOT NULL,
    event_timestamp_utc TEXT NOT NULL,
    price REAL,
    metadata_json TEXT NOT NULL
);
"""
