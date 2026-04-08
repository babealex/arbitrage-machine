BELIEF_SCHEMA = """
CREATE TABLE IF NOT EXISTS belief_sources (
    source_key TEXT PRIMARY KEY,
    source_type TEXT NOT NULL,
    venue TEXT,
    instrument_class TEXT,
    description TEXT NOT NULL,
    active INTEGER NOT NULL DEFAULT 1,
    metadata_json TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS event_definitions (
    event_key TEXT PRIMARY KEY,
    family TEXT NOT NULL,
    normalized_underlying_key TEXT NOT NULL,
    title TEXT NOT NULL,
    event_date TEXT,
    metadata_json TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS belief_snapshots (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp_utc TEXT NOT NULL,
    source_key TEXT,
    event_key TEXT,
    normalized_underlying_key TEXT,
    family TEXT NOT NULL,
    event_ticker TEXT NOT NULL,
    market_ticker TEXT NOT NULL,
    source_type TEXT NOT NULL,
    object_type TEXT NOT NULL,
    value REAL NOT NULL,
    confidence REAL,
    comparison_metric TEXT,
    payload_summary_json TEXT,
    belief_summary_json TEXT,
    quality_flags_json TEXT,
    cost_estimate_bps REAL,
    bid REAL,
    ask REAL,
    size_bid INTEGER,
    size_ask INTEGER,
    metadata_json TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS disagreement_snapshots (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp_utc TEXT NOT NULL,
    normalized_underlying_key TEXT,
    left_source_key TEXT,
    right_source_key TEXT,
    gross_disagreement REAL,
    cost_estimate_bps REAL,
    net_disagreement REAL,
    tradeable INTEGER NOT NULL DEFAULT 0,
    event_group TEXT NOT NULL,
    kalshi_reference TEXT NOT NULL,
    external_reference TEXT NOT NULL,
    disagreement_score REAL NOT NULL,
    zscore REAL NOT NULL,
    metadata_json TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS cross_market_disagreements (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp_utc TEXT NOT NULL,
    normalized_underlying_key TEXT NOT NULL,
    event_key TEXT NOT NULL,
    comparison_metric TEXT NOT NULL,
    left_source_key TEXT NOT NULL,
    right_source_key TEXT NOT NULL,
    left_value REAL NOT NULL,
    right_value REAL NOT NULL,
    gross_disagreement REAL NOT NULL,
    fees_bps REAL NOT NULL,
    slippage_bps REAL NOT NULL,
    execution_uncertainty_bps REAL NOT NULL,
    net_disagreement REAL NOT NULL,
    tradeable INTEGER NOT NULL,
    metadata_json TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS outcome_tracking (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp_detected TEXT NOT NULL,
    event_ticker TEXT NOT NULL,
    family TEXT NOT NULL,
    initial_value REAL NOT NULL,
    disagreement_score REAL NOT NULL,
    metadata_json TEXT NOT NULL,
    due_at_30s TEXT,
    due_at_60s TEXT,
    due_at_300s TEXT,
    due_at_900s TEXT,
    value_after_30s REAL,
    value_after_60s REAL,
    value_after_300s REAL,
    value_after_900s REAL,
    max_favorable_move REAL,
    max_adverse_move REAL,
    resolved_direction TEXT
);
CREATE TABLE IF NOT EXISTS outcome_realizations (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp_utc TEXT NOT NULL,
    event_key TEXT NOT NULL,
    normalized_underlying_key TEXT NOT NULL,
    outcome_type TEXT NOT NULL,
    realized_value REAL NOT NULL,
    metadata_json TEXT NOT NULL
);
"""
