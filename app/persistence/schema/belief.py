BELIEF_SCHEMA = """
CREATE TABLE IF NOT EXISTS belief_snapshots (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp_utc TEXT NOT NULL,
    family TEXT NOT NULL,
    event_ticker TEXT NOT NULL,
    market_ticker TEXT NOT NULL,
    source_type TEXT NOT NULL,
    object_type TEXT NOT NULL,
    value REAL NOT NULL,
    bid REAL,
    ask REAL,
    size_bid INTEGER,
    size_ask INTEGER,
    metadata_json TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS disagreement_snapshots (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp_utc TEXT NOT NULL,
    event_group TEXT NOT NULL,
    kalshi_reference TEXT NOT NULL,
    external_reference TEXT NOT NULL,
    disagreement_score REAL NOT NULL,
    zscore REAL NOT NULL,
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
"""
