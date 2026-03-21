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
"""
