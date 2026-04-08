TREND_SCHEMA = """
CREATE TABLE IF NOT EXISTS daily_bars (
    symbol TEXT NOT NULL,
    bar_date TEXT NOT NULL,
    close REAL NOT NULL,
    close_decimal TEXT NOT NULL,
    PRIMARY KEY(symbol, bar_date)
);
CREATE TABLE IF NOT EXISTS trend_positions (
    symbol TEXT PRIMARY KEY,
    quantity INTEGER NOT NULL,
    average_price_cents INTEGER NOT NULL,
    market_price_cents INTEGER NOT NULL,
    market_value_decimal TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS trend_equity_snapshots (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    snapshot_date TEXT NOT NULL,
    equity_decimal TEXT NOT NULL,
    cash_decimal TEXT NOT NULL,
    gross_exposure_decimal TEXT NOT NULL,
    net_exposure_decimal TEXT NOT NULL,
    drawdown_decimal TEXT NOT NULL,
    trade_count INTEGER NOT NULL,
    payload TEXT NOT NULL,
    created_at TEXT NOT NULL
);
"""
