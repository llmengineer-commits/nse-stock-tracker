-- NSE Kenya Stock Tracker - ClickHouse Schema
-- Ian Nyaga | DE Zoomcamp 2026 Capstone
--
-- ClickHouse is a columnar DB optimised for analytical queries.
-- We're using MergeTree engines throughout — it's the go-to for time-series
-- data like stock ticks because it handles range scans on timestamps very fast.

CREATE DATABASE IF NOT EXISTS nse;

-- ── RAW TICKS ──────────────────────────────────────────────────────────────
-- Every record polled from yfinance lands here unmodified.
-- Think of this as the bronze layer — raw, append-only, never updated.
-- Partitioned by month so old data doesn't slow down recent queries.
-- TTL of 2 years keeps the disk from filling up over time.

CREATE TABLE IF NOT EXISTS nse.raw_ticks
(
    ticker       String                COMMENT 'Yahoo Finance ticker e.g. SCOM.NR',
    name         String                COMMENT 'Short name e.g. SCOM',
    price        Float64               COMMENT 'Last traded price in KES',
    volume       Int64                 COMMENT '3-month average volume (proxy for live volume)',
    market_cap   Float64               COMMENT 'Market cap in KES',
    currency     String  DEFAULT 'KES',
    fetched_at   String                COMMENT 'ISO-8601 timestamp from producer (Africa/Nairobi)',
    exchange     String  DEFAULT 'NSE',
    inserted_at  DateTime DEFAULT now() COMMENT 'When ClickHouse received this row'
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(inserted_at)
ORDER BY (ticker, inserted_at)
TTL inserted_at + INTERVAL 2 YEAR
SETTINGS index_granularity = 8192;


-- ── 5-MINUTE OHLCV ─────────────────────────────────────────────────────────
-- PyFlink writes windowed aggregates here.
-- O/H/L/C are approximate because yfinance doesn't give true tick data —
-- we're computing them from the 5-min poll snapshots, which is fine for a
-- capstone project and honestly for most retail analytics use cases too.

CREATE TABLE IF NOT EXISTS nse.ohlcv_5min
(
    ticker        String,
    window_start  DateTime,
    window_end    DateTime,
    open_price    Float64,
    high_price    Float64,
    low_price     Float64,
    close_price   Float64,
    tick_count    Int64     COMMENT 'Number of polls captured in this window',
    inserted_at   DateTime DEFAULT now()
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(window_start)
ORDER BY (ticker, window_start)
SETTINGS index_granularity = 8192;


-- ── DAILY SUMMARY ──────────────────────────────────────────────────────────
-- dbt materialises this view after market close.
-- It's what Looker Studio reads for the main dashboard.
-- Defined here as a dummy placeholder so ClickHouse knows the schema exists
-- before dbt runs for the first time.

CREATE TABLE IF NOT EXISTS nse.daily_summary
(
    trade_date      Date,
    ticker          String,
    open_price      Float64,
    high_price      Float64,
    low_price       Float64,
    close_price     Float64,
    price_change    Float64   COMMENT 'close - open',
    pct_change      Float64   COMMENT '(close - open) / open * 100',
    total_ticks     Int64,
    updated_at      DateTime  DEFAULT now()
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(trade_date)
ORDER BY (trade_date, ticker);


-- ── TOP MOVERS ─────────────────────────────────────────────────────────────
-- Top 5 gainers and top 5 losers per day — what gets highlighted on the dashboard.

CREATE TABLE IF NOT EXISTS nse.top_movers
(
    trade_date   Date,
    direction    String    COMMENT 'gainer or loser',
    rank         Int8,
    ticker       String,
    close_price  Float64,
    pct_change   Float64
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(trade_date)
ORDER BY (trade_date, direction, rank);
