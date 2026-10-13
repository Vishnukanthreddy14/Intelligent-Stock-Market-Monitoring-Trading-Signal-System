-- ============================================================================
-- Real-Time Stock Market Analytics Database Schema
-- Database: PostgreSQL 15+
-- Designed for High-Throughput Stream Ingestion & Power BI DirectQuery
-- ============================================================================

-- Create database extensions if needed
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- 1. Raw Stock Ticks Table (From Kafka Topic: stock_ticks)
CREATE TABLE IF NOT EXISTS stock_ticks (
    id BIGSERIAL PRIMARY KEY,
    symbol VARCHAR(20) NOT NULL,
    price NUMERIC(12, 2) NOT NULL,
    open_price NUMERIC(12, 2),
    high_price NUMERIC(12, 2),
    low_price NUMERIC(12, 2),
    previous_close NUMERIC(12, 2),
    volume BIGINT DEFAULT 0,
    timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
    source VARCHAR(50) DEFAULT 'google_finance',
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Index for real-time querying by symbol and time range
CREATE INDEX IF NOT EXISTS idx_stock_ticks_symbol_timestamp 
    ON stock_ticks (symbol, timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_stock_ticks_timestamp 
    ON stock_ticks (timestamp DESC);


-- 2. Calculated Technical Features Table (From Flink Stream Processing)
CREATE TABLE IF NOT EXISTS stock_features (
    id BIGSERIAL PRIMARY KEY,
    symbol VARCHAR(20) NOT NULL,
    timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
    price NUMERIC(12, 2) NOT NULL,
    sma_1m NUMERIC(12, 2),
    sma_5m NUMERIC(12, 2),
    ema_1m NUMERIC(12, 2),
    ema_5m NUMERIC(12, 2),
    rolling_high NUMERIC(12, 2),
    rolling_low NUMERIC(12, 2),
    rsi_14 NUMERIC(8, 2),
    price_momentum NUMERIC(10, 4),
    returns_pct NUMERIC(10, 4),
    volatility NUMERIC(10, 4),
    window_start TIMESTAMP WITH TIME ZONE,
    window_end TIMESTAMP WITH TIME ZONE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_stock_features_symbol_timestamp 
    ON stock_features (symbol, timestamp DESC);


-- 3. Machine Learning Predictions & Signals Table (From Random Forest Model)
CREATE TABLE IF NOT EXISTS stock_signals (
    id BIGSERIAL PRIMARY KEY,
    symbol VARCHAR(20) NOT NULL,
    timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
    price NUMERIC(12, 2) NOT NULL,
    signal VARCHAR(10) NOT NULL CHECK (signal IN ('BUY', 'SELL', 'HOLD')),
    confidence NUMERIC(5, 2) NOT NULL, -- e.g., 78.50%
    prob_buy NUMERIC(5, 4),
    prob_sell NUMERIC(5, 4),
    prob_hold NUMERIC(5, 4),
    features_used JSONB,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_stock_signals_symbol_timestamp 
    ON stock_signals (symbol, timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_stock_signals_signal 
    ON stock_signals (signal);


-- 4. Daily Aggregated Summary (For historical reporting & Power BI trend charts)
CREATE TABLE IF NOT EXISTS daily_summary (
    symbol VARCHAR(20) NOT NULL,
    date DATE NOT NULL,
    open_price NUMERIC(12, 2),
    high_price NUMERIC(12, 2),
    low_price NUMERIC(12, 2),
    close_price NUMERIC(12, 2),
    volume BIGINT,
    vwap NUMERIC(12, 2),
    total_ticks INTEGER DEFAULT 0,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (symbol, date)
);

CREATE INDEX IF NOT EXISTS idx_daily_summary_date 
    ON daily_summary (date DESC);


-- 5. Helper Analytical View for Power BI Real-Time Dashboard
-- Combines current tick, calculated features, and latest ML signal
CREATE OR REPLACE VIEW v_live_stock_dashboard AS
WITH ranked_ticks AS (
    SELECT 
        t.symbol,
        t.price,
        t.open_price,
        t.high_price,
        t.low_price,
        t.volume,
        t.timestamp,
        ROW_NUMBER() OVER (PARTITION BY t.symbol ORDER BY t.timestamp DESC) as rn
    FROM stock_ticks t
),
ranked_features AS (
    SELECT 
        f.symbol,
        f.sma_1m,
        f.sma_5m,
        f.ema_1m,
        f.ema_5m,
        f.rsi_14,
        f.rolling_high,
        f.rolling_low,
        f.volatility,
        ROW_NUMBER() OVER (PARTITION BY f.symbol ORDER BY f.timestamp DESC) as rn
    FROM stock_features f
),
ranked_signals AS (
    SELECT 
        s.symbol,
        s.signal,
        s.confidence,
        s.prob_buy,
        s.prob_sell,
        s.prob_hold,
        s.timestamp as signal_time,
        ROW_NUMBER() OVER (PARTITION BY s.symbol ORDER BY s.timestamp DESC) as rn
    FROM stock_signals s
)
SELECT 
    t.symbol,
    t.price AS current_price,
    t.open_price,
    t.high_price AS day_high,
    t.low_price AS day_low,
    t.volume AS total_volume,
    t.timestamp AS tick_timestamp,
    f.sma_1m,
    f.sma_5m,
    f.ema_1m,
    f.ema_5m,
    f.rsi_14,
    f.rolling_high,
    f.rolling_low,
    f.volatility,
    COALESCE(s.signal, 'HOLD') AS current_signal,
    COALESCE(s.confidence, 50.0) AS signal_confidence,
    s.prob_buy,
    s.prob_sell,
    s.prob_hold,
    s.signal_time
FROM ranked_ticks t
LEFT JOIN ranked_features f ON t.symbol = f.symbol AND f.rn = 1
LEFT JOIN ranked_signals s ON t.symbol = s.symbol AND s.rn = 1
WHERE t.rn = 1;

-- 6. Trigger to populate initial daily_summary on new ticks
CREATE OR REPLACE FUNCTION update_daily_summary()
RETURNS TRIGGER AS $$
BEGIN
    INSERT INTO daily_summary (symbol, date, open_price, high_price, low_price, close_price, volume, total_ticks, updated_at)
    VALUES (
        NEW.symbol,
        DATE(NEW.timestamp),
        NEW.price,
        NEW.price,
        NEW.price,
        NEW.price,
        NEW.volume,
        1,
        CURRENT_TIMESTAMP
    )
    ON CONFLICT (symbol, date) DO UPDATE SET
        high_price = GREATEST(daily_summary.high_price, EXCLUDED.high_price),
        low_price = LEAST(daily_summary.low_price, EXCLUDED.low_price),
        close_price = EXCLUDED.close_price,
        volume = EXCLUDED.volume,
        total_ticks = daily_summary.total_ticks + 1,
        updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_update_daily_summary ON stock_ticks;
CREATE TRIGGER trg_update_daily_summary
    AFTER INSERT ON stock_ticks
    FOR EACH ROW
    EXECUTE FUNCTION update_daily_summary();
