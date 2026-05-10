-- =====================================================
-- CryptoPulse - Database schema
-- Loaded automatically by Postgres on first startup
-- (mounted into /docker-entrypoint-initdb.d/).
-- =====================================================

SELECT 'CREATE DATABASE cryptopulse_airflow'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'cryptopulse_airflow')\gexec
-- ---------- Price observations ----------
-- One row per (symbol, minute) candle from Binance.
CREATE TABLE IF NOT EXISTS prices (
    id              BIGSERIAL PRIMARY KEY,
    symbol          VARCHAR(20)  NOT NULL,
    open_time       TIMESTAMPTZ  NOT NULL,
    open_price      NUMERIC(20, 8) NOT NULL,
    high_price      NUMERIC(20, 8) NOT NULL,
    low_price       NUMERIC(20, 8) NOT NULL,
    close_price     NUMERIC(20, 8) NOT NULL,
    volume          NUMERIC(30, 8) NOT NULL,
    ingested_at     TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    CONSTRAINT prices_symbol_time_unique UNIQUE (symbol, open_time)
);

CREATE INDEX IF NOT EXISTS idx_prices_symbol_time
    ON prices (symbol, open_time DESC);

-- ---------- News articles ----------
-- One row per article. Full text is stored short here; ES has the searchable copy.
CREATE TABLE IF NOT EXISTS news (
    id              BIGSERIAL PRIMARY KEY,
    article_uid     VARCHAR(255) NOT NULL UNIQUE,   -- hash of (link) for idempotency
    source          VARCHAR(64)  NOT NULL,          -- e.g. 'coindesk'
    title           TEXT         NOT NULL,
    summary         TEXT,
    link            TEXT         NOT NULL,
    published_at    TIMESTAMPTZ  NOT NULL,
    sentiment_score NUMERIC(6, 4),                  -- VADER compound, [-1, 1]
    sentiment_label VARCHAR(16),                    -- 'positive' / 'negative' / 'neutral'
    mentioned_symbols TEXT[],                       -- e.g. {'BTC','ETH'}
    ingested_at     TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_news_published_at
    ON news (published_at DESC);

CREATE INDEX IF NOT EXISTS idx_news_source
    ON news (source);

-- GIN index lets us query 'WHERE mentioned_symbols @> ARRAY[''BTC'']' fast.
CREATE INDEX IF NOT EXISTS idx_news_symbols
    ON news USING GIN (mentioned_symbols);

-- ---------- Detected anomalies ----------
CREATE TABLE IF NOT EXISTS anomalies (
    id              BIGSERIAL PRIMARY KEY,
    symbol          VARCHAR(20)  NOT NULL,
    detected_at     TIMESTAMPTZ  NOT NULL,          -- timestamp of the anomalous candle
    z_score         NUMERIC(8, 4) NOT NULL,
    return_pct      NUMERIC(10, 6) NOT NULL,        -- 1-min return at this point
    rolling_mean    NUMERIC(20, 8) NOT NULL,
    rolling_std     NUMERIC(20, 8) NOT NULL,
    direction       VARCHAR(8)   NOT NULL,          -- 'up' or 'down'
    created_at      TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    CONSTRAINT anomalies_symbol_time_unique UNIQUE (symbol, detected_at)
);

CREATE INDEX IF NOT EXISTS idx_anomalies_detected_at
    ON anomalies (detected_at DESC);

-- ---------- Anomaly <-> news links ----------
-- Many-to-many: each anomaly may link to multiple temporally-nearby articles.
CREATE TABLE IF NOT EXISTS anomaly_news_links (
    id              BIGSERIAL PRIMARY KEY,
    anomaly_id      BIGINT       NOT NULL REFERENCES anomalies(id) ON DELETE CASCADE,
    news_id         BIGINT       NOT NULL REFERENCES news(id)      ON DELETE CASCADE,
    time_offset_min INTEGER      NOT NULL,          -- news.published_at - anomaly.detected_at, in minutes
    created_at      TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    CONSTRAINT anomaly_news_links_unique UNIQUE (anomaly_id, news_id)
);

CREATE INDEX IF NOT EXISTS idx_links_anomaly
    ON anomaly_news_links (anomaly_id);
CREATE INDEX IF NOT EXISTS idx_links_news
    ON anomaly_news_links (news_id);
