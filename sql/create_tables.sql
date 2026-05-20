-- =============================================================
-- create_tables.sql
-- Database: happiness_db
-- Creates: raw table, dimension tables, and fact table
-- =============================================================

-- ── Raw Table ──────────────────────────────────────────────────
-- Stores every Kafka event exactly as received, before any
-- validation or transformation. Supports traceability and auditing.
CREATE TABLE IF NOT EXISTS raw_happiness_events (
    raw_event_id        SERIAL PRIMARY KEY,
    raw_message         TEXT            NOT NULL,
    processing_status   VARCHAR(20)     NOT NULL DEFAULT 'PENDING',
    received_at         TIMESTAMPTZ     NOT NULL DEFAULT NOW(),

    CONSTRAINT chk_processing_status CHECK (
        processing_status IN ('PENDING', 'VALID', 'INVALID_SCHEMA', 'INVALID_VALUES', 'PREDICTION_ERROR')
    )
);

-- ── Dimension: Country ─────────────────────────────────────────
CREATE TABLE IF NOT EXISTS dim_country (
    country_id      SERIAL PRIMARY KEY,
    country_name    VARCHAR(100) NOT NULL UNIQUE
);

-- ── Dimension: Date ────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS dim_date (
    date_id     SERIAL PRIMARY KEY,
    year        INTEGER NOT NULL UNIQUE
);

-- ── Fact Table: Predictions ────────────────────────────────────
-- Stores prediction results linked to dimensions and raw events.
CREATE TABLE IF NOT EXISTS fact_predictions (
    prediction_id           SERIAL PRIMARY KEY,
    raw_event_id            INTEGER         NOT NULL REFERENCES raw_happiness_events(raw_event_id),
    country_id              INTEGER         NOT NULL REFERENCES dim_country(country_id),
    date_id                 INTEGER         NOT NULL REFERENCES dim_date(date_id),
    actual_score            NUMERIC(6, 4)   NOT NULL,
    predicted_score         NUMERIC(6, 4)   NOT NULL,
    prediction_error        NUMERIC(6, 4)   NOT NULL,
    prediction_timestamp    TIMESTAMPTZ     NOT NULL DEFAULT NOW()
);

-- ── Indexes ────────────────────────────────────────────────────
CREATE INDEX IF NOT EXISTS idx_fact_country  ON fact_predictions(country_id);
CREATE INDEX IF NOT EXISTS idx_fact_date     ON fact_predictions(date_id);
CREATE INDEX IF NOT EXISTS idx_fact_raw      ON fact_predictions(raw_event_id);
CREATE INDEX IF NOT EXISTS idx_raw_status    ON raw_happiness_events(processing_status);

-- ── Verification ───────────────────────────────────────────────
SELECT 'raw_happiness_events' AS table_name, COUNT(*) AS rows FROM raw_happiness_events
UNION ALL
SELECT 'dim_country',   COUNT(*) FROM dim_country
UNION ALL
SELECT 'dim_date',      COUNT(*) FROM dim_date
UNION ALL
SELECT 'fact_predictions', COUNT(*) FROM fact_predictions;