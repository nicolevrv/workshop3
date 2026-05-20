-- =============================================================
-- kpis.sql
-- Database: happiness_db
-- Dashboard KPIs for Workshop 3 — Streaming ETL with Kafka
-- =============================================================


-- ── KPI 1: Average Prediction Error ───────────────────────────
-- Overall model performance metric.
-- Shows the mean absolute error across all predictions.
SELECT
    ROUND(AVG(prediction_error)::NUMERIC, 4)    AS avg_error,
    ROUND(MIN(prediction_error)::NUMERIC, 4)    AS min_error,
    ROUND(MAX(prediction_error)::NUMERIC, 4)    AS max_error,
    ROUND(STDDEV(prediction_error)::NUMERIC, 4) AS stddev_error,
    COUNT(*)                                     AS total_predictions
FROM fact_predictions;


-- ── KPI 2: Predictions by Country ─────────────────────────────
-- How many predictions were generated per country,
-- along with average actual and predicted scores.
SELECT
    c.country_name,
    COUNT(*)                                        AS total_predictions,
    ROUND(AVG(f.actual_score)::NUMERIC, 4)          AS avg_actual_score,
    ROUND(AVG(f.predicted_score)::NUMERIC, 4)       AS avg_predicted_score,
    ROUND(AVG(f.prediction_error)::NUMERIC, 4)      AS avg_error
FROM fact_predictions f
JOIN dim_country c ON f.country_id = c.country_id
GROUP BY c.country_name
ORDER BY total_predictions DESC, avg_actual_score DESC;


-- ── KPI 3: Predicted vs Actual Score ──────────────────────────
-- Side-by-side comparison of actual and predicted happiness scores
-- per country and year. Useful for scatter/bar charts.
SELECT
    c.country_name,
    d.year,
    ROUND(f.actual_score::NUMERIC, 4)       AS actual_score,
    ROUND(f.predicted_score::NUMERIC, 4)    AS predicted_score,
    ROUND(f.prediction_error::NUMERIC, 4)   AS prediction_error
FROM fact_predictions f
JOIN dim_country c ON f.country_id = c.country_id
JOIN dim_date    d ON f.date_id    = d.date_id
ORDER BY d.year, actual_score DESC;


-- ── KPI 4: Prediction Trends Over Time ────────────────────────
-- Average actual vs predicted happiness score per year.
-- Shows whether model performance is consistent across years.
SELECT
    d.year,
    COUNT(*)                                        AS total_predictions,
    ROUND(AVG(f.actual_score)::NUMERIC, 4)          AS avg_actual_score,
    ROUND(AVG(f.predicted_score)::NUMERIC, 4)       AS avg_predicted_score,
    ROUND(AVG(f.prediction_error)::NUMERIC, 4)      AS avg_error
FROM fact_predictions f
JOIN dim_date d ON f.date_id = d.date_id
GROUP BY d.year
ORDER BY d.year;


-- ── KPI 5: Top 10 Happiest Countries (by predicted score) ─────
-- Countries with the highest predicted happiness scores.
-- Averaged across all years available.
SELECT
    c.country_name,
    ROUND(AVG(f.predicted_score)::NUMERIC, 4)   AS avg_predicted_score,
    ROUND(AVG(f.actual_score)::NUMERIC, 4)       AS avg_actual_score,
    ROUND(AVG(f.prediction_error)::NUMERIC, 4)   AS avg_error
FROM fact_predictions f
JOIN dim_country c ON f.country_id = c.country_id
GROUP BY c.country_name
ORDER BY avg_predicted_score DESC
LIMIT 10;


-- ── KPI 6: Worst Predictions (highest error) ──────────────────
-- Countries and years where the model performed worst.
-- Useful for identifying model weaknesses.
SELECT
    c.country_name,
    d.year,
    ROUND(f.actual_score::NUMERIC, 4)       AS actual_score,
    ROUND(f.predicted_score::NUMERIC, 4)    AS predicted_score,
    ROUND(f.prediction_error::NUMERIC, 4)   AS prediction_error
FROM fact_predictions f
JOIN dim_country c ON f.country_id = c.country_id
JOIN dim_date    d ON f.date_id    = d.date_id
ORDER BY prediction_error DESC
LIMIT 20;


-- ── Bonus: Processing Status Summary ──────────────────────────
-- Overview of raw event quality — how many were valid,
-- invalid, or had errors. Good for pipeline health monitoring.
SELECT
    processing_status,
    COUNT(*)                                            AS total_events,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS percentage
FROM raw_happiness_events
GROUP BY processing_status
ORDER BY total_events DESC;