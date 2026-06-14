-- Create the Airflow metadata database (Airflow stores its own data here)
CREATE DATABASE airflow_db;
-- ─────────────────────────────────────────────────────────────────────────────
-- BRONZE LAYER — raw data exactly as received from the API
-- ─────────────────────────────────────────────────────────────────────────────

CREATE SCHEMA IF NOT EXISTS bronze;

CREATE TABLE IF NOT EXISTS bronze.weather_raw (
    id         BIGSERIAL    PRIMARY KEY,
    city       VARCHAR(100) NOT NULL,
    fetched_at TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    payload    JSONB        NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_bronze_city_fetched
    ON bronze.weather_raw (city, fetched_at DESC);

-- ─────────────────────────────────────────────────────────────────────────────
-- SILVER LAYER — clean, parsed, typed hourly readings
-- ─────────────────────────────────────────────────────────────────────────────

CREATE SCHEMA IF NOT EXISTS silver;

CREATE TABLE IF NOT EXISTS silver.weather_readings (
    id                  BIGSERIAL    PRIMARY KEY,
    city                VARCHAR(100) NOT NULL,
    recorded_at         TIMESTAMPTZ  NOT NULL,    
    fetched_at          TIMESTAMPTZ  NOT NULL,    
    temp_celsius        NUMERIC(5,2),         
    apparent_temp       NUMERIC(5,2),             
    humidity_pct        SMALLINT,                 
    precipitation_mm    NUMERIC(6,3),        
    rain_mm             NUMERIC(6,3),             
    weather_code        SMALLINT,                
    pressure_hpa        NUMERIC(7,2),     
    cloud_cover_pct     SMALLINT,                 
    wind_speed_mps      NUMERIC(5,2),           
    wind_direction_deg  SMALLINT,                 
    wind_gusts_mps      NUMERIC(5,2),               
    visibility_m        INTEGER
);

CREATE UNIQUE INDEX IF NOT EXISTS uidx_silver_city_recorded
    ON silver.weather_readings (city, recorded_at);

CREATE INDEX IF NOT EXISTS idx_silver_recorded_at
    ON silver.weather_readings (recorded_at DESC);

CREATE INDEX IF NOT EXISTS idx_silver_city_time
    ON silver.weather_readings (city, recorded_at DESC);

-- ─────────────────────────────────────────────────────────────────────────────
-- GOLD LAYER — pre-computed aggregates
-- ─────────────────────────────────────────────────────────────────────────────

CREATE SCHEMA IF NOT EXISTS gold;

-- Daily summary: one row per city per calendar day
CREATE TABLE IF NOT EXISTS gold.daily_summary (
    city             VARCHAR(100) NOT NULL,
    summary_date     DATE         NOT NULL,
    avg_temp         NUMERIC(5,2),   
    min_temp         NUMERIC(5,2),
    max_temp         NUMERIC(5,2),
    avg_humidity     NUMERIC(5,2),    
    avg_pressure     NUMERIC(7,2),    
    total_rain       NUMERIC(6,3),    
    avg_wind_speed   NUMERIC(5,2),   
    avg_wind_gusts   NUMERIC(5,2),    
    avg_visibility   NUMERIC(8,1),
    dominant_code    SMALLINT,       
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (city, summary_date)
);

CREATE INDEX IF NOT EXISTS idx_gold_daily_date
    ON gold.daily_summary (summary_date DESC);

-- Monthly summary: one row per city per month
CREATE TABLE IF NOT EXISTS gold.monthly_summary (
    city           VARCHAR(100) NOT NULL,
    year           SMALLINT     NOT NULL,
    month          SMALLINT     NOT NULL CHECK (month BETWEEN 1 AND 12),
    avg_temp       NUMERIC(5,2),
    min_temp       NUMERIC(5,2),
    max_temp       NUMERIC(5,2),
    total_rain     NUMERIC(7,3),
    rainy_days     SMALLINT,          
    avg_wind_speed NUMERIC(5,2),
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (city, year, month)
);

-- Temperature anomalies: flags unusual hourly readings
CREATE TABLE IF NOT EXISTS gold.temperature_anomalies (
    id           BIGSERIAL    PRIMARY KEY,
    city         VARCHAR(100) NOT NULL,
    recorded_at  TIMESTAMPTZ  NOT NULL,
    temp_celsius NUMERIC(5,2),
    baseline_avg NUMERIC(5,2),
    deviation    NUMERIC(5,2),   
    is_anomaly   BOOLEAN      NOT NULL DEFAULT FALSE,
    detected_at  TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    UNIQUE (city, recorded_at)
);

CREATE INDEX IF NOT EXISTS idx_gold_anomalies_city
    ON gold.temperature_anomalies (city, detected_at DESC)
    WHERE is_anomaly = TRUE;

-- Rainfall trends: rolling averages (a view — no storage needed)
CREATE OR REPLACE VIEW gold.rainfall_trends AS
SELECT
    city,
    summary_date,
    total_rain,
    ROUND(
        AVG(total_rain) OVER (
            PARTITION BY city
            ORDER BY summary_date
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        )::NUMERIC, 3
    ) AS rolling_7d_rain_mm,
    ROUND(
        AVG(total_rain) OVER (
            PARTITION BY city
            ORDER BY summary_date
            ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
        )::NUMERIC, 3
    ) AS rolling_30d_rain_mm
FROM gold.daily_summary
ORDER BY city, summary_date DESC;

COMMENT ON TABLE silver.weather_readings IS 'Clean parsed hourly readings.';
COMMENT ON TABLE gold.daily_summary      IS 'Daily aggregates.';
COMMENT ON TABLE gold.monthly_summary    IS 'Monthly aggregates.';
COMMENT ON TABLE gold.temperature_anomalies IS 'Hourly readings > 2 std deviations from 30-day mean.';
