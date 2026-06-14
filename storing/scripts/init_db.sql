
DO $$
BEGIN
    -- Writer: used by Stage 1 (weather_ingest.py via SQLAlchemy)
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'weather_writer') THEN
        CREATE ROLE weather_writer WITH LOGIN PASSWORD 'writer_password_change_me';
    END IF;

    -- Reader: used by Stage 5 (Grafana)
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'grafana_reader') THEN
        CREATE ROLE grafana_reader WITH LOGIN PASSWORD 'reader_password_change_me';
    END IF;
END
$$;

CREATE SCHEMA IF NOT EXISTS bronze
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;

-- Grant schema usage to roles
GRANT USAGE ON SCHEMA raw       TO weather_writer;
GRANT USAGE ON SCHEMA silver TO weather_writer;
GRANT USAGE ON SCHEMA gold   TO weather_writer;
GRANT USAGE ON SCHEMA gold   TO grafana_reader;


CREATE TABLE IF NOT EXISTS bronze.weather_events (
    id          BIGSERIAL       PRIMARY KEY,
    city        VARCHAR(100)    NOT NULL,
    fetched_at  TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    source      VARCHAR(50)     NOT NULL DEFAULT 'open-meteo',
    payload     JSONB           NOT NULL
);

-- Index for querying by city and fetch time
CREATE INDEX IF NOT EXISTS idx_raw_events_city_fetched
    ON bronze.weather_events (city, fetched_at DESC);

-- Prevent ingesting the same city payload twice in the same second
CREATE UNIQUE INDEX IF NOT EXISTS uidx_raw_events_city_fetched
    ON bronze.weather_events (city, fetched_at);

GRANT INSERT, SELECT ON bronze.weather_events       TO weather_writer;
GRANT USAGE, SELECT  ON bronze.weather_events_id_seq TO weather_writer;

COMMENT ON TABLE bronze.weather_events IS
    'Immutable raw API payloads from Open-Meteo. One row per city per ingest run.';

CREATE TABLE IF NOT EXISTS silver.weather_readings (
    id                  BIGSERIAL       PRIMARY KEY,
    city                VARCHAR(100)    NOT NULL,
    recorded_at         TIMESTAMPTZ     NOT NULL,
    fetched_at          TIMESTAMPTZ     NOT NULL,
    temp_celsius        NUMERIC(5, 2),         
    apparent_temp       NUMERIC(5, 2),          
    humidity_pct        SMALLINT,               
    precipitation_mm    NUMERIC(6, 3),         
    rain_mm             NUMERIC(6, 3),
    weather_code        SMALLINT
    pressure_hpa        NUMERIC(7, 2),         
    cloud_cover_pct     SMALLINT,           
    wind_speed_mps      NUMERIC(5, 2),        
    wind_direction_deg  SMALLINT,
    wind_gusts_mps      NUMERIC(5,2),
    visibility          INTEGER
);

CREATE UNIQUE INDEX IF NOT EXISTS uidx_readings_city_recorded
    ON silver.weather_readings (city, recorded_at);

CREATE INDEX IF NOT EXISTS idx_readings_recorded_at
    ON silver.weather_readings (recorded_at DESC);


CREATE INDEX IF NOT EXISTS idx_readings_city_recorded
    ON silver.weather_readings (city, recorded_at DESC);

-- Partial index: flag anomalous temperatures for fast anomaly queries
CREATE INDEX IF NOT EXISTS idx_readings_high_temp
    ON silver.weather_readings (city, recorded_at)
    WHERE temp_celsius > 35;

GRANT INSERT, UPDATE, SELECT ON silver.weather_readings        TO weather_writer;
GRANT USAGE, SELECT          ON silver.weather_readings_id_seq TO weather_writer;

COMMENT ON TABLE silver.weather_readings IS
    'Parsed hourly weather readings. One row per city per hour. '
    'Unique on (city, recorded_at). Written by Stage 1, read by Stage 4.';

COMMENT ON COLUMN silver.weather_readings.weather_code IS
    'WMO Weather Interpretation Code. See: https://open-meteo.com/en/docs#weathervariables';

CREATE TABLE IF NOT EXISTS gold.daily_summary (
    city            VARCHAR(100)    NOT NULL,
    summary_date    DATE            NOT NULL,
    avg_temp        NUMERIC(5, 2),
    min_temp        NUMERIC(5, 2),
    max_temp        NUMERIC(5, 2),
    avg_humidity    NUMERIC(5, 2),
    avg_pressure    NUMERIC(7, 2),
    total_precip    NUMERIC(6, 3),
    total_rain      NUMERIC(6, 3),
    avg_wind_speed  NUMERIC(5, 2),
    avg_wind_gusts  NUMERIC(5, 2),
    dominant_code   SMALLINT,              
    created_at      TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    PRIMARY KEY (city, summary_date)
);

CREATE INDEX IF NOT EXISTS idx_daily_date
    ON metrics.daily_summary (summary_date DESC);

CREATE INDEX IF NOT EXISTS idx_daily_city_date
    ON metrics.daily_summary (city, summary_date DESC);

CREATE TABLE IF NOT EXISTS metrics.monthly_summary (
    city            VARCHAR(100)    NOT NULL,
    year            SMALLINT        NOT NULL,
    month           SMALLINT        NOT NULL CHECK (month BETWEEN 1 AND 12),
    avg_temp        NUMERIC(5, 2),
    min_temp        NUMERIC(5, 2),
    max_temp        NUMERIC(5, 2),
    total_precip    NUMERIC(7, 3),
    total_rain      NUMERIC(7, 3),
    rainy_days      SMALLINT,               -- days with rain > 0
    avg_wind_speed  NUMERIC(5, 2),
    created_at      TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    PRIMARY KEY (city, year, month)
);

CREATE INDEX IF NOT EXISTS idx_monthly_city
    ON metrics.monthly_summary (city, year DESC, month DESC);

CREATE TABLE IF NOT EXISTS metrics.temperature_anomalies (
    id              BIGSERIAL       PRIMARY KEY,
    city            VARCHAR(100)    NOT NULL,
    recorded_at     TIMESTAMPTZ     NOT NULL,
    temp_celsius    NUMERIC(5, 2),
    baseline_avg    NUMERIC(5, 2),          -- 30-day rolling mean for this city
    deviation       NUMERIC(5, 2),          -- temp_celsius - baseline_avg
    is_anomaly      BOOLEAN         NOT NULL DEFAULT FALSE,
    detected_at     TIMESTAMPTZ     NOT NULL DEFAULT NOW()
);

CREATE UNIQUE INDEX IF NOT EXISTS uidx_anomalies_city_recorded
    ON metrics.temperature_anomalies (city, recorded_at);

CREATE INDEX IF NOT EXISTS idx_anomalies_city_detected
    ON metrics.temperature_anomalies (city, detected_at DESC)
    WHERE is_anomaly = TRUE;

CREATE OR REPLACE VIEW metrics.rainfall_trends AS
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
FROM metrics.daily_summary
ORDER BY city, summary_date DESC;

-- Grafana read access on the whole metrics schema
GRANT SELECT ON ALL TABLES IN SCHEMA metrics TO grafana_reader;
GRANT SELECT ON metrics.rainfall_trends       TO grafana_reader;

-- Weather_writer can insert/update metric tables (Stage 4 writes here)
GRANT INSERT, UPDATE, SELECT ON metrics.daily_summary           TO weather_writer;
GRANT INSERT, UPDATE, SELECT ON metrics.monthly_summary         TO weather_writer;
GRANT INSERT, UPDATE, SELECT ON metrics.temperature_anomalies   TO weather_writer;
GRANT USAGE, SELECT ON metrics.temperature_anomalies_id_seq     TO weather_writer;

COMMENT ON TABLE metrics.daily_summary IS
    'Pre-computed daily weather aggregates per city. Written by Stage 4.';
COMMENT ON TABLE metrics.monthly_summary IS
    'Pre-computed monthly weather aggregates per city. Written by Stage 4.';
COMMENT ON TABLE metrics.temperature_anomalies IS
    'Hourly readings flagged as temperature anomalies (> 2σ deviation). Written by Stage 4.';
COMMENT ON VIEW  metrics.rainfall_trends IS
    'Rolling 7-day and 30-day rainfall averages per city. Computed from daily_summary.';
