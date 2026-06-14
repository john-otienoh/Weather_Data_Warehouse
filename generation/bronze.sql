CREATE SCHEMA IF NOT EXISTS bronze;

CREATE TABLE IF NOT EXISTS bronze.weather_readings (
    city                VARCHAR(100)  NOT NULL,
    recorded_at         TIMESTAMPTZ   NOT NULL,
    fetched_at          TIMESTAMPTZ   NOT NULL,
    temp_celsius        NUMERIC(5,2),
    apparent_temp       NUMERIC(5,2),
    humidity_pct        INTEGER,
    precipitation_mm    NUMERIC(6,3),
    rain_mm             NUMERIC(6,3),
    weather_code        INTEGER,
    pressure_hpa        NUMERIC(7,2),
    cloud_cover_pct     INTEGER,
    wind_speed_mps      NUMERIC(5,2),
    wind_direction_deg  INTEGER,
    wind_gusts_mps      NUMERIC(5,2),
    visibility            INTEGER
);
CREATE UNIQUE INDEX ON bronze.weather_readings (city, recorded_at);
