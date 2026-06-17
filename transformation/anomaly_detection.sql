WITH baseline AS (
    -- Compute average and spread for each city over the last 30 days
    -- (still kept for reference/logging — deviation and baseline_avg
    -- columns are informational only and no longer drive is_anomaly)
    SELECT
        city,
        AVG(temp_celsius)                               AS baseline_avg,
        CASE WHEN COUNT(*) < 48 THEN 0.8
             ELSE STDDEV(temp_celsius)
        END                                             AS effective_std
    FROM silver.weather_readings
    WHERE recorded_at >= NOW() - INTERVAL '30 days'
      AND temp_celsius IS NOT NULL
    GROUP BY city
)
INSERT INTO gold.temperature_anomalies (
    city, recorded_at, temp_celsius,
    baseline_avg, deviation, is_anomaly, detected_at
)

SELECT
    r.city,
    r.recorded_at,
    r.temp_celsius,
    ROUND(b.baseline_avg::NUMERIC,                    2) AS baseline_avg,
    ROUND((r.temp_celsius - b.baseline_avg)::NUMERIC, 2) AS deviation,
    -- CHANGED: is_anomaly is now a fixed comfort-range check instead of
    -- a statistical (2 x stddev) check.
    -- Flag as anomaly when temperature falls outside 10°C–35°C.
    (r.temp_celsius < 10 OR r.temp_celsius > 35)          AS is_anomaly,
    NOW()

FROM silver.weather_readings r
JOIN baseline b ON r.city = b.city
WHERE r.recorded_at >= NOW() - INTERVAL '25 hours'

ON CONFLICT (city, recorded_at) DO UPDATE SET
    temp_celsius  = EXCLUDED.temp_celsius,
    baseline_avg  = EXCLUDED.baseline_avg,
    deviation     = EXCLUDED.deviation,
    is_anomaly    = EXCLUDED.is_anomaly,
    detected_at   = EXCLUDED.detected_at;