INSERT INTO gold.daily_summary(
    city, 
    summary_date,
    avg_temp, 
    min_temp, 
    max_temp,
    avg_humidity, 
    avg_pressure,
    total_rain,
    avg_wind_speed, 
    avg_wind_gusts,
    avg_visibility, 
    dominant_code,
    updated_at
)
SELECT 
    city,
    DATE(recorded_at AT TIME ZONE 'Africa/Nairobi') AS summary_date,
    ROUND(AVG(temp_celsius)::NUMERIC, 2) AS avg_temp,
    ROUND(MIN(temp_celsius)::NUMERIC, 2) AS min_temp,
    ROUND(MAX(temp_celsius)::NUMERIC, 2) AS max_temp,
    ROUND(AVG(humidity_pct)::NUMERIC, 2) AS avg_humidity,
    ROUND(AVG(pressure_hpa)::NUMERIC, 2) AS avg_pressure,
    ROUND(SUM(rain_mm)::NUMERIC, 3) AS total_rain,
    ROUND(AVG(wind_speed_mps)::NUMERIC, 2) AS avg_wind_speed,
    ROUND(AVG(wind_gusts_mps)::NUMERIC, 2) AS avg_wind_gusts,
    ROUND(AVG(visibility_m)::NUMERIC, 1) AS avg_visibility,
    MODE() WITHIN GROUP (ORDER BY weather_code) AS dominant_code,
    NOW()

FROM silver.weather_readings
WHERE DATE(recorded_at AT TIME ZONE 'Africa/Nairobi')
      = COALESCE(CAST(:target_date AS DATE), CURRENT_DATE - 1)
  AND temp_celsius IS NOT NULL
GROUP BY city, summary_date

ON CONFLICT (city, summary_date) DO UPDATE SET
    avg_temp = EXCLUDED.avg_temp,
    min_temp = EXCLUDED.min_temp,
    max_temp = EXCLUDED.max_temp,
    avg_humidity = EXCLUDED.avg_humidity,
    avg_pressure = EXCLUDED.avg_pressure,
    total_rain = EXCLUDED.total_rain,
    avg_wind_speed = EXCLUDED.avg_wind_speed,
    avg_wind_gusts = EXCLUDED.avg_wind_gusts,
    avg_visibility = EXCLUDED.avg_visibility,
    dominant_code  = EXCLUDED.dominant_code,
    updated_at = NOW();
