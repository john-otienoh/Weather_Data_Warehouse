INSERT INTO gold.monthly_summary (
    city, 
    year, 
    month,
    avg_temp, 
    min_temp, 
    max_temp,
    total_rain, 
    rainy_days,
    avg_wind_speed, 
    updated_at
)
SELECT
    city,
    EXTRACT(YEAR  FROM summary_date)::INT AS year,
    EXTRACT(MONTH FROM summary_date)::INT AS month,
    ROUND(AVG(avg_temp)::NUMERIC, 2) AS avg_temp,
    ROUND(MIN(min_temp)::NUMERIC, 2) AS min_temp,   
    ROUND(MAX(max_temp)::NUMERIC, 2) AS max_temp,   
    ROUND(SUM(total_rain)::NUMERIC, 3) AS total_rain,
    SUM(CASE WHEN total_rain > 0.1 THEN 1 ELSE 0 END)  AS rainy_days,
    ROUND(AVG(avg_wind_speed)::NUMERIC, 2) AS avg_wind_speed,
    NOW()
FROM gold.daily_summary
WHERE EXTRACT(YEAR FROM summary_date) = COALESCE(:target_year::INT, EXTRACT(YEAR FROM CURRENT_DATE - INTERVAL '1 month'))
  AND EXTRACT(MONTH FROM summary_date) = COALESCE(:target_month::INT, EXTRACT(MONTH FROM CURRENT_DATE - INTERVAL '1 month'))
GROUP BY city, year, month

ON CONFLICT (city, year, month) DO UPDATE SET
    avg_temp = EXCLUDED.avg_temp,
    min_temp = EXCLUDED.min_temp,
    max_temp = EXCLUDED.max_temp,
    total_rain = EXCLUDED.total_rain,
    rainy_days = EXCLUDED.rainy_days,
    avg_wind_speed = EXCLUDED.avg_wind_speed,
    updated_at = NOW();
