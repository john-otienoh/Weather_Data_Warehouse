
#!/usr/bin/env bash
set -euo pipefail

echo "Setting up Airflow connections and variables..."

SCHEDULER="docker compose exec airflow-scheduler"

# Add the weather database connection
$SCHEDULER airflow connections add weather_postgres \
    --conn-type    postgres \
    --conn-host    postgres \
    --conn-port    5432 \
    --conn-schema  weather_db \
    --conn-login   "${POSTGRES_USER}" \
    --conn-password "${POSTGRES_PASSWORD}"

# Set variables that DAGs will read at runtime
$SCHEDULER airflow variables set ALERT_EMAIL        "${ALERT_EMAIL}"
$SCHEDULER airflow variables set WEATHER_CITIES     "Nairobi,Mombasa,Kisumu,Nakuru,Eldoret"
$SCHEDULER airflow variables set MIN_ROWS_PER_CITY  "20"

echo "Done! Open http://localhost:8080 (admin / your AIRFLOW_ADMIN_PASSWORD)"
