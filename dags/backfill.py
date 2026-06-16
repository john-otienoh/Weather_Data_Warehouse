# ==============================================================================
# Loads 31 days of historical data. Trigger manually when needed.

import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable

log = logging.getLogger(__name__)

default_args = {
    "owner":           "data-team",
    "retries":         2,
    "retry_delay":     timedelta(minutes=10),
    "email_on_failure": True,
    "email": [Variable.get("ALERT_EMAIL", default_var="admin@example.com")],
}

with DAG(
    dag_id="backfill_weather",
    default_args=default_args,
    description="Loads 31 days of weather history. Trigger manually.",
    schedule=None,             
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["weather", "backfill", "manual"],
) as dag:
    def task_check(**context):
        from db.connection import check_connection
        if not check_connection():
            raise ConnectionError("Database not reachable")
        return "DB OK"
    check_db = PythonOperator(
        task_id="check_database",    
        python_callable=task_check_db
    )

    def task_backfill(**context):
        from generation.weather_ingest import main as ingest
        log.info("Starting 31-day backfill...")
        ingest(mode="backfill", dry_run=False)
        return "Backfill complete"
    
    backfill  = PythonOperator(
        task_id="run_backfill",
        python_callable=task_backfill,
        execution_timeout=timedelta(minutes=25)
    )

    def task_validate_backfill(**context):
        from sqlalchemy import text
        from db.connection import get_engine
        
        engine = get_engine()
        with engine.connect() as conn:
            result = conn.execute(text("""
                SELECT city, COUNT(*) AS rows
                FROM silver.weather_readings
                WHERE recorded_at >= NOW() - INTERVAL '32 days'
                GROUP BY city ORDER BY city
            """))
            rows = {r[0]: r[1] for r in result.fetchall()}
        total = sum(rows.values())
        log.info("Backfill complete: %d total rows | %s", total, rows)
        context["ti"].xcom_push(key="backfill_counts", value=rows)
        if total < 3000:
            log.warning("Fewer rows than expected (%d). Check API response.", total)
        return f"Backfill: {total} rows across {len(rows)} cities"

    validate  = PythonOperator(
        task_id="validate_backfill", python_callable=task_validate_backfill
    )

    check_db >> backfill >> validate
