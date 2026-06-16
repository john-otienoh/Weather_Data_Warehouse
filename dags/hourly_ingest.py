import logging
from datetime import datetime, timedelta
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator

log = logging.getLogger(__name__)

default_args = {
    "owner":           "data-team",
    "retries":         3,                      
    "retry_delay":     timedelta(minutes=5),    
    "email_on_failure": True,
    "email": [Variable.get("ALERT_EMAIL", default_var="admin@example.com")],
}

with DAG(
    dag_id="hourly_weather_ingest",
    default_args=default_args,
    description="Pulls Open-Meteo data every hour for 5 Kenyan cities",
    schedule="0 * * * *",    # cron: "at minute 0 of every hour"
    start_date=datetime(2025, 1, 1),
    catchup=False,            # don't re-run missed hours if Airflow restarts
    max_active_runs=1,        # only one run at a time (no overlaps)
    tags=["weather", "hourly", "bronze", "silver"],
) as dag:
    def task_check_db(**context):
        from db.connection import check_connection
        if not check_connection():
            raise ConnectionError("PostgreSQL is not reachable! Check docker compose ps.")
        log.info("Database OK")
        return "DB OK"
    check_db = PythonOperator(
        task_id="check_database",
        python_callable=task_check_db,
    )

    def task_ingest(**context):
        from generation.weather_ingest import main as ingest
        ingest(mode="live", dry_run=False)
        return "Ingest complete"

    ingest_data = PythonOperator(
        task_id="fetch_and_store",
        python_callable=task_ingest,
        execution_timeout=timedelta(minutes=10),
    )

    def task_validate(**context):
        from sqlalchemy import text
        from db.connection import get_engine

        engine = get_engine()
        with engine.connect() as conn:
            result = conn.execute(text("""
                SELECT city, COUNT(*) AS rows
                FROM silver.weather_readings
                WHERE recorded_at >= NOW() - INTERVAL '2 hours'
                GROUP BY city
                ORDER BY city
            """))
            rows = result.fetchall()

        counts = {r[0]: r[1] for r in rows}
        log.info("Row counts last 2 hours: %s", counts)
        context["ti"].xcom_push(key="row_counts", value=counts)

        if len(counts) < 5:
            raise ValueError(
                f"Only {len(counts)}/5 cities have data. "
                f"Found: {list(counts.keys())}"
            )
        return f"All 5 cities confirmed: {sum(counts.values())} rows"

    validate = PythonOperator(
        task_id="validate_data",
        python_callable=task_validate,
    )
    
    check_db >> ingest_data >> validate