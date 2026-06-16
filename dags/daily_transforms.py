import logging
from datetime import datetime, timedelta, date

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator, ShortCircuitOperator

log = logging.getLogger(__name__)

default_args = {
    "owner":           "data-team",
    "retries":         2,
    "retry_delay":     timedelta(minutes=10),
    "email_on_failure": True,
    "email": [Variable.get("ALERT_EMAIL", default_var="admin@example.com")],
}

with DAG(
    dag_id="daily_weather_transforms",
    default_args=default_args,
    description="Computes daily/monthly summaries and anomaly detection at 01:00 EAT",
    schedule="0 22 * * *",    
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["weather", "transforms", "gold"],
) as dag:
    def task_check_freshness(**context):
        from sqlalchemy import text
        from db.connection import get_engine
        yesterday = date.today() - timedelta(days=1)
        with get_engine().connect() as conn:
            result = conn.execute(text("""
                SELECT city, COUNT(*) FROM silver.weather_readings
                WHERE DATE(recorded_at AT TIME ZONE 'Africa/Nairobi') = :d
                GROUP BY city
            """), {"d": str(yesterday)})
            rows = result.fetchall()
        if len(rows) < 5:
            raise ValueError(f"Only {len(rows)}/5 cities in silver for {yesterday}")
        log.info("Silver data fresh for %s: %d cities", yesterday, len(rows))
        return "Silver fresh"
    check_freshness  = PythonOperator(
        task_id="check_silver_freshness", python_callable=task_check_freshness
    )
    
    def task_daily_summary(**context):
        from transformation.transform_runner import run_daily_summary
        result = run_daily_summary()
        context["ti"].xcom_push(key="daily_result", value=result)
        return str(result)

    daily_summary = PythonOperator(
        task_id="run_daily_summary", python_callable=task_daily_summary
    )
    
    def task_anomalies(**context):
        from transformation.transform_runner import run_anomaly_detection
        result = run_anomaly_detection()
        context["ti"].xcom_push(key="anomaly_result", value=result)
        if result.get("anomalies", 0) > 0:
            log.warning("%d temperature anomalies detected!", result["anomalies"])
        return str(result)
    
    anomalies = PythonOperator(
        task_id="run_anomaly_detection", python_callable=task_anomalies
    )
    
    def is_first_of_month(**context):
        return date.today().day == 1
    
    monthly_gate = ShortCircuitOperator(
        task_id="monthly_gate",      python_callable=is_first_of_month
    )

    
    def task_monthly_summary(**context):
        from transformation.transform_runner import run_monthly_summary
        result = run_monthly_summary()
        return str(result)
    
    monthly_summary = PythonOperator(
        task_id="run_monthly_summary", python_callable=task_monthly_summary
    )
    
    
    def task_validate_gold(**context):
        from transformation.transform_runner import validate_gold
        result = validate_gold()
        return str(result)

    validate_gold_t = PythonOperator(
        task_id="validate_gold_layer", python_callable=task_validate_gold
    )

    check_freshness >> daily_summary >> anomalies >> monthly_gate >> monthly_summary >> validate_gold_t
