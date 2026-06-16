
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable

with DAG(
    dag_id="weekly_weather_digest",
    default_args={"owner": "data-team", "retries": 2, "retry_delay": timedelta(minutes=10)},
    description="Sends weekly HTML email report every Monday 07:00 EAT",
    schedule="0 4 * * 1",   
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["weather", "notifications"],
) as dag:

    def send_report(**context):
        from serving.reports import send_email_report
        recipients = Variable.get("ALERT_EMAIL", default_var="admin@example.com").split(",")
        send_email_report(recipients)
        return f"Report sent to {recipients}"

    PythonOperator(task_id="send_weekly_report", python_callable=send_report)
