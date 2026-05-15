import os
import sys
import pendulum
from datetime import datetime, timedelta

from airflow import DAG

sys.path.append("/opt/airflow/include/code")
from analytics.main import main as analytics_main

try:
    from airflow.providers.standard.operators.python import PythonOperator
except ImportError:
    from airflow.operators.python import PythonOperator


DAG_ID = "analytics_quickreport_etl_dag"


default_args = {
    "owner": "qidu",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


with DAG(
    dag_id=DAG_ID,
    description="Build quickreport analytics tables from storage_search files and log data.",
    default_args=default_args,
    start_date=datetime(2026, 5, 7, tzinfo=pendulum.timezone("Asia/Taipei")),
    schedule="@hourly",
    catchup=False,
    tags=["qidu", "analytics"],
) as dag:
    run_etl = PythonOperator(
        task_id="run_analytics_etl",
        python_callable=analytics_main,
    )
