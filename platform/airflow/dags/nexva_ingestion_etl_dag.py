import os
import sys
from datetime import datetime, timedelta

import pendulum

from airflow import DAG

sys.path.append("/opt/airflow/include/code")
from nexva_ingestion.main import main as nexva_ingestion_main

try:
    from airflow.providers.standard.operators.python import PythonOperator
except ImportError:
    from airflow.operators.python import PythonOperator


DAG_ID = "nexva_ingestion_etl_dag"


default_args = {
    "owner": "qidu",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


with DAG(
    dag_id=DAG_ID,
    description="Ingest Nexva files from SSH into MinIO and storage_search metadata.",
    default_args=default_args,
    start_date=datetime(2026, 5, 7, tzinfo=pendulum.timezone("Asia/Taipei")),
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    tags=["qidu", "ingestion"],
) as dag:
    run_etl = PythonOperator(
        task_id="run_nexva_ingestion_etl",
        python_callable=nexva_ingestion_main,
    )
