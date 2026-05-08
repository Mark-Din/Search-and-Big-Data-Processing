import os
import sys
from datetime import datetime, timedelta

import pendulum

from airflow import DAG

try:
    from airflow.providers.standard.operators.python import PythonOperator
except ImportError:
    from airflow.operators.python import PythonOperator


DAG_ID = "nexva_ingestion_etl_dag"

AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow")
INCLUDE_ROOT = os.path.join(AIRFLOW_HOME, "include", "code")
CODE_ROOT = os.path.join(INCLUDE_ROOT, "code")

for path in (INCLUDE_ROOT, CODE_ROOT):
    if path not in sys.path:
        sys.path.insert(0, path)


def run_nexva_ingestion_etl():
    from include.code.nexva_ingestion import main as nexva_ingestion_main

    nexva_ingestion_main.main()


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
    tags=["qidu", "nexva", "ingestion"],
) as dag:
    run_etl = PythonOperator(
        task_id="run_nexva_ingestion_etl",
        python_callable=run_nexva_ingestion_etl,
    )
