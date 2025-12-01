import sys

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.email import send_email

sys.path.append('/opt/airflow')
from include.code.mysql_log import store_metadata

# -------------------------------------------------------------------
# DAG configuration
# -------------------------------------------------------------------
default_args = {
    "owner": "mark",
    "depends_on_past": False,
    "email":["m7812252009@gmail.com"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "start_date":datetime(2025, 10, 24),
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    "arxiv_orchestrator_dag",
    default_args=default_args,
    description="End-to-end pipeline orchestrator for arXiv ETL + clustering",
    schedule="@daily",  # or "@once" for manual testing
    catchup=False,
    max_active_runs=1,
    tags=["arxiv", "pipeline", "controller"],
)

# -------------------------------------------------------------------
# Define task functions
# -------------------------------------------------------------------

def validate_output(**context):
    """
    Basic quality check:
    - Ensure output folder not empty
    - Could later check MinIO file count or row count in Delta
    """
    import boto3

    s3 = boto3.client(
        "s3",
        endpoint_url="http://minio:9000",
        aws_access_key_id="minioadmin",
        aws_secret_access_key="minioadmin",
    )

    bucket = "deltabucket"
    prefix = "gold/arxiv_features/"
    objects = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)

    count = objects.get("KeyCount", 0)
    if count == 0:
        raise ValueError("❌ Gold layer output is empty in MinIO.")
    else:
        print(f"✅ Quality check passed: {count} files found under {prefix}.")


def write_final_status(**context):
    """
    Write final orchestration status into MySQL metadata table.
    """
    run_id = f"dag_{context['dag_run'].run_id}"
    status = "S" if context["ti"].state == "success" else "F"

    store_metadata(
        run_id=run_id,
        stage_name="orchestrator_dag",
        component="airflow",
        record_count=0,
        duration=0,
        status=status,
        note=f"Airflow orchestrator DAG finished with status {status}",
    )
    print(f"✅ Logged orchestrator DAG status to MySQL ({status})")


# -------------------------------------------------------------------
# Task definitions
# -------------------------------------------------------------------

ingest_task = TriggerDagRunOperator(
    task_id="ingest_task",
    trigger_dag_id="python_dag",  # existing DAG for ingestion
    wait_for_completion=True,
    poke_interval=30,
    retries=2,
    dag=dag,
)

process_task = TriggerDagRunOperator(
    task_id="process_task",
    trigger_dag_id="spark_dag",  # existing DAG for Spark jobs
    wait_for_completion=True,
    poke_interval=30,
    retries=2,
    dag=dag,
)

quality_check_task = PythonOperator(
    task_id="quality_check_task",
    python_callable=validate_output,
    retries=2,
    dag=dag,
)

notify_task = PythonOperator(
    task_id="notify_task",
    python_callable=write_final_status,
    dag=dag,
)

# -------------------------------------------------------------------
# Dependencies
# -------------------------------------------------------------------
ingest_task >> process_task >> quality_check_task >> notify_task
