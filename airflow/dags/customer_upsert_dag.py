from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime, timedelta

with DAG(
    dag_id="silver_clean_dag",
    start_date=datetime(2025, 1, 1),
    # schedule_interval=None,
    catchup=False,

) as dag:

    spark_etl = SparkSubmitOperator(
        task_id="silver_clean_task",
        application="/usr/local/airflow/include/spark_jobs/silver_clean.py",
        conn_id="spark_default",
        retries=3,
        retry_delay=timedelta(minutes=2),
    )