from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime, timedelta
import os

with DAG(
    dag_id="customer_upsert_dag",
    start_date=datetime(2025, 1, 1),
    # schedule_interval=None,
    catchup=False,
) as dag:

    jar_dir = "/usr/local/airflow/include/dependencies"
    jars = ",".join([os.path.join(jar_dir, f) for f in os.listdir(jar_dir) if f.endswith(".jar")])

    spark_task = SparkSubmitOperator(
        task_id="silver_clean_task",
        application="/usr/local/airflow/include/spark_jobs/silver_clean.py",
        conn_id="spark_default",
        jars=jars
    )
