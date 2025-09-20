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

    spark_etl = SparkSubmitOperator(
        task_id="silver_clean_task",
        application="/usr/local/airflow/include/spark_jobs/silver_clean.py",
        conn_id="spark_default",
        deploy_mode="client",  # Python on Standalone must use client mode
        conf={
            "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
            "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            "spark.cores.max": "2",
            "spark.executor.cores": "1",
            "spark.executor.memory": "1g",
            # Driver+executors will load these:
        }
    )
