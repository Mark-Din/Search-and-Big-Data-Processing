from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime, timedelta
import os

default_args = {
    'owner': 'Mark',
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 2,  # Retry up to 3 times
    'retry_delay': timedelta(minutes=5),
    "start_date": datetime(2025, 1, 30),
    # "email":["m7812252009@gmail.com"]
}

with DAG(
    dag_id="arxiv_spark_dag",
    schedule= "@daily",
    catchup=False,
    default_args=default_args
) as dag:

    jar_dir = "/usr/local/airflow/include/dependencies"
    jars = ",".join([os.path.join(jar_dir, f) for f in os.listdir(jar_dir) if f.endswith(".jar")])

    main_path = '/usr/local/airflow/include/arxiv_lakehouse/spark_code'

    silver_task = SparkSubmitOperator(
        task_id="silver_clean_task",
        application=f"{main_path}/task_silver_clean.py",
        conn_id="spark_default",
        jars=jars,
    )
    
    gold_task = SparkSubmitOperator(
        task_id="gold_features_task",
        application=f"{main_path}/task_build_features_gold.py",
        conn_id="spark_default",
        jars=jars
    )

    clustering_task = SparkSubmitOperator(
        task_id="data_clustering_task",
        application=f"{main_path}/task_data_clustering.py",
        conn_id="spark_default",
        jars=jars
    )

    silver_task >> gold_task >> clustering_task