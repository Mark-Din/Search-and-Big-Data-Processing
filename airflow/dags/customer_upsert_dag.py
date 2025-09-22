from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime, timedelta
import os

with DAG(
    dag_id="customer_upsert_dag",
    start_date=datetime(2025, 9, 22),
    schedule= None, #"@daily",
    catchup=False,
) as dag:

    jar_dir = "/usr/local/airflow/include/dependencies"
    jars = ",".join([os.path.join(jar_dir, f) for f in os.listdir(jar_dir) if f.endswith(".jar")])

    silver_task = SparkSubmitOperator(
        task_id="silver_clean_task",
        application="/usr/local/airflow/include/spark_code/silver_clean.py",
        conn_id="spark_default",
        jars=jars,
    )
    
    gold_task = SparkSubmitOperator(
        task_id="gold_features_task",
        application="/usr/local/airflow/include/spark_code/build_features_gold.py",
        conn_id="spark_default",
        jars=jars
    )

    clustering_task = SparkSubmitOperator(
        task_id="data_clustering_task",
        application="/usr/local/airflow/include/spark_code/data_clustering.py",
        conn_id="spark_default",
        jars=jars
    )

    silver_task >> gold_task >> clustering_task