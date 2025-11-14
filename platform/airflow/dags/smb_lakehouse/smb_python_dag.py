import logging
import sys

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

sys.path.append('/opt/airflow')
from include.smb_lakehouse.python_code.etl_mysql_to_es import main as etl_mysql_to_es_main
from include.smb_lakehouse.python_code.etl_mysql_to_es_cluster import main as etl_mysql_to_es_cluster_main    


# Turn down elasticsearch-py logs
logging.getLogger("elastic_transport.transport").setLevel(logging.WARNING)
logging.getLogger("elasticsearch").setLevel(logging.WARNING)

default_args = {
    'owner': 'Mark',
    'depends_on_past': False,
    'email_on_failure': True,
    'retries': 3,  # Retry up to 3 times
    'retry_delay': timedelta(minutes=5),
    "start_date": datetime(2025, 1, 30),
    "email":["m7812252009@gmail.com"]
}

with DAG(
    dag_id = "smb_es_data_dag",
    start_date=datetime(2025,9,22),
    schedule="*/15 * * * *",
    catchup=False,
    default_args = default_args
) as dag:
    
    PythonOperator(
        task_id="etl_mysql_to_es",
        python_callable=etl_mysql_to_es_main
        )
        

default_args = {
    'owner': 'Mark',
    'depends_on_past': False,
    'email_on_failure': True,
    'retries': 3,  # Retry up to 3 times
    'retry_delay': timedelta(minutes=5),
    "start_date": datetime(2025, 1, 30),
    "email":["m7812252009@gmail.com"]
}

with DAG(
    dag_id = "smb_es_cluster_dag",
    start_date=datetime(2025,9,22),
    schedule="@daily",
    catchup=False,
    default_args = default_args
) as dag:
    
    PythonOperator(
        task_id="etl_mysql_to_es_cluster",
        python_callable=etl_mysql_to_es_cluster_main
        )