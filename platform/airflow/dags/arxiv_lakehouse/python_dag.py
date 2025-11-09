from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from include.arxiv_lakehouse.python_code.etl_data_to_mysql_OAI import main as etl_data_to_mysql_oai_main
from include.arxiv_lakehouse.python_code.etl_data_to_mysql_api import main as etl_data_to_mysql_api_main
from include.arxiv_lakehouse.python_code.etl_coauthorship_edges import main as etl_coauthorship_edges_main

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
    dag_id = "arxiv_python_dag",
    start_date=datetime(2025,9,22),
    schedule="@daily",
    catchup=False,
    default_args=default_args
) as dag:
    
    # oai = PythonOperator(
    #     task_id="etl_data_to_mysql_oai",
    #     python_callable=etl_data_to_mysql_oai_main
    #     )    

    # api = PythonOperator(
    #     task_id="etl_data_to_mysql_api",
    #     python_callable=etl_data_to_mysql_api_main
    #     )
    
    analysis = PythonOperator(
        task_id ="etl_coauthorship_edges",
        python_callable=etl_coauthorship_edges_main
    )
    
    # oai >> api >> 
    analysis