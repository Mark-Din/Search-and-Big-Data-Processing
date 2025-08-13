from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
import sys
import os

# Add your script paths
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), 'code')))

from main import uuid_generator
from query_insert import get_data, upsert_data, query

def extract_data(**kwargs):
    df = get_data()
    kwargs['ti'].xcom_push(key='user_df', value=df.to_json())

def load_customer_attributes(**kwargs):
    import pandas as pd
    user_df = pd.read_json(kwargs['ti'].xcom_pull(task_ids='extract_data', key='user_df'))

    attributes = []
    for attribute in user_df.columns:
        if attribute == 'id':
            continue
        attributes.append({
            "id": uuid_generator(attribute),
            "name": attribute,
            "value_type": "text",
            "is_required": False,
            "is_ui": True,
            "is_editable": True,
            'default_value_text': None,
            'default_value_integer': None,
            'default_value_float': None,
            'default_value_boolean': None,
            'default_value_date': None,
            'default_value_varchar_list': None
        })
    upsert_data("customer_attributes", attributes, ["name"])
    kwargs['ti'].xcom_push(key='attributes', value=attributes)

def load_customers(**kwargs):
    import pandas as pd
    json_data = kwargs['ti'].xcom_pull(task_ids='extract_data', key='user_df')
    user_df = pd.read_json(json_data)
    id_list = [{'id': uid} for uid in user_df.id]
    upsert_data("customers", id_list, ['id'])

def load_customer_values(**kwargs):
    import pandas as pd
    json_data = kwargs['ti'].xcom_pull(task_ids='extract_data', key='user_df')
    user_df = pd.read_json(json_data)
    col_to_id = {row[0]: row[1] for row in query(user_df)}
    values = []
    for _, row in user_df.iterrows():
        for index, value in enumerate(row):
            if row.keys()[index] in col_to_id:
                if value == row.id:
                    continue
                customer_id = row.id
                attribute_id = col_to_id[row.keys()[index]]
                values.append({
                    'id': uuid_generator(str(customer_id) + str(attribute_id)),
                    'customer_id': str(customer_id),
                    'attribute_id': str(attribute_id),
                    'value_text': value
                })
    upsert_data("customer_values", values, ['id'])

def load_user_attribute_categories(**kwargs):
    name = 'Nexva 屬性'
    attribute_category_id = uuid_generator(name)
    upsert_data("user_attribute_categories", [{
        'id': attribute_category_id,
        'name': name,
        'user_id': '31f4f622-54f1-493f-821a-7bf03790fdbd',
        'priority': 1
    }], ['id'])
    kwargs['ti'].xcom_push(key='attribute_category_id', value=attribute_category_id)

def load_user_attributes(**kwargs):
    attributes = kwargs['ti'].xcom_pull(task_ids = 'load_customer_attributes', key='attributes')
    attribute_category_id = kwargs['ti'].xcom_pull(task_ids = 'load_user_attribute_categories', key='attribute_category_id')
    user_attributes = []
    priority = 0
    for attribute in attributes:
        user_attributes.append({
            'id': uuid_generator(str(attribute['id']) + str(attribute_category_id)),
            'user_id': '31f4f622-54f1-493f-821a-7bf03790fdbd',
            'attribute_id': str(attribute['id']),
            'attribute_category_id': str(attribute_category_id),
            'priority': priority
        })
        priority += 1
    upsert_data("user_attributes", user_attributes, ['id'])

# DAG definition
with DAG(
    dag_id='customer_upsert_dag',
    start_date=datetime(2025, 7, 30),
    schedule=None,  # Run manually or define CRON
    catchup=False,
    default_args={'owner': 'airflow'}
) as dag:

    t1 = PythonOperator(task_id='extract_data', python_callable=extract_data)
    t2 = PythonOperator(task_id='load_customer_attributes', python_callable=load_customer_attributes)
    t3 = PythonOperator(task_id='load_customers', python_callable=load_customers)
    t4 = PythonOperator(task_id='load_customer_values', python_callable=load_customer_values)
    t5 = PythonOperator(task_id='load_user_attribute_categories', python_callable=load_user_attribute_categories)
    t6 = PythonOperator(task_id='load_user_attributes', python_callable=load_user_attributes)

    t1 >> t2 >> [t3, t4] >> t5 >> t6
