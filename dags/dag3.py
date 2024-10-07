from airflow import DAG
from airflow.operators.empty import EmptyOperator
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 10, 7),
}

with DAG('simple_dag', default_args=default_args, schedule_interval='@daily') as dag:
    start = EmptyOperator(task_id='start')