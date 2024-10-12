from datetime import datetime
from io import BytesIO

import psycopg2 as pg
import boto3 as s3
from botocore.client import Config

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook


def s3_conn_test():
    file = BytesIO()
    file.write(b'test')
    file.seek(0)

    connection = BaseHook.get_connection('conn_s3')
    print(connection)
    s3_client = s3.client(
        's3',
        endpoint_url=connection.host,
        aws_access_key_id=connection.login,
        aws_secret_access_key=connection.password,
        config=Config(signature_version="s3v4"),
    )

    s3_client.put_object(
        Body=file,
        Bucket='default-storage',
        Key='test.txt'
    )


def pg_conn_test():
    connection = BaseHook.get_connection('conn_pg')

    print(connection)

    with pg.connect(
        dbname='etl',
        sslmode='disable',
        user=connection.login,
        password=connection.password,
        host=connection.host,
        port=connection.port,
        connect_timeout=600,
        keepalives_idle=600,
        tcp_user_timeout=600
    ) as conn:
        cursor = conn.cursor()
        cursor.execute("INSERT INTO test_table VALUES ('test', 1)")
        conn.commit()


with DAG(
    dag_id="test_dag",
    schedule='@once',
    start_date=datetime(2024, 8, 1),
    max_active_runs=1,
    max_active_tasks=1
) as dag:
    dag_start = EmptyOperator(task_id='dag_start')
    dag_end = EmptyOperator(task_id='dag_end')

    pip_list = BashOperator(
        task_id='pip_list',
        bash_command='pip list',
    )

    pg_conn_test = PythonOperator(
        task_id='pg_conn_test',
        python_callable=pg_conn_test,
    )

    s3_conn_test = PythonOperator(
        task_id='s3_conn_test',
        python_callable=s3_conn_test,
    )

    dag_start >> pip_list >> pg_conn_test >> s3_conn_test >> dag_end
