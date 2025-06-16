import pandas as pd
from dotenv import load_dotenv
import os
from sqlalchemy import create_engine
import psycopg2
import sys
from datetime import timedelta
from airflow.decorators import dag, task
from airflow.operators.python import PythonOperator
from datetime import datetime

POSTGRES_USER = os.getenv('POSTGRES_USER')
POSTGRES_PASSWORD = os.getenv('POSTGRES_PASSWORD')
POSTGRES_DB = os.getenv('POSTGRES_DB')
POSTGRES_HOST = os.getenv('POSTGRES_HOST')
POSTGRES_PORT = os.getenv('POSTGRES_PORT')

# Подключение к базе данных PostgreSQL
def open_conn():
    '''Открытие подключения к БД в случае его разрыва'''
    return psycopg2.connect(
        dbname=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
        host=POSTGRES_HOST,
        port=POSTGRES_PORT
    )

from spark_app.load_orders import load_orders_from_csv      


# Создание подключения
conn = open_conn()

default_args={
    "owner": "rvegrc",
    "depends_on_past": True,                
    'email_on_failure': False,
    'email_on_retry': False,
    "retries": 0
}

@dag(
    tags=["test", "edu", "postgres"],
    schedule="@daily"
    ,start_date=datetime(2025, 6, 12)
    ,catchup=False
)

def etl_orders_pipeline():
    load_orders_task = PythonOperator(
        task_id='load_orders_from_csv',
        python_callable=load_orders_from_csv,
        op_kwargs={
            'path_to_csv': '/opt/airflow/airflow_data/orders.csv',
            'cur': conn.cursor()
        }
    )

    load_orders_task
    load_orders_task.doc_md = __doc__

etl_orders_pipeline()