import pandas as pd
import os
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

from spark_app.orders_agg import load_orders_from_csv, load_items_from_csv, customer_summary_to_db     


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
    path_to_csv = '/opt/airflow/airflow_data/de_market/data'
    
    load_orders_task = PythonOperator(
        task_id='load_orders_from_csv',
        python_callable=load_orders_from_csv,
        op_kwargs={
            'path_to_csv': f'{path_to_csv}/orders_new.csv',
            'cur': conn.cursor()
        }
    )

    load_order_items_task = PythonOperator(
        task_id='load_order_items_from_csv',
        python_callable=load_items_from_csv,
        op_kwargs={
            'path_to_csv': f'{path_to_csv}/order_items_new.csv',
            'cur': conn.cursor()
        }
    )

    customer_summary_to_db_task = PythonOperator(
        task_id='customer_summary_to_db_task',
        python_callable=customer_summary_to_db,
        op_kwargs={
            'cur': conn.cursor()            
        }        
    )


    load_orders_task >> load_order_items_task >> customer_summary_to_db_task

    load_orders_task.doc_md = __doc__
    load_order_items_task.doc_md = __doc__
    customer_summary_to_db_task.doc_md = __doc__

etl_orders_pipeline()