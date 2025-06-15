import pandas as pd
from dotenv import load_dotenv
import os
from sqlalchemy import create_engine
import sys
from datetime import timedelta
from airflow.decorators import dag, task
from airflow.operators.python import PythonOperator
from datetime import datetime
import subprocess


POSTGRES_USER = os.getenv('POSTGRES_USER')
POSTGRES_PASSWORD = os.getenv('POSTGRES_PASSWORD')
POSTGRES_DB = os.getenv('POSTGRES_DB')
POSTGRES_HOST = os.getenv('POSTGRES_HOST')
POSTGRES_PORT = os.getenv('POSTGRES_PORT')

# Подключение к базе данных PostgreSQL
engine = create_engine(f'postgresql+psycopg2://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}')

path_to_data = '/opt/airflow/airflow_data/de_market/data'


default_args={
    "owner": "rvegrc",
    "depends_on_past": True,                
    'email_on_failure': False,
    'email_on_retry': False,
    "retries": 0
}

# from spark_app.orders_agg import load_orders_from_csv

# def list_files():
#     result = subprocess.run(['ls', '-l', path_to_data], capture_output=True, text=True)
#     print(result.stdout)

@dag(
    tags=["agg", "edu", "postgres", 'shop'],
    schedule="@daily"
    ,start_date=datetime(2025, 6, 6)
    ,catchup=False
)


def etl_orders_pipeline():
    def load_orders_from_csv() -> None:
        """
        Загружает данные заказов из CSV файла в таблицу orders в БД PostgreSQL.
        Параметры:
        - path_to_csv: Путь к CSV файлу с данными заказов.
        - engine: объект SQLAlchemy для подключения к базе данных
        """

        # Считываем данные из CSV
        orders_new = pd.read_csv(f'{path_to_data}/orders_new.csv')

        # Считываем данные из таблиц customers и orders
        orders_ids = pd.read_sql_query('SELECT order_id from orders', con=engine.connect())
        customers_ids = pd.read_sql_query('SELECT customer_id from customers', con=engine.connect())

        print(orders_ids)

    load_orders_task = PythonOperator(
        task_id='load_orders',
        python_callable=load_orders_from_csv
    )

    load_orders_task


    # load_order_items_task = PythonOperator(...)
    # build_summary_task = PythonOperator(...)
   
    # load_orders_task >> load_order_items_task >> build_summary_task

etl_orders_pipeline()