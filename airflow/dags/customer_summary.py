import pandas as pd
from dotenv import load_dotenv
import os
from sqlalchemy import create_engine
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
engine = create_engine(f'postgresql+psycopg2://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}')

default_args={
    "owner": "rvegrc",
    "depends_on_past": True,                
    'email_on_failure': False,
    'email_on_retry': False,
    "retries": 0
}

@dag(
    tags=["agg", "edu", "postgres", 'shop'],
    schedule="@daily"
    ,start_date=datetime(2025, 6, 6)
    ,catchup=False
)
def etl_orders_pipeline():
    load_orders_task = PythonOperator(...)
    load_order_items_task = PythonOperator(...)
    build_summary_task = PythonOperator(...)
    
    load_orders_task >> load_order_items_task >> build_summary_task

etl_orders_pipeline()
        # @task
    # def load_orders():
    #     # Загрузка данных из CSV в DataFrame
    #     df = pd.read_csv('/opt/airflow/airflow_data/orders.csv')
    #     # Запись данных в таблицу orders
    #     df.to_sql('orders', con=engine, if_exists='replace', index=False)
    #     print("Orders loaded successfully.")

    # @task
    # def load_order_items():
    #     # Загрузка данных из CSV в DataFrame
    #     df = pd.read_csv('/opt/airflow/airflow_data/order_items.csv')
    #     # Запись данных в таблицу order_items
    #     df.to_sql('order_items', con=engine, if_exists='replace', index=False)
    #     print("Order items loaded successfully.")

    # @task
    # def build_summary():
    #     # Выполнение SQL-запроса для создания сводной таблицы
    #     with engine.connect() as connection:
    #         result = connection.execute("""
    #             CREATE TABLE IF NOT EXISTS order_summary AS
    #             SELECT o.order_id, SUM(oi.quantity * oi.price) AS total_amount
    #             FROM orders o
    #             JOIN order_items oi ON o.order_id = oi.order_id
    #             GROUP BY o.order_id;
    #         """)
    #         print("Order summary built successfully.")

    # load_orders() >> load_order_items() >> build_summary()


# def etl_orders_pipeline():
#     load_orders_task = PythonOperator(



    # load_order_items_task = PythonOperator(...)
    # build_summary_task = PythonOperator(...)
    # load_orders_task >> load_order_items_task >> build_summary_task