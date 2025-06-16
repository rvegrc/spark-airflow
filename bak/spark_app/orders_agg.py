import pandas as pd
from dotenv import load_dotenv
import os
from sqlalchemy import create_engine
import sys
from datetime import timedelta
from airflow.decorators import dag, task
from airflow.operators.python import PythonOperator
from datetime import datetime

def load_orders_from_csv(path_to_csv: str, engine) -> None:
    """
    Загружает данные заказов из CSV файла в таблицу orders в БД PostgreSQL.
    Параметры:
    - path_to_csv: Путь к CSV файлу с данными заказов.
    - engine: объект SQLAlchemy для подключения к базе данных
    """

    # Считываем данные из CSV
    orders_new = pd.read_csv(path_to_csv)

    # Считываем данные из таблиц customers и orders
    orders_ids = pd.read_sql_query('SELECT order_id from orders', engine)
    customers_ids = pd.read_sql_query('SELECT customer_id from customers', engine)

    print(orders_ids)

    # # Оставляем только те строки, где customer_id уже существует в таблице customers и только новые заказы
    # orders_new_to_db = customers_ids. \
    #     merge(
    #     orders_new
    #     ,on='customer_id'
    #     ,how='inner'
    #     ). \
    #     merge(
    #     orders_ids
    #     ,on='order_id'
    #     ,how='left'
    #     ,indicator=True
    #     ). \
    #     query('_merge == "left_only"').drop(columns=['_merge'])
    
    # # Загружаем очищенные данные в таблицу orders
    # # orders_new_to_db.to_sql('orders', engine, if_exists='append', index=False)
    # print(f"Загружено {len(orders_new_to_db)} новых заказов в таблицу orders.")
