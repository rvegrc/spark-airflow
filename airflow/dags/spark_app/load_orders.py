
import pandas as pd
from dotenv import load_dotenv
import os
from sqlalchemy import create_engine
import psycopg2
import sys
from datetime import timedelta
from datetime import datetime
from io import StringIO

# Импортируем функцию для вставки данных в таблицу
import insert_to_db_table 


def load_orders_from_csv(path_to_csv: str, cur) -> None:
    """
    Загружает данные заказов из CSV файла в таблицу orders в БД PostgreSQL.
    Параметры:
    - path_to_csv: Путь к CSV файлу с данными заказов.
    - cur: Курсор для выполнения SQL-запросов.
    """

    # Считываем данные из CSV
    orders_new = pd.read_csv(path_to_csv)

    customer_id_tuple = tuple(orders_new['customer_id'].tolist())
    cur.execute(f'''
        SELECT c.customer_id
        FROM customers c
        WHERE 
            customer_id IN {customer_id_tuple};
        ''')
    customer_ids_same = cur.fetchall()
    
    
    customer_ids_same = pd.DataFrame(customer_ids_same)

    orders_new_filtered = orders_new.query('customer_id in @customer_ids_same[0]')

    # Вставляем данные в таблицу orders
    insert_to_db_table(orders_new_filtered, 'orders', 'order_id', cur)
