# def run():
#     """
#     Функция для вставки новых заказов в таблицу orders в базе данных.
#     Использует psycopg2 для подключения к базе данных и выполнения SQL-запросов.
#     """

import pandas as pd
from dotenv import load_dotenv
import os
from sqlalchemy import create_engine
import psycopg2
import sys
from datetime import timedelta
from datetime import datetime
from io import StringIO


def insert_to_db_table(df: pd.DataFrame, table: str, id: str, cur) -> None:
    """
    Добавляет новые заказы из DataFrame 'df' в таблицу 'table' orders через psycopg2.
    Выводит количество добавленных заказов.
    Параметры:
    - df: DataFrame с новыми заказами
    - table: имя таблицы в базе данных, куда будут добавлены заказы
    - cur: курсор для выполнения SQL-запросов
    """

    df_id_tuple = tuple(df[id].tolist())

    cols = ','.join(df.columns)

    # Получаем уже существующие order_id
    cur.execute(f'''
        SELECT o.{id}
        FROM {table} o
        WHERE {id} IN {df_id_tuple};
    ''')

    same_table_ids = cur.fetchall()

    if len(same_table_ids) > 0:
        same_table_ids = pd.DataFrame(same_table_ids)[0].tolist()
        print(f'Найдено {len(same_table_ids)} заказов, которые уже есть в таблице {table}')    

    else:
        same_table_ids = []

    # Оставляем только новые заказы
    df_filtered = df.query(f'{id} not in @same_table_ids')

    if df_filtered.empty:
        print('Нет новых заказов для добавления.')

    else:
        # Используем StringIO для передачи данных через stdin в COPY
        csv_buffer = StringIO()
        to_csv = df_filtered.copy(deep=True)
        to_csv.to_csv(csv_buffer, index=False, header=False)
        csv_buffer.seek(0)

        cur.copy_expert(
            f"""
            COPY {table} ({cols})
            FROM STDIN WITH (FORMAT CSV)
            """,
            csv_buffer
        )

        print(f'Добавлено {len(df_filtered)} новых заказов в таблицу {table}')     

# # запускать только при непосредственном выполнении файла; если файл импортируется как модуль, функция run не будет вызвана
# if __name__ == "__main__":
#     run()

