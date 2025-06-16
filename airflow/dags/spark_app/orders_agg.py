import pandas as pd
from dotenv import load_dotenv
import os
import sys
from datetime import timedelta
from datetime import datetime
from io import StringIO

# Импортируем функцию для вставки данных в таблицу
# from spark_app.insert_to_db_table import insert_to_db_table

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
        print(f'Найдено {len(same_table_ids)} одинаковых {id}, которые уже есть в таблице {table}')    

    else:
        same_table_ids = []

    # Оставляем только новые заказы
    df_filtered = df.query(f'{id} not in @same_table_ids')

    if df_filtered.empty:
        print('Нет новых данных для добавления.')

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

        print(f'Добавлено {len(df_filtered)} новых {id} в таблицу {table}')   



def load_orders_from_csv(path_to_csv: str, cur) -> None:
    """
    Загружает данные заказов из CSV файла в таблицу orders в БД PostgreSQL.
    Параметры:
    - path_to_csv: Путь к CSV файлу с данными заказов.
    - cur: Курсор для выполнения SQL-запросов.
    """

    # Считываем данные из CSV
    orders_new = pd.read_csv(path_to_csv)

    # Выбираем, что в orders_new есть столбец customer_id с такими же customer_id, как в таблице customers
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

def load_items_from_csv(path_to_csv: str, cur) -> None:
    """
    Загружает данные позиций заказов из CSV файла в таблицу в БД PostgreSQL.
    Параметры:
    - path_to_csv: Путь к CSV файлу с данными позиций заказов.
    - cur: Курсор для выполнения SQL-запросов.
    """

    # Считываем данные из CSV
    order_items_new = pd.read_csv(path_to_csv)

    # Получаем все order_id из таблицы orders, которые есть в order_items_new
    cur.execute(f'''
        SELECT order_id
        FROM orders
        WHERE order_id IN {tuple(order_items_new['order_id'].tolist())}
    ''')
    orders_ids = pd.DataFrame(cur.fetchall(), columns=['order_id'])

    # Получаем из таблицы products все price и product_id, которые есть в order_items_new
    cur.execute(f'''
        SELECT product_id, price
        FROM products
        WHERE product_id IN {tuple(order_items_new['product_id'].tolist())}
    ''')
    products_prices = pd.DataFrame(cur.fetchall(), columns=['product_id', 'price'])

    # Объединяем данные для вставки в таблицу order_items
    order_items_new_to_db = order_items_new.merge(orders_ids['order_id'], on='order_id', how='inner'). \
        merge(products_prices[['product_id', 'price']], on='product_id', how='inner'). \
        rename(columns={'price': 'unit_price'})
        
    # Вставляем данные в таблицу order_items
    insert_to_db_table(order_items_new_to_db, 'order_items', 'order_item_id', cur)

def customer_summary_to_db(cur) -> None:
    """
    Обновляет в таблице customer_summary новые агрегированные данные по потраченным суммам клиентов, заменяет старые данные.
    Параметры:
    - cur: курсор для выполнения SQL-запросов
    """
    
    # Получаем агрегированные данные по клиентам
    cur.execute('''
        SELECT 
            o.customer_id AS customer_id,
            c.customer_name AS customer_name,
            COUNT(o.order_id) AS total_orders,
            SUM(o.total_amount) AS total_spent,
            MAX(o.order_date) AS last_order_date                 
        FROM orders o
        LEFT JOIN customers c
            ON o.customer_id = c.customer_id
        GROUP BY o.customer_id, c.customer_name
    '''
)

    cols=['customer_id', 'customer_name', 'total_orders', 'total_spent', 'last_order_date']

    customer_summary = pd.DataFrame(cur.fetchall(), columns=cols)
    customer_summary['total_spent'] = customer_summary['total_spent'].astype(float)

    id = 'customer_id'
    table = 'customer_summary'

    customer_summary_id_tuple = tuple(customer_summary[id].tolist())
    
    cols = ','.join(customer_summary.columns)

    # Получаем уже существующие customer_id
    cur.execute(f'''
        SELECT c.{id}
        FROM {table} c
        WHERE {id} IN {customer_summary_id_tuple};
    ''')

    same_table_ids = cur.fetchall()

    if len(same_table_ids) > 0:
        same_table_ids = pd.DataFrame(same_table_ids)[0].tolist()
        print(f'Найдено {len(same_table_ids)} записей по клиентам, которые уже есть в таблице {table}')
        # удаляем строчки с существующими customer_id из БД
        cur.execute(f'''
            DELETE FROM {table}
            WHERE {id} IN {tuple(same_table_ids)};
        ''')
        print(f'Удалено {len(same_table_ids)} старых записей по клиентам из таблицы {table}')    
  

    # Используем StringIO для передачи данных через stdin в COPY
    csv_buffer = StringIO()
    to_csv = customer_summary.copy(deep=True)
    to_csv.to_csv(csv_buffer, index=False, header=False)
    csv_buffer.seek(0)

    cur.copy_expert(
        f"""
        COPY {table} ({cols})
        FROM STDIN WITH (FORMAT CSV)
        """,
        csv_buffer
    )

    print(f'Обновлено {len(same_table_ids)} записей по клиентам в таблице {table}')
    print(f'Добавлено {len(customer_summary) - len(same_table_ids)} новых записей по клиентам в таблице {table}')
