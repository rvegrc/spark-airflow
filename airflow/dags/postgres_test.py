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

def postgres_test():
    @task
    def test_connection():
        try:
            cur = conn.cursor()
            cur.execute("SELECT 1;")
            result = cur.fetchone()
            cur.close()
            if result[0] == 1:
                print("Connection to PostgreSQL is successful.")
            else:
                print("Connection to PostgreSQL failed.")
        except Exception as e:
            print(f"An error occurred: {e}")
            sys.exit(1)
    
    test_connection()

postgres_test()