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
    tags=["test", "edu", "postgres"],
    schedule="@daily"
    ,start_date=datetime(2025, 6, 12)
    ,catchup=False
)

def postgres_test():
    @task
    def test_connection():
        try:
            with engine.connect() as connection:
                result = connection.execute("SELECT 1")
                print("Connection successful:", result.fetchone())
        except Exception as e:
            print("Connection failed:", e)
            sys.exit(1)

    test_connection()

postgres_test()