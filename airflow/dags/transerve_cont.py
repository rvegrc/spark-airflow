from pendulum import datetime, duration
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from lib.clickhouse_operator_extended import ClickHouseOperatorExtended
from datetime import datetime, timedelta
from airflow.decorators import dag, task
from pyspark.sql import SparkSession
from pyspark import SparkContext
import pandas as pd
import os
import clickhouse_connect

CLICKHOUSE_CONN_ID = 'clickhouse'
SPARK_CONN_ID = 'spark'
ROOT_PATH = '/opt/airflow/airflow_data'

CH_IP = os.getenv('CH_IP')
CH_USER = os.getenv('CH_USER')
CH_PASS = os.getenv('CH_PASS')


default_args={
    "owner": "rvegrc",
    "depends_on_past": False,
    "retries": 0
}
ram = 20
cpu = 30*3

@dag(
    tags=["transerv_cont", "predict"],
    render_template_as_native_obj=True,
    max_active_runs=1,
    #schedule='50 2 * * *',
    schedule=None,
    default_args=default_args,
    start_date=datetime(2024, 12, 1),
    catchup=False,
    description='predict costs for services',
    template_searchpath='dags/include',
    doc_md=__doc__
)
def costs_predict():
    legs_costs_predict = SparkSubmitOperator(
        task_id='legs_costs_predict',
        application='dags/spark_app/legs_cost_predict.py',
        #conn_id='spark_master',
        conn_id='spark',
        total_executor_cores='1',
        executor_cores='1',
        executor_memory=f'{ram}g',
        num_executors='1',
        driver_memory=f'{ram}g',
        verbose=True
    )

        # test ClickHouseOperatorExtended
    update_legs_costs_result = ClickHouseOperatorExtended(
        task_id='update_legs_costs_result',
        clickhouse_conn_id=CLICKHOUSE_CONN_ID,
        sql='create_legs_costs_result.sql' # sql file in dag template_searchpath
    )



    update_legs_costs_result >> legs_costs_predict

costs_predict()