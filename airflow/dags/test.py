from pendulum import datetime, duration
import itertools
from airflow import XComArg
from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator  # Import the BashOperator
from lib.clickhouse_operator_extended import ClickHouseOperatorExtended
import logging
from airflow.operators.python import PythonOperator
import os
CLICKHOUSE_CONN_ID = 'clickhouse'
default_args={
    "owner": "Smalch",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": duration(seconds=5),
    # 'email_on_failure': True,
    # 'email_on_success': True,
    # 'email': EMAIL_DEVELOP,
}

@dag(
    tags=["test", "stocks"],
    render_template_as_native_obj=True,
    max_active_runs=1,
    #schedule='50 2 * * *',
    schedule=None,
    start_date=datetime(2023, 12, 1, tz='Europe/Moscow'),
    catchup=False,
    default_args=default_args,
    template_searchpath='dags/repo/airflow/dags/include',
    description='testing connection',
    doc_md=__doc__,
)
def test():
    # Task to print the current working directory
    print_pwd = BashOperator(
        task_id='print_pwd',
        bash_command='pwd'
    )
    def list_include_files():
        print("Contents of /opt/airflow/dags/repo/airflow/dags/include:")
        for file in os.listdir("/opt/airflow/dags/repo/airflow/dags/include"):
            print(file)

    list_files_task = PythonOperator(
        task_id='list_include_files',
        python_callable=list_include_files,
    )
    ch_list_count_rows_start_of_month = ClickHouseOperatorExtended(
        task_id='ch_list_count_rows_start_of_month',
        clickhouse_conn_id=CLICKHOUSE_CONN_ID,
        sql='test.sql'
    )

    # Setting up task dependencies
    print_pwd >> list_files_task >> ch_list_count_rows_start_of_month

test()
