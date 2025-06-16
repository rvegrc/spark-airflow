from pendulum import datetime, duration
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime, timedelta
from airflow.decorators import dag, task
default_args={
    "owner": "rvegrc",
    "depends_on_past": False,
    "retries": 0
}
ram = 20
cpu = 30*3
@dag(
    tags=["test", "stocks"],
    render_template_as_native_obj=True,
    max_active_runs=1,
    #schedule='50 2 * * *',
    schedule=None,
    default_args=default_args,
    start_date=datetime(2023, 12, 1),
    catchup=False,
    description='testing connection',
    doc_md=__doc__
)
def spark_example():
    spark_submit_task = SparkSubmitOperator(
        task_id='spark_submit_job',
        application='dags/spark_app/spark_1.py',
        #conn_id='spark_master',
        conn_id='spark',
        total_executor_cores='1',
        executor_cores='1',
        executor_memory=f'{ram}g',
        num_executors='1',
        driver_memory=f'{ram}g',
        verbose=True
    )


    spark_submit_task

spark_example()