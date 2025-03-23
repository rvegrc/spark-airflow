from pendulum import datetime, duration
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from lib.clickhouse_operator_extended import ClickHouseOperatorExtended
from datetime import datetime, timedelta
from airflow.decorators import dag, task
from pyspark.sql import SparkSession
from pyspark import SparkContext
import pandas as pd
# from sedona.spark import SedonaContext
import os

CLICKHOUSE_CONN_ID = 'clickhouse'
SPARK_CONN_ID = 'spark'

CH_IP = os.getenv('CH_IP')
CH_USER = os.getenv('CH_USER')
CH_PASS = os.getenv('CH_PASS')



default_args={
    "owner": "rvegrc",
    "depends_on_past": False,
    "retries": 0
}
ram = 5
cpu = 30*3
@dag(
    tags=["test", "stocks"],
    render_template_as_native_obj=True,
    max_active_runs=1,
    #schedule='50 2 * * *',
    schedule=None,
    default_args=default_args,
    start_date=datetime(2023, 12, 1),
    template_searchpath='dags/include', 
    catchup=False,
    description='testing connection',
    doc_md=__doc__
)
def spark_clickhouse_test():
    packages = [
    "com.clickhouse.spark:clickhouse-spark-runtime-3.5_2.12:0.8.0"
    ,"com.clickhouse:clickhouse-jdbc:0.7.1-patch1"
    ,"com.clickhouse:clickhouse-client:0.7.1-patch1"
    ,"com.clickhouse:clickhouse-http-client:0.7.1-patch1"
    ,"org.apache.httpcomponents.client5:httpclient5:5.3.1"
    # ,"ai.catboost:catboost-spark_3.5_2.12:1.2.7"
    # ,"com.microsoft.azure:synapseml_2.12:1.0.8" 
    ]
    # packages = ["com.clickhouse.spark:clickhouse-spark-runtime-3.5_2.12:0.8.0",
    # "com.clickhouse:clickhouse-jdbc:0.7.0",
    # "com.clickhouse:clickhouse-client:0.7.0",
    # "com.clickhouse:clickhouse-http-client:0.7.0",
    # "org.apache.httpcomponents.client5:httpclient5:5.3.1",
    # 'org.apache.sedona:sedona-spark-3.5_2.12:1.7.0',
    # 'org.datasyslab:geotools-wrapper:1.7.0-28.5',
    # 'uk.co.gresearch.spark:spark-extension_2.12:2.11.0-3.4'
    # ]



    # spark_submit_task = SparkSubmitOperator(
    #     packages= ','.join(packages),
    #     task_id='spark_submit_task',
    #     application='dags/spark_app/spark_1.py',
    #     #conn_id='spark_master',
    #     conn_id=SPARK_CONN_ID,        
    #     total_executor_cores='1',
    #     executor_cores='1',
    #     executor_memory=f'{ram}g',
    #     num_executors='1',
    #     driver_memory=f'{ram}g',
    #     verbose=True
    # )
    
    clickhouse_test_conn = ClickHouseOperatorExtended(
        task_id='clickhouse_test_conn',
        clickhouse_conn_id=CLICKHOUSE_CONN_ID,
        sql='test.sql'
    )


    from functools import reduce
    from pyspark.sql import DataFrame
    import pyspark.sql.functions as F
    import pandas as pd

    packages = [
            "com.clickhouse.spark:clickhouse-spark-runtime-3.5_2.12:0.8.0"
            ,"com.clickhouse:clickhouse-jdbc:0.7.0"
            ,"com.clickhouse:clickhouse-client:0.7.0"
            ,"com.clickhouse:clickhouse-http-client:0.7.0"
            ,"org.apache.httpcomponents.client5:httpclient5:5.3.1"
            # ,'org.apache.sedona:sedona-spark-3.5_2.12:1.7.0'
            # ,'org.datasyslab:geotools-wrapper:1.7.0-28.5'
            # ,'uk.co.gresearch.spark:spark-extension_2.12:2.11.0-3.4'
        ]

    @task.pyspark(
            conn_id=SPARK_CONN_ID
            ,config_kwargs={'spark.jars.packages':','.join(packages)}
        )
    def run(spark: SparkSession, sc: SparkContext):

        appName = "Connect To ClickHouse via PySpark"
        spark = (SparkSession.builder
                .appName(appName)
                # .config("spark.jars.packages", ",".join(packages))
                #  .config("spark.sql.catalog.clickhouse", "xenon.clickhouse.ClickHouseCatalog")
                .config("spark.sql.catalog.clickhouse", "com.clickhouse.spark.ClickHouseCatalog")
                .config("spark.sql.catalog.clickhouse.host", CH_IP)
                .config("spark.sql.catalog.clickhouse.protocol", "http")
                .config("spark.sql.catalog.clickhouse.http_port", "8123")
                .config("spark.sql.catalog.clickhouse.user", CH_USER)
                .config("spark.sql.catalog.clickhouse.password", CH_PASS)
                .config("spark.sql.catalog.clickhouse.database", "default")
                #  .config("spark.spark.clickhouse.write.compression.codec", "lz4")
                #  .config("spark.clickhouse.read.compression.codec", "lz4")
                 .config("spark.executor.memory", f"{ram}g")
                #  .config("spark.executor.cores", "5")
                .config("spark.driver.maxResultSize", f"{ram}g")
                .config("spark.driver.memory", f"{ram}g")
                .config("spark.executor.memoryOverhead", f"{ram}g")
                #  .config("spark.sql.debug.maxToStringFields", "100000")
                .getOrCreate()
                )       



        # config = (
        #     SedonaContext.builder()
        #     .config('spark.jars.repositories', 'https://artifacts.unidata.ucar.edu/repository/unidata-all')
        #     .config("spark.sql.catalog.clickhouse", "com.clickhouse.spark.ClickHouseCatalog")
        #     .config("spark.sql.catalog.clickhouse.host", CH_IP)
        #     .config("spark.sql.catalog.clickhouse.protocol", "http")
        #     .config("spark.sql.catalog.clickhouse.http_port", "8123")
        #     .config("spark.sql.catalog.clickhouse.user", CH_USER)
        #     .config("spark.sql.catalog.clickhouse.password", CH_PASS) 
        #     .config("spark.sql.catalog.clickhouse.database", "default")
        #     #.config("spark.clickhouse.write.format", "json")
        #     .getOrCreate()
        # )
        # spark = SedonaContext.create(config)
        # sc = spark.sparkContext
        spark.sql("use clickhouse")


    clickhouse_test_conn >> run()

spark_clickhouse_test()

    

