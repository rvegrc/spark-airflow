from pyspark.sql import SparkSession
import os

from dotenv import load_dotenv
load_dotenv()

CH_IP = os.getenv('CH_IP')
CH_USER = os.getenv('CH_USER')
CH_PASS = os.getenv('CH_PASS')



def run():
    # packages = [
    #     "com.clickhouse.spark:clickhouse-spark-runtime-3.5_2.12:0.8.0"
    #     # "com.github.housepower:clickhouse-spark-runtime-3.4_2.12:0.7.3"
    #     ,"com.clickhouse:clickhouse-jdbc:0.7.1-patch1"
    #     # ,"com.clickhouse:clickhouse-jdbc:0.6.0-patch5"
    #     ,"com.clickhouse:clickhouse-http-client:0.7.1-patch1"
    #     # ,"com.clickhouse:clickhouse-http-client:0.6.0-patch5"
    #     ,"org.apache.httpcomponents.client5:httpclient5:5.3.1"
    #     # for jdbc 2.7.1 required java 8/11
    #     # ,"com.github.housepower:clickhouse-native-jdbc:2.7.1"


    # ]
    
    appName = "Connect To ClickHouse via PySpark"
    
    spark = (SparkSession.builder.appName(appName)
        #  .config("spark.jars.packages", ",".join(packages))
        #  .config("spark.sql.catalog.clickhouse", "xenon.clickhouse.ClickHouseCatalog")
         .config("spark.sql.catalog.clickhouse", "com.clickhouse.spark.ClickHouseCatalog")
         .config("spark.sql.catalog.clickhouse.host", CH_IP)
         .config("spark.sql.catalog.clickhouse.protocol", "http")
         .config("spark.sql.catalog.clickhouse.http_port", "8123")
         .config("spark.sql.catalog.clickhouse.user", CH_USER)
         .config("spark.sql.catalog.clickhouse.password", CH_PASS)
         .config("spark.sql.catalog.clickhouse.database", "default")
         .config('spark.jars.repositories', 'https://artifacts.unidata.ucar.edu/repository/unidata-all')
            #  .config("spark.sql.debug.maxToStringFields", "100000")
         .getOrCreate()
    )
    
    spark.sql("use clickhouse")

    # root = "/opt/airflow"
    # data = [("Alice", 1), ("Bob", 2), ("Charlie", 3)]
    # df = spark.createDataFrame(data, ["Name", "Value"])
    
    # # upload df to clickhouse
    # (
    #     df.writeTo("tmp.spark_test")
    #     .tableProperty("engine", "MergeTree()")
    #     .tableProperty("order_by", "tuple()")
    #     .tableProperty("settings.index_granularity", "8192")
    #     .tableProperty("settings.allow_nullable_key", "1")
    #     .createOrReplace()
    # )


    # spark.sql("select * from tmp.spark_test").show()
      
    
    return


# start only if you run the file, if the file would be instead imported the run command will be ignored 
if __name__ == "__main__":
    run()