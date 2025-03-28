import json
import pandas as pd
import os
import sys
import sklearn
import datetime
from datetime import datetime as dt
import numpy as np

import io
import sys

import clickhouse_connect

from pprint import pprint
import pytz

from pydantic import BaseModel, Field
from typing import List, Dict, Any, Optional

import logging


# eda
import phik


# visualization
import matplotlib.pyplot as plt
import seaborn as sns

# ml
# import xgboost as xgb
import catboost as ctb
# import lightgbm as lgb

import joblib

# import optuna
# from optuna.visualization.matplotlib import plot_param_importances

import mlflow
import mlflow.sklearn
from mlflow.models.signature import infer_signature



from sklearn.model_selection import cross_val_score
from sklearn.model_selection import train_test_split

from sklearn.pipeline import Pipeline as skl_pipeline

from sklearn.base import BaseEstimator, TransformerMixin

# from imblearn.pipeline import Pipeline as imb_pipeline
# from imblearn.over_sampling import SMOTE

from sklearn.compose import ColumnTransformer
from sklearn.preprocessing import OneHotEncoder, PowerTransformer, OrdinalEncoder, StandardScaler, RobustScaler

sklearn.set_config(transform_output='pandas')

# load metrics
from sklearn.metrics import mean_squared_error, mean_absolute_error, r2_score

# turn off warnings
import warnings
warnings.filterwarnings('ignore')


# set all columns to be displayed
pd.set_option('display.max_columns', None)


from dotenv import load_dotenv
load_dotenv()

# constants
RAND_ST = 345
CH_USER = os.getenv("CH_USER")
CH_PASS = os.getenv("CH_PASS")
CH_IP = os.getenv('CH_IP')
# AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID')
# AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
# AWS_DEFAULT_REGION = os.getenv('AWS_DEFAULT_REGION')
# MLFLOW_S3_ENDPOINT_URL = os.getenv('MLFLOW_S3_ENDPOINT_URL')
# AWS_S3_ENDPOINT_URL=os.getenv('AWS_S3_ENDPOINT_URL')
# os.environ["AWS_S3_SIGNATURE_VERSION"] = "s3v4"

MLFLOW_TRACKING_URI = os.getenv('MLFLOW_TRACKING_URI')
# MLFLOW_TRACKING_URI = 'http://localhost:15000'
mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)


from pendulum import datetime, duration
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime, timedelta
from airflow.decorators import dag, task


# Define the timezone
EXP_TIMEZONE = pytz.timezone('Etc/GMT-3')


# get connection to db
client = clickhouse_connect.get_client(host=CH_IP, port=8123, username=CH_USER, password=CH_PASS)

CLICKHOUSE_CONN_ID = 'clickhouse'
SPARK_CONN_ID = 'spark'
ROOT_PATH = '/opt/airflow/airflow_data'

# import tools
# Get the parent directory
parent_dir = os.path.abspath(os.path.join(os.getcwd(), ".."))

# Add parent directory to sys.path
sys.path.append(parent_dir)


from tools import pd_tools, db_tools
from tools.db_tools import DbTools
from custom_transformers import SafePowerTransformer

db_tools = DbTools(data_path, tmp_path, client)





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



