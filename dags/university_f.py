from datetime import datetime, timedelta

from decouple import config
from sqlalchemy import create_engine
from sqlalchemy_utils import database_exists
from airflow import DAG
from airflow.operators.python import PythonOperator

import os

from s3_operator import S3Operator

from config_loader import get_logger

logger = get_logger()

# Arguments to be used by the dag by default
default_args = {
    "owner": "alkymer",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 5,
    "retry_delay": timedelta(minutes=5)
}


# It will contain the functions of the dag
class ETL():
    # will initialize configurations for the ETL
    def __init__(self, rows_file="./config/rows.yaml",
                 columns_file="./config/columns.yaml"):

        self.rows_config = rows_file
        self.columns_config = columns_file

        self.data_path = os.path.dirname(__file__) + "/../data/"

    def _database_status(self, engine):
        if not bool(engine):
            return False
        return True

    # public function that will be in charge of the
    # extraction through data queries hosted in AWS
    def extract(self):
        engine = create_engine("postgresql://" + config("_PG_USER") +
                               ":" + config("_PG_PASSWD") +
                               "@" + config("_PG_HOST") +
                               ":" + config("_PG_PORT") +
                               "/" + config("_PG_DB"))

        if not self._database_status(engine):
            raise ValueError("Database doesn't exists")

    # public function that will be in charge of data analysis
    # using pandas of raw data extracted from the database
    def transform(self):
        pass

    # public function that will be in charge of loading the information
    # later to be analyzed, cleaned and processed to a database for later use
    def load(self):
        pass


# The DAG that will be in charge of managing the ETL functions
with DAG(
    "university_f",
    default_args=default_args,
    description="""Dag to extract, process and load data
                  from Moron and Rio Cuarto's Universities""",
    schedule_interval=timedelta(hours=1),
    start_date=datetime.now()
) as dag:
    # initialize the ETL configuration
    etl = ETL()

    # declare the operators

    # all operators will be compatible with all the universities found in
    # the queries to be reusable in case queries are added or simply changed
    sql_task = PythonOperator(task_id="extract",
                              python_callable=etl.extract)

    pandas_task = PythonOperator(task_id="transform",
                                 python_callable=etl.transform)

    save_task = S3Operator(task_id="save",
                           logger=logger,
                           s3_credentials=config,
                           data_path=etl.data_path)

    # the dag will allow the ETL process to be done for all
    # the universities that have queries, are in /university_f/sql
    # and the configuration files of their columns/rows are in ./config/
    sql_task >> pandas_task >> save_task
error credentials airflow