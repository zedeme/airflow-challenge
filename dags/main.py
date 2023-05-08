import logging
import os
from datetime import datetime, timedelta

import requests as rq
import pandas as pd

from airflow import DAG
from airflow.operators.python import PythonOperator

# I will use this loger config because its not in the challenge
logging.basicConfig()
logger = logging.getLogger(__name__)

args = {
    "owner": "test",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 5,
    "retry_delay": timedelta(minutes=5)
}


# ETL process
class ETL():
    # extract function works if the url's option its True
    # with the data is already in ../data this function dont will work
    def extract(self) -> None:
        if self.option_url:
            logger.info("url option selected")
            for url in self.urls:
                name_url = url.split("//")[1].split(".")[0]

                with open(self.data_path + name_url + ".csv",
                          "w+",
                          encoding="utf8") as f:
                    try:
                        logger.info("making requests to " + url)
                        response = rq.get(url)
                        csv = response.content
                        f.write(csv)
                        logger.info("saving " + name_url + ".csv")

                    except Exception as e:
                        logger.error("Error getting the data from url " +
                                     str(e))

                    finally:
                        f.close()
                        return

    # transform function read all csv in ../data/
    # and append into one in ../c_data
    def transform(self) -> None:
        try:
            files = [file for file in os.listdir(self.data_path)
                     if os.path.isfile(self.data_path + file)]

            columns_t: list = []
            df: pd.DataFrame = pd.DataFrame()

            for file in files:
                logger.info("reading " + file)

                df_t = pd.read_csv(self.data_path + file)

                if columns_t != []:
                    if df_t.columns == columns_t:
                        df.append(df_t)
                        logger.info("Append data " + file)
                        continue
                    logger.warning("Error data format " +
                                   file + "!=" + files[0])
                    continue

                columns_t = df.columns

            c_data = os.path.dirname(__file__) + "../c_data"
            self.c_data = c_data

            df = df.dropna(axis="rows")
            logger.info("Saving append data")

            with open(self.c_data + "/" + "data.csv",
                      "w+",
                      encoding="utf8") as f:
                f.write(df)
                f.close()

        except Exception as e:
            logger.error("Error founding data files in ../data/ " + str(e))

    # load function make the table
    def load(self):
        df: pd.DataFrame = pd.read_csv(self.c_data + "/" + "data.csv")
        logger.info("Making the table")

        try:
            df_t = df.groupby(index=["YEAR", "STNAME"], columns="AAWDT")

        except Exception as e:
            logger.error("Error data format" + str(e))

        print(df_t)

    # I set the data path on ../data but
    # I use os.path to no get error if you change the routers
    def __init__(self,
                 data_path: str = os.path.dirname(__file__) + "../data/",
                 data_urls: list = []
                 ) -> None:
        self.data_path = data_path
        logger.info("To use the ETL process need to put the url with the .csv or put your csv in ../data/")
        logger.info("If you want to put the csv in ../data dont type in input")
        logger.info("If you want put url modify __init__ in " + __file__)

        if data_urls == []:
            self.option_url = False
            return
        self.option_url = True
        self.urls = data_urls


with DAG(
    "test",
    default_args=args,
    description="DAG to get seattle's traffic accidents and make ETL process",
    schedule_interval=timedelta(hours=24),
    start_date=datetime(day=7, month=5, year=2023)

) as dag:
    etl = ETL()

    extract = PythonOperator(task_id="extract",
                             python_callable=etl.extract)

    transform = PythonOperator(task_id="transform",
                               python_callable=etl.transform)

    load = PythonOperator(task_id="load",
                          python_callable=etl.load)

    extract >> transform >> load
