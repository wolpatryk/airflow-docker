import sys
import pendulum

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.decorators import task

# custom modules

from defs.mongodb import get_mongo_creds

import json
import requests
import urllib.parse
from pymongo import MongoClient
from bs4 import BeautifulSoup
from datetime import datetime

def function_1():
    print('@'*40)
    print("Geting LG C1 data...")
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/101.0.4951.54 Safari/537.36'
    }

    url = "https://www.ceneo.pl/103735224"
    req = requests.get(url = url, headers = headers).text
    soup = BeautifulSoup(req, 'html.parser')
    data = json.loads(soup.script.text.strip())
    lowest_price = data['offers']['lowPrice']
    price = soup.find_all("div", {"class": "product-top__price"})
    top_offer = price[0].text.strip().split('\n')[0]
    top_offer = float(top_offer.replace(",", ".").replace(" ", "").replace("zł", ""))
    print("Done")
    print("Exporting data to db...")
    test = {
        "scrapTime": str(datetime.now()),
        "TV": "C1",
        "url": url,
        "lowestPrice": lowest_price,
        "topOffer": top_offer,
    }

    password = urllib.parse.quote_plus(get_mongo_creds())
    lconn = f'mongodb+srv://kagmajn:{password}@ceneo.6uttn.mongodb.net/myFirstDatabase?retryWrites=true&w=majority'
    lcluster = MongoClient(lconn)
    ldb = lcluster['lg']
    lcollection = ldb['c1']
    lcollection.insert_one(test)
    print('Done, success')
    print('@'*40)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'catchup_by_default': False,
}

with DAG(
    # [BEGIN DAG CONFIG]
    dag_id='scrap_lg_c1',
    schedule_interval="0 * * * *",
    start_date=pendulum.datetime(2022, 5, 10, tz="Europe/Warsaw"),
    max_active_runs=1,
    concurrency=1,
    catchup=False,
    tags=['scrap', 'logging', 'requests'],
    default_args=default_args,
    # [END OF DAG CONFIG]
) as dag:

    # [START fun_1]


    fun_1 = PythonOperator(
        task_id='function_1',
        python_callable=function_1
    )

    # [END fun_1]


    # [PIPELINE ORDER]
    fun_1

    @task(task_id="dag_debug")
    def debug_function():
        print('@'*40)
        print('[debug]')
        print(f"Python {sys.version.split()[0]}")
        print('@'*40)

    debug_task = debug_function()
    # debug_task


