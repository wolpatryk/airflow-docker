import sys
import pendulum

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.decorators import task

from defs.mail import sendemail

def function_1():
    print('@'*40)
    print("sending mail...")
    sendemail()
    print('done')
    print('@'*40)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'catchup_by_default': False,
}

with DAG(
    # [BEGIN DAG CONFIG]
    dag_id='send_email',
    schedule_interval='0 * * * *',
    start_date=pendulum.datetime(2022, 4, 24, tz="Europe/Warsaw"),
    max_active_runs=1,
    concurrency=1,
    catchup=False,
    tags=['email', 'logging'],
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