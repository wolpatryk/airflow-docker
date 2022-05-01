import sys
import json
import pendulum
import datetime

from pathlib import Path

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.decorators import task

from defs.custom_library import custom_function


# sample data to be processed

data = {
    'fname': 'Patryk',
    'lname': 'Wolniewicz'
}

# always pass ti parameter to init XCOM push/pull

# [START howto_operator_python]


def function_1(python_dict, ti):
    print('@'*40)
    name = python_dict['fname']
    ti.xcom_push(key="testing_name", value=name)
    ti.xcom_push(key="dict", value=python_dict)
    print('@'*40)


def function_2(ti):
    print('@'*40)
    name_from_function_1 = ti.xcom_pull(key='testing_name')
    name_from_function_1 = name_from_function_1 + " from function_1"
    print(name_from_function_1)
    print('@'*40)
    full_dict = ti.xcom_pull(key='dict')
    print(full_dict)
    print('@'*40)

    print('create file in custom folder')

    custom_folder = 'first_dag_output'
    custom_folder_path = f"/opt/airflow/airflowdump/{custom_folder}"
    Path(custom_folder_path).mkdir(parents=True, exist_ok=True)

    with open(f'{custom_folder_path}/data{str(datetime.datetime.now().strftime("%m_%d_%Y_%H_%M_%S"))}.json', 'w') as f:
        f.write(json.dumps(full_dict))
    print('done')
    print('@'*40)


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'catchup_by_default': False,
}

with DAG(
    # [BEGIN DAG CONFIG]
    dag_id='0aA_first_dag',
    schedule_interval='*/1 * * * *',
    start_date=pendulum.datetime(2022, 4, 24, tz="Europe/Warsaw"),
    max_active_runs=1,
    concurrency=1,
    catchup=False,
    tags=['my_test'],
    default_args=default_args,
    # [END OF DAG CONFIG]
) as dag:

    # [START fun_1]

    fun_1 = PythonOperator(
        task_id='function_1',
        python_callable=function_1,
        op_kwargs={'python_dict': data}
    )

    # [END fun_1]

    # [START fun_2]

    fun_2 = PythonOperator(
        task_id='function_2',
        python_callable=function_2,
    )

    # [END fun_2]

    # [PIPELINE ORDER]
    fun_1 >> fun_2

    @task(task_id="dag_debug")
    def debug_function():
        print('@'*40)
        print('[debug]')
        print(f"Python {sys.version.split()[0]}")
        print(custom_function('Patryk'))
        print('@'*40)

    debug_task = debug_function()
    # debug_task