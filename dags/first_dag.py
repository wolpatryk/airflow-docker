import pendulum
import sys

from pprint import pprint

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.decorators import task

from selenium import webdriver

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
    name_from_function_1 = name_from_function_1 + " from function_2"
    print(name_from_function_1)
    print('@'*40)
    full_dict = ti.xcom_pull(key='dict')
    print(full_dict)
    print('@'*40)


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
}

with DAG(
    # [BEGIN DAG CONFIG]
    dag_id='first_dag',
    schedule_interval='*/5 * * * * *',
    start_date=pendulum.datetime(2021, 4, 23, tz="Europe/Warsaw"),
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

    # # [START howto_operator_python]
    # @task(task_id="function_1")
    # def function_1():
    #     print('function_2')

    # task_1 = function_1()
    # # [END howto_operator_python]

    # # [START howto_operator_python_kwargs]

    # @task(task_id="function_2")
    # def function_2():
    #     print(f'function_2')

    # task_2 = function_2()
    # # [END howto_operator_python_kwargs]

    # # [AIRFLOW FUNCTION RUN ORDER]

    # task_1 >> task_2
