import sys
import os
import pendulum

from PIL import Image
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.decorators import task

from defs.custom_library import custom_function

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

    source_folder = f"/opt/airflow/images/source"
    output_folder = f"/opt/airflow/images/ame"

    print('data in transform')
    images_to_transform = []
    for root, dirs, files in os.walk(source_folder, topdown = True):
        for name in files:
            images_to_transform.append(name)

    images_to_transform = [image for image in images_to_transform if image.endswith(".webp")]
    for img in images_to_transform:
        source_img = f"{source_folder}/{img}"
        im = Image.open(source_img).convert("RGB")
        output = f"{output_folder}/{img}.jpg"
        im.save(output)
        os.remove(source_img)

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
    schedule_interval='*/10 * * * *',
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