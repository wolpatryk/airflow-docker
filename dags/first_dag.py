import sys
import os
import pendulum
import glob
import uuid
import shutil

from PIL import Image
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
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

    for tmp_ext in ['.jpeg', '.JPEG', '.jpg', '.JPG', '.PNG', '.png', '.jfif', '.JFIF', '.webp', '.WEBP']:
        images_to_transform = []
        for root, dirs, files in os.walk(source_folder, topdown = True):
            for name in files:
                images_to_transform.append(name)


        try:
            images_to_transform = [image for image in images_to_transform if image.endswith(tmp_ext)]

            for img in images_to_transform:
                source_img = f"{source_folder}/{img}"
                im = Image.open(source_img).convert("RGB")
                output = f"{output_folder}/{img.split(tmp_ext)[0]}.jpg"
                im.save(output)
                os.remove(source_img)
        except Exception as e:
            print("Exception while image convertion:")
            print(e)

    print('done')
    print('@'*40)

def function_3():
    extensions = ['.jpeg', '.JPEG', '.jpg', '.JPG', '.PNG', '.png', '.jfif', '.JFIF']
    source_dir = f"/opt/airflow/images/source"
    output_dir = f"/opt/airflow/images/ame"

    images = []
    for root, dirs, files in os.walk(source_dir, topdown=False):
        for name in files:
            image_path = os.path.join(f"{root}/{name}")
            images.append(image_path)
    print('@' * 40)
    print(images)
    print('@'*40)
    L = [f for f in os.listdir(source_dir) if os.path.splitext(f)[1] in extensions]

    for f in L:
        try:
            source_file = f"{source_dir}/{f}"
            ext = source_file.split('.')[-1]
            shutil.move(f"{source_file}", output_dir)
        except:
            old_name = fr"{source_file}"
            new_name = fr"{source_file}-{str(uuid.uuid4())}.{ext}"
            os.rename(old_name, new_name)
            shutil.move(f"{new_name}", output_dir)


# def function_4():
#     # make a backup
#     import subprocess
#     src_path = f"/opt/airflow/images/ame"
#     dst_path = f"/opt/airflow/images/ame_backup"
#
#     # subprocess.call(['xcopy', "/d", "/y", src_path, dst_path])
#
#     import shutil
#
#     # shutil.copytree('bar', 'foo')
#     shutil.copytree(src_path, dst_path, dirs_exist_ok = True)



default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'catchup_by_default': False,
}

with DAG(
    # [BEGIN DAG CONFIG]
    dag_id='transform_images',
    description="""
    Converts images to jpg and moves to different folder
    """,
    schedule_interval='0 * * * *',
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

    # [START fun_3]

    fun_3 = PythonOperator(
        task_id = 'function_3',
        python_callable = function_3,
    )

    # fun_4 = PythonOperator(
    #     task_id = 'function_4',
    #     python_callable = function_4,
    # )

    # [END fun_3]

    # run external DAG
    # trigger = TriggerDagRunOperator(
    #     task_id = "send_email",
    #     trigger_dag_id = "send_email",
    #     dag = dag,
    # )
    # [PIPELINE ORDER]
    fun_1 >> fun_2 >> fun_3
    # fun_1 >> fun_2 >> fun_3 >> fun_4
    # fun_1 >> fun_2 >> fun_3 >> trigger


    @task(task_id="dag_debug")
    def debug_function():
        print('@'*40)
        print('[debug]')
        print(f"Python {sys.version.split()[0]}")
        print(custom_function('Patryk'))
        print('@'*40)

    debug_task = debug_function()
    # debug_task