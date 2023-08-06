from datetime import datetime

from airflow.models import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator

default_args = {
    'owner': 'orchestra',
    'start_date': datetime(2023, 3, 1, 12, 0, 0)
}

def hello_world_loop():
    for palabra in ['hello', 'world']:
        print(palabra)

with DAG(
    dag_id='hello_world',
    default_args = default_args,
    schedule_interval='@once'
) as dag:

    test_start = DummyOperator(task_id='test_start')

    test_python = PythonOperator(task_id='test_python', python_callable=hello_world_loop)

    test_bash =  BashOperator(task_id='test_bash', bash_command='echo Hello World!')

test_start >> test_python >> test_bash


