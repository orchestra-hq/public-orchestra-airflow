import datetime
from airflow import DAG
from airflow.operators.bash_operator import BashOperator

default_args = {
    'owner': 'orchestra',
    'start_date': datetime.datetime(2023, 8, 1),
    'schedule_interval': '@daily'
}

dag = DAG('my_dag_tasks', default_args=default_args)

task1 = BashOperator(task_id='task1', bash_command='echo "This is Task 1"', dag=dag)
task2 = BashOperator(task_id='task2', bash_command='echo "This is Task 2"', dag=dag)
task3 = BashOperator(task_id='task3', bash_command='echo "This is Task 3"', dag=dag)

task1 >> task2
task1 >> task3
