from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

# Define default arguments
default_args = {
    'owner': 'Panagiotis Vaidomarkakis',
    'start_date': datetime(2023, 4, 15),
    'depends_on_past': False,
    'retries': 1
}

# Define DAG
dag = DAG(
    'id-t4',
    default_args=default_args,
    description='Toy workflow using BashOperator',
    schedule=None,
)

# Define the 2 tasks
task_1_1 = BashOperator(
    task_id='print_first_name',
    bash_command='echo "Panagiotis"',
    dag=dag
)

task_1_2 = BashOperator(
    task_id='calculate_last_name_length',
    bash_command='echo "${#LAST_NAME}"',
    env={'LAST_NAME': 'Vaidomarkakis'},
    dag=dag
)

# Define task dependencies and exectution
task_1_1 >> task_1_2