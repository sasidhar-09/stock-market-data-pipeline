"""
Simple DAG to understand Airflow basics
This is a learning DAG - shows task dependencies and operators
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

# Default arguments for all tasks
default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Python functions that tasks will run
def say_hello():
    print("Hello from Airflow!")
    return "Hello task completed"

def print_date():
    print(f"Current date: {datetime.now()}")
    return "Date task completed"

def say_goodbye():
    print("Goodbye from Airflow!")
    return "Goodbye task completed"

# Define the DAG
dag = DAG(
    'hello_world_pipeline',
    default_args=default_args,
    description='A simple tutorial DAG',
    schedule_interval=None,  # Manual trigger only
    start_date=datetime(2026, 3, 1),
    catchup=False,
    tags=['tutorial', 'learning'],
)

# Define tasks
task_hello = PythonOperator(
    task_id='say_hello',
    python_callable=say_hello,
    dag=dag,
)

task_date = PythonOperator(
    task_id='print_current_date',
    python_callable=print_date,
    dag=dag,
)

task_bash = BashOperator(
    task_id='run_bash_command',
    bash_command='echo "Running a bash command!"',
    dag=dag,
)

task_goodbye = PythonOperator(
    task_id='say_goodbye',
    python_callable=say_goodbye,
    dag=dag,
)

# Define task dependencies
# Task flow: hello → date → bash → goodbye
task_hello >> task_date >> task_bash >> task_goodbye