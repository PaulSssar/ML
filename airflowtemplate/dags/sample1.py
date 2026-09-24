from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id='classic_linear_dag',
    start_date=datetime(2023, 1, 1),
    schedule='@daily',
    catchup=False
) as dag:

    task_1 = BashOperator(task_id='first_step', bash_command='echo "Step 1"')
    task_2 = BashOperator(task_id='second_step', bash_command='echo "Step 2"')
    task_3 = BashOperator(task_id='third_step', bash_command='echo "Step 3"')

    # Установка линейной связи оператором >>
    task_1 >> task_2 >> task_3
