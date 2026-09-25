from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id='simple_parallel_dag',
    start_date=datetime(2023, 1, 1),
    schedule=None,
    catchup=False
) as dag:

    start = BashOperator(task_id='start', bash_command='echo "Начало"')

    # Эти две задачи будут выполняться параллельно
    process_a = BashOperator(task_id='process_A', bash_command='sleep 5 && echo "Ветка А"')
    process_b = BashOperator(task_id='process_B', bash_command='sleep 5 && echo "Ветка Б"')

    end = BashOperator(task_id='end', bash_command='echo "Конец"')

    # Определение структуры:
    # start -> [process_a, process_b] -> end
    start >> [process_a, process_b] >> end
