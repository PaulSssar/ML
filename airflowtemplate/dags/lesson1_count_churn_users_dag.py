# импортируем библиотеки
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

# задаем параметры по-умолчанию
default_args = {
	'start_date': datetime(2026, 1, 1),
	'retries': 1,
	'retry_delay': timedelta(minutes=5)
}

def count_churn_users():
    import pandas as pd

    users_df = pd.DataFrame({
        "user_name": ["Alice", "Bob", "Charlie", "Diana", "Eve"],
        "churn": [0, 1, 0, 1, 0],
    })
    churned = users_df["churn"].sum()
    retained = len(users_df) - churned

    print(f"Churned users: {churned}")
    print(f"Retained users: {retained}")

with DAG(
    dag_id="count_churn",
    default_args=default_args,
    schedule=None,         
    catchup=False,
    tags=["Churned", "Retained"],
) as dag:

    # старт пайплайна: просто логируем начало выполнения
    start = BashOperator(
        task_id="start",
        bash_command='echo "Here we start!"',
    )
    # задача для расчета метрик
    count_churn_users_task =  PythonOperator(
        task_id="count_churn_users",
        python_callable=count_churn_users,
    )

start >> count_churn_users_task