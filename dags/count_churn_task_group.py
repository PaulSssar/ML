from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.task_group import TaskGroup

default_args = {
    "start_date": datetime(2026, 1, 1),
    "retries": 1,
}


def log_churn_count_from_xcom(**context):
    ti = context["ti"]

    rows = ti.xcom_pull(task_ids="cnt_churn_task_group.get_churn_counts")  # return_value
    # rows ожидаем как список кортежей: [(0, cnt0), (1, cnt1)]
    counts = {int(churn): int(cnt) for churn, cnt in rows} if rows else {}

    churned = counts.get(1, 0)
    retained = counts.get(0, 0)

    print(f"Churned users (unique customerID): {churned}")
    print(f"Retained users (unique customerID): {retained}")


def log_dag_duration(start_date, end_date):
    # start_date/end_date сюда приходят уже отрендеренные Jinja-шаблоны
    print(f"DAG start_date: {start_date}")
    print(f"DAG end_date:   {end_date}")


with DAG(
        dag_id="count_churn_users_postgres",
        default_args=default_args,
        schedule=None,
        catchup=False,
        tags=["ml", "postgres", "report"],
) as dag:
    with TaskGroup(group_id="cnt_churn_task_group") as cnt_churn_task_group:
        get_churn_counts = PostgresOperator(
            task_id="get_churn_counts",
            postgres_conn_id="postgres_default",  # Добавьте в поле Connection Id
            sql="""
                -- DAG: {{ dag.dag_id }}
                -- Execution date: {{ ds }}
                SELECT
                    CASE
                        WHEN churn = 'Yes' THEN 1
                        WHEN churn = 'No'  THEN 0
                    END AS churn,
                    COUNT(DISTINCT customerid) AS cnt
                FROM public.telecom_churn
                GROUP BY 1
                ORDER BY 1;
            """,
            do_xcom_push=True,
        )

        log_churn_counts = PythonOperator(
            task_id="log_churn_counts",
            python_callable=log_churn_count_from_xcom,
        )

        get_churn_counts >> log_churn_counts

    log_duration = PythonOperator(
        task_id="log_dag_duration",
        python_callable=log_dag_duration,
        op_kwargs={
            "start_date": "{{ dag_run.start_date }}",
            "end_date": "{{ dag_run.end_date }}",
        },
    )

    cnt_churn_task_group >> log_duration
