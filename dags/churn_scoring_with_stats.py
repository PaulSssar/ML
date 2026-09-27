from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.task_group import TaskGroup

from scoring.s3_utils import check_model_on_s3, download_bytes_from_s3
from scoring.db_operations import extract_leads_from_postgres, load_scored_to_postgres
from scoring.score import score_and_filter_top_clients

default_args = {
    "owner": "data-science-team",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

S3_CONN_ID = "s3_default"
DB_CONN_ID = "postgres_default"

S3_BUCKET = Variable.get("churn_s3_bucket", default_var="s3-ds-20251009-0d49661b2c")
S3_MODEL_KEY = Variable.get("churn_model_key", default_var="model/telcom_catboost_pipeline.pkl")

LEADS_LIMIT = int(Variable.get("churn_leads_limit", default_var="1000"))
TOP_N_CLIENTS = int(Variable.get("churn_top_n", default_var="100"))
TARGET_TABLE = Variable.get("churn_res_table", default_var="public.telcom_churn_res")
STATS_TABLE = Variable.get("churn_stats_table", default_var="public.churn_scoring_stats")

RAW_S3_KEY = "data/raw/churn_leads_{{ ds_nodash }}.csv"
SCORED_S3_KEY = "data/scored/churn_predictions_{{ ds_nodash }}.csv"


def calculate_and_prepare_stats(
        s3_conn_id: str,
        input_task_id: str = None,
        **context,
) -> dict:
    """
    Вычисляет статистику скоринга и возвращает данные для вставки.

    Returns:
        dict: Статистика для вставки в таблицу через XCom
    """
    import logging
    import pandas as pd
    from io import StringIO

    logger = logging.getLogger(__name__)
    ti = context["ti"]
    execution_date = context.get("ds")

    # Получаем S3 key из XCom: перебираем кандидатов по приоритету,
    # 1. input_task_id — если передан явно через параметр функции
    # 2. "score_and_filter" — короткое имя задачи
    # 3. "data_processing.score_and_filter" — полный путь с именем группы
    candidate_task_ids = [
        input_task_id,
        "score_and_filter",
        "data_processing.score_and_filter",
    ]
    # Перебираем кандидатов по очереди и берём первый, у которого xcom_pull вернул значение.
    s3_key = next(
        (ti.xcom_pull(task_ids=tid) for tid in candidate_task_ids if tid and ti.xcom_pull(task_ids=tid)),
        ti.xcom_pull(),
    )
    # Иначе выдаём ошибку
    if not s3_key:
        raise ValueError("Не удалось получить S3 key из XCom")

    logger.info(f"Расчёт статистики из: {s3_key}")

    # Скачиваем scored CSV из S3
    scored_csv = download_bytes_from_s3(s3_key=s3_key, conn_id=s3_conn_id).decode("utf-8")
    df = pd.read_csv(StringIO(scored_csv))

    if df.empty:
        raise ValueError("Scored CSV пустой — нет данных для расчёта статистики")

    # Вычисляем простую статистику
    stats = {
        "scoring_date": execution_date,
        "total_clients": int(len(df)),
        "avg_churn_proba": float(df["churn_proba"].mean()),
        "min_churn_proba": float(df["churn_proba"].min()),
        "max_churn_proba": float(df["churn_proba"].max()),
    }

    logger.info(f"Клиентов: {stats['total_clients']}, "
                f"Avg: {stats['avg_churn_proba']:.4f}, "
                f"Min: {stats['min_churn_proba']:.4f}, "
                f"Max: {stats['max_churn_proba']:.4f}")

    return stats


with DAG(
        dag_id="churn_scoring_with_stats",
        default_args=default_args,
        description="Ежедневный скоринг клиентов моделью с выгрузкой в БД",
        schedule_interval=None,
        start_date=datetime(2024, 1, 1),
        catchup=False,
        tags=["ml", "scoring", "churn", "daily"],
        max_active_runs=1,
) as dag:
    # 1) Проверяем, что модель существует на S3
    check_model_task = PythonOperator(
        task_id="check_model_exists",
        python_callable=check_model_on_s3,
        op_kwargs={
            "model_key": S3_MODEL_KEY,
            "conn_id": S3_CONN_ID,
        },
    )

    # 2) Основной пайплайн в группе
    with TaskGroup(group_id="data_processing") as data_processing_group:
        # Извлекаем данные из Postgres и сохраняем в S3
        extract_task = PythonOperator(
            task_id="extract_leads",
            python_callable=extract_leads_from_postgres,
            op_kwargs={
                "postgres_conn_id": DB_CONN_ID,
                "s3_conn_id": S3_CONN_ID,
                "s3_key": RAW_S3_KEY,
                "limit": LEADS_LIMIT,
            },
        )

        # Выполняем скоринг и фильтруем топ клиентов
        score_task = PythonOperator(
            task_id="score_and_filter",
            python_callable=score_and_filter_top_clients,
            op_kwargs={
                "s3_conn_id": S3_CONN_ID,
                "model_key": S3_MODEL_KEY,
                "output_key": SCORED_S3_KEY,
                "top_n": TOP_N_CLIENTS,
            },
        )

        # Вычисляем статистику (возвращает данные в XCom)
        calc_stats_task = PythonOperator(
            task_id="calculate_statistics",
            python_callable=calculate_and_prepare_stats,
            op_kwargs={
                "s3_conn_id": S3_CONN_ID,
            },
        )

        # Удаляем старые записи за дату и вставляем новую статистику
        upsert_stats = PostgresOperator(
            task_id="upsert_statistics",
            postgres_conn_id=DB_CONN_ID,
            sql=f"""
                {{% set stats_task_id = task.upstream_task_ids | list | first %}}
                {{% set stats = ti.xcom_pull(task_ids=stats_task_id) %}}
                DELETE FROM {STATS_TABLE}
                WHERE scoring_date = '{{{{ ds }}}}';

                INSERT INTO {STATS_TABLE} (scoring_date, total_clients, avg_churn_proba, min_churn_proba, max_churn_proba)
                VALUES (
                    '{{{{ ds }}}}',
                    {{{{ stats['total_clients'] }}}},
                    {{{{ stats['avg_churn_proba'] }}}},
                    {{{{ stats['min_churn_proba'] }}}},
                    {{{{ stats['max_churn_proba'] }}}}
                );
            """,
        )

        # Загружаем результаты в Postgres
        load_task = PythonOperator(
            task_id="load_to_postgres",
            python_callable=load_scored_to_postgres,
            op_kwargs={
                "postgres_conn_id": DB_CONN_ID,
                "s3_conn_id": S3_CONN_ID,
                "target_table": TARGET_TABLE,
            },
        )

        # Цепочка выполнения внутри группы
        extract_task >> score_task >> calc_stats_task >> upsert_stats
        score_task >> load_task

    # Связь: сначала проверка модели, потом группа обработки
    check_model_task >> data_processing_group