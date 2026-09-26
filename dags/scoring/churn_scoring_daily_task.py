from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup

from scoring.s3_utils import check_model_on_s3
from scoring.db_operations import extract_leads_from_postgres, load_scored_to_postgres
from scoring.score import score_and_filter_top_clients


default_args = {
    "owner": "data-science-team",
    "depends_on_past": False,
    "email_on_failure": True,
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

RAW_S3_KEY = "data/raw/churn_leads_{{ ds_nodash }}.csv"
SCORED_S3_KEY = "data/scored/churn_predictions_{{ ds_nodash }}.csv"


with DAG(
    dag_id="churn_scoring_daily",
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
        extract_task >> score_task >> load_task

    # Связь: сначала проверка модели, потом группа обработки
    check_model_task >> data_processing_group