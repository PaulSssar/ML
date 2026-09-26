import io
from datetime import datetime

import boto3
import pandas as pd
from airflow import DAG
from airflow.hooks.base import BaseHook
from airflow.operators.python import PythonOperator

# Connection ID, который вы задали в Connection
S3_CONN_ID = "s3_default"

# Функция для подключения к S3
def _get_s3_client_and_bucket(conn_id: str = S3_CONN_ID):
# BaseHook забирает параметры подключения из Airflow Connection
    conn = BaseHook.get_connection(conn_id)
    extra = conn.extra_dejson or {}

# Добавляет endpoint и bucket из поля Extra
    endpoint_url = extra.get("endpoint_url")
    bucket = extra.get("bucket")

    if not endpoint_url or not bucket:
        raise ValueError(
            "В Airflow Connection (Extra) должны быть endpoint_url и bucket. "
            "Пример Extra: {'endpoint_url': 'https://storage.yandexcloud.net', 'bucket': 'my-bucket'}"
        )

# Создаёт S3-клиент через boto3
# Использует креды из Airflow Connection
    s3 = boto3.client(
        "s3",
        aws_access_key_id=conn.login,
        aws_secret_access_key=conn.password,
        endpoint_url=endpoint_url
    )
    return s3, bucket


def task_upload_raw_to_s3(**context):
    s3, bucket = _get_s3_client_and_bucket()

# Берёт из контекста дату запуска DAG в строковом формате (ds)
    ds = context["ds"]
# Формирует имя файла
    raw_key = f"data_raw_{ds}.csv"

    # Небольшой пример данных
    df = pd.DataFrame(
        {
            "user_id": [1987575, 19485657, 19483625],
            "churn_proba": [0.12, 0.87, 0.34],
            "dt": [ds, ds, ds],
        }
    )

# Создаёт временный буфер в памяти для записи файла без сохранения на диск
    buf = io.StringIO()
    df.to_csv(buf, index=False)
# Берёт CSV из памяти buf и сохраняет его как объект в S3 под указанным именем
    s3.put_object(Bucket=bucket, Key=raw_key, Body=buf.getvalue().encode("utf-8"))

    # Передаёт путь к файлу в XCom для следующей задачи
    return raw_key


def task_download_process_upload(**context):
# Получает из контекста Airflow объект текущей задачи (TaskInstance)
    ti = context["ti"]
# Забирает то, что вернула задача upload_raw_to_s3
    raw_key = ti.xcom_pull(task_ids="upload_raw_to_s3")

    if not raw_key:
        raise ValueError("Не найден raw_key в XCom. Проверь, что Task A вернула ключ.")

    s3, bucket = _get_s3_client_and_bucket()
# Обращается к S3 и загружает объект из бакета по указанному ключу
    obj = s3.get_object(Bucket=bucket, Key=raw_key)
# Читает содержимое файла из S3 в виде байтов и загружает его в pandas DataFrame.
    csv_bytes = obj["Body"].read()
    df = pd.read_csv(io.BytesIO(csv_bytes))

    # Простая обработка: добавим колонку-метку и отфильтруем «горячих» клиентов
    df["is_hot"] = df["churn_proba"] >= 0.7
    df_hot = df[df["is_hot"]].copy()

    ds = context["ds"]
    processed_key = f"data_processed_{ds}.csv"

    out = io.StringIO()
    df_hot.to_csv(out, index=False)
    s3.put_object(Bucket=bucket, Key=processed_key, Body=out.getvalue().encode("utf-8"))

    return processed_key


with DAG(
    dag_id="S3_dag",
    start_date=datetime(2026, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["xcom", "s3"],
) as dag:
    upload_raw_to_s3 = PythonOperator(
        task_id="upload_raw_to_s3",
        python_callable=task_upload_raw_to_s3,
    )

    download_process_upload = PythonOperator(
        task_id="download_process_upload",
        python_callable=task_download_process_upload,
    )

    upload_raw_to_s3 >> download_process_upload