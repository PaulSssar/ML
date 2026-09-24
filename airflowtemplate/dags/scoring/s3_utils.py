# plugins/scoring/s3_utils.py

import logging
from typing import Tuple, Optional

import boto3
from botocore.exceptions import ClientError
from airflow.hooks.base import BaseHook

logger = logging.getLogger(__name__)


def _get_s3_client_and_bucket(conn_id: str) -> Tuple[object, str]:
    conn = BaseHook.get_connection(conn_id)
    extra = conn.extra_dejson or {}

    endpoint_url = extra.get("endpoint_url")
    bucket = extra.get("bucket")

    if not endpoint_url or not bucket:
        raise ValueError(
            "В Airflow Connection (Extra) должны быть endpoint_url и bucket. "
            "Пример Extra: {'endpoint_url': 'https://storage.yandexcloud.net', 'bucket': 'my-bucket'}"
        )

    s3 = boto3.client(
        "s3",
        aws_access_key_id=conn.login,
        aws_secret_access_key=conn.password,
        endpoint_url=endpoint_url,
    )
    return s3, bucket


def check_model_on_s3(model_key: str, conn_id: str) -> bool:
    logger.info("Проверка наличия модели на S3: key=%s, conn_id=%s", model_key, conn_id)
    s3, bucket = _get_s3_client_and_bucket(conn_id)

    try:
        s3.head_object(Bucket=bucket, Key=model_key)
        logger.info("✓ Модель найдена: s3://%s/%s", bucket, model_key)
        return True
    except ClientError as e:
        code = str(e.response.get("Error", {}).get("Code", ""))
        if code in ("404", "NoSuchKey", "NotFound"):
            msg = f"✗ Модель НЕ найдена: s3://{bucket}/{model_key}"
            logger.error(msg)
            raise FileNotFoundError(msg)
        logger.exception("Ошибка при head_object (S3). Code=%s", code)
        raise


def upload_bytes_to_s3(data: bytes, s3_key: str, conn_id: str, bucket: Optional[str] = None) -> str:
    """
    Загрузка bytes в S3 по ключу.
    Возвращает s3_key (для XCom).
    """
    s3, bucket_from_conn = _get_s3_client_and_bucket(conn_id)
    bucket = bucket or bucket_from_conn

    logger.info("Загрузка на S3: s3://%s/%s (size=%s bytes)", bucket, s3_key, len(data))

    s3.put_object(Bucket=bucket, Key=s3_key, Body=data)

    logger.info("✓ Uploaded: s3://%s/%s", bucket, s3_key)
    return s3_key

def download_bytes_from_s3(s3_key: str, conn_id: str, bucket: str | None = None) -> bytes:
    """
    Скачивание bytes с S3 по ключу.
    Returns: Байты скачанного файла
    """
    try:
        s3, bucket_from_conn = _get_s3_client_and_bucket(conn_id)
        bucket = bucket or bucket_from_conn
        logger.info(f"Скачивание с S3: s3://{bucket}/{s3_key}")

        # Скачиваем файл
        obj = s3.get_object(Bucket=bucket, Key=s3_key)
        return obj["Body"].read()
    except Exception as e:
        logger.error(f"Ошибка при скачивании с S3: {str(e)}")
        raise