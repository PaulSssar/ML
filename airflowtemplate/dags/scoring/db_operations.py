"""
Операции с PostgreSQL: извлечение данных и загрузка результатов.

Данные в XCom: S3 key (путь к файлу) или количество записей.
"""

import logging
from io import StringIO

import pandas as pd
from airflow.providers.postgres.hooks.postgres import PostgresHook

from scoring.s3_utils import upload_bytes_to_s3, download_bytes_from_s3

logger = logging.getLogger(__name__)


def extract_leads_from_postgres(
    postgres_conn_id: str,
    s3_conn_id: str,
    s3_key: str,
    limit: int = 1000,
    **context,
) -> str:
    """
    1) Берём данные из Postgres
    2) Сохраняем CSV в S3
    3) Возвращаем s3_key (в XCom)
    """

    hook = PostgresHook(postgres_conn_id=postgres_conn_id)

    sql = f"""
        SELECT *
        FROM public.telecom_churn
        LIMIT {int(limit)}
    """

    logger.info("Extract leads from Postgres (limit=%s)", limit)
    df = hook.get_pandas_df(sql)

    if df.empty:
        raise ValueError("Из Postgres вернулся пустой датасет")

    csv_buf = StringIO()
    df.to_csv(csv_buf, index=False)
    upload_bytes_to_s3(csv_buf.getvalue().encode("utf-8"), s3_key=s3_key, conn_id=s3_conn_id)

    logger.info("Uploaded raw leads to S3 key: %s", s3_key)
    return s3_key


def load_scored_to_postgres(
    postgres_conn_id: str,
    s3_conn_id: str,
    target_table: str,
    input_task_id: str = None,
    **context,
) -> int:
    """
    1) Скачиваем scored CSV из S3
    2) Удаляем записи за текущую дату (если есть)
    3) Загружаем новые данные с датой
    4) Возвращаем количество загруженных записей
    
    Args:
        postgres_conn_id: Airflow connection ID для Postgres
        s3_conn_id: Airflow connection ID для S3
        target_table: Целевая таблица (например, "public.telcom_churn_res")
    
    Returns:
        int: Количество загруженных записей
    """
    ti = context["ti"]
    execution_date = context.get("ds")  # Формат: YYYY-MM-DD
    
    logger.info(f"Загрузка результатов скоринга за дату: {execution_date}")
    
    # Получаем S3 key из XCom
    if input_task_id:
        s3_key = ti.xcom_pull(task_ids=input_task_id)
        logger.info(f"Попытка получить S3 key из task_id: {input_task_id}")
    else:
        # Пробуем разные варианты (как в score.py)
        s3_key = ti.xcom_pull(task_ids="score_and_filter")
        logger.info("Получение S3 key из task_id: score_and_filter")
    
    if not s3_key:
        s3_key = ti.xcom_pull(task_ids="data_processing.score_and_filter")
        logger.info("Получение S3 key из task_id: data_processing.score_and_filter")
    
    if not s3_key:
        s3_key = ti.xcom_pull()
        logger.info("Получение S3 key из предыдущей задачи")
    
    if not s3_key:
        raise ValueError("Не удалось получить S3 key из XCom")
    
    logger.info(f"S3 key с результатами скоринга: {s3_key}")
    
    # Скачиваем scored CSV из S3
    scored_csv = download_bytes_from_s3(s3_key=s3_key, conn_id=s3_conn_id).decode("utf-8")
    df = pd.read_csv(StringIO(scored_csv))
    
    if df.empty:
        raise ValueError("Scored CSV пустой — загрузка невозможна")
    
    logger.info(f"Загружено {len(df)} записей из S3")
    
    # Добавляем колонку с датой скоринга
    df["scoring_date"] = execution_date
    
    # Проверяем наличие необходимых колонок
    required_cols = ["customer_id", "churn_proba"]
    missing_cols = [col for col in required_cols if col not in df.columns]
    if missing_cols:
        raise ValueError(f"В данных отсутствуют колонки: {missing_cols}")
    
    logger.info(f"Колонки для загрузки: {', '.join(df.columns)}")
    
    # Подключаемся к Postgres
    hook = PostgresHook(postgres_conn_id=postgres_conn_id)
    
    # 1) Удаляем существующие записи за текущую дату
    delete_sql = f"""
        DELETE FROM {target_table}
        WHERE scoring_date = %s
    """
    
    logger.info(f"Удаление старых записей за дату {execution_date}")
    deleted_rows = hook.run(delete_sql, parameters=(execution_date,))
    logger.info(f"Удалено записей: {deleted_rows if deleted_rows else 0}")
    
    # 2) Создаем таблицу если её нет
    create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {target_table} (
            customer_id VARCHAR(255) NOT NULL,
            churn_proba DECIMAL(10, 8) NOT NULL,
            scoring_date DATE NOT NULL,
            PRIMARY KEY (customer_id, scoring_date)
        )
    """
    
    logger.info(f"Создание таблицы {target_table} (если не существует)")
    hook.run(create_table_sql)
    
    # 3) Загружаем данные
    rows = df[["customer_id", "churn_proba", "scoring_date"]].values.tolist()
    
    logger.info(f"Загрузка {len(rows)} записей в {target_table}")
    
    # Вставляем данные через executemany для производительности
    insert_sql = f"""
        INSERT INTO {target_table} (customer_id, churn_proba, scoring_date)
        VALUES (%s, %s, %s)
    """
    
    conn = hook.get_conn()
    cursor = conn.cursor()
    
    try:
        cursor.executemany(insert_sql, rows)
        conn.commit()
        logger.info(f"✓ Успешно загружено {len(rows)} записей")
    except Exception as e:
        conn.rollback()
        logger.error(f"Ошибка при загрузке данных: {str(e)}")
        raise
    finally:
        cursor.close()
        conn.close()
    
    # Проверяем количество записей за дату
    check_sql = f"""
        SELECT COUNT(*) as cnt
        FROM {target_table}
        WHERE scoring_date = %s
    """
    
    result = hook.get_first(check_sql, parameters=(execution_date,))
    count_in_db = result[0] if result else 0
    
    logger.info(f"Записей в БД за {execution_date}: {count_in_db}")
    
    return len(rows)
