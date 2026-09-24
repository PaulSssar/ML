import logging
import pickle
from io import StringIO

import pandas as pd

from scoring.s3_utils import download_bytes_from_s3, upload_bytes_to_s3

# ВАЖНО: Импортируем препроцессор, чтобы pickle мог его найти при загрузке модели
from scoring.preprocessor import TelcomPreprocessor

logger = logging.getLogger(__name__)


def score_and_filter_top_clients(
    s3_conn_id: str,
    model_key: str,
    output_key: str,
    top_n: int = 200,
    input_task_id: str = None,
    id_col: str = "customerID",
    **context,
) -> str:
    """
    Скоринг клиентов с помощью модели из S3.
    
    Args:
        s3_conn_id: Airflow connection ID для S3
        model_key: Путь к модели в S3
        output_key: Путь для сохранения результатов в S3
        top_n: Количество топовых клиентов для выборки
        input_task_id: ID задачи, из которой брать входные данные
        id_col: Название колонки с ID клиента
    
    Returns:
        str: S3 key с результатами
    """
    ti = context["ti"]

    # Получаем input_key из XCom
    # Пробуем разные варианты для совместимости с TaskGroup
    if input_task_id:
        input_key = ti.xcom_pull(task_ids=input_task_id)
        logger.info(f"Попытка получить данные из task_id: {input_task_id}")
    else:
        # Пробуем получить из текущей группы
        input_key = ti.xcom_pull(task_ids="extract_leads")
        logger.info("Получение данных из task_id: extract_leads")
    
    # Если не получилось, пробуем с полным путем
    if not input_key:
        input_key = ti.xcom_pull(task_ids="data_processing.extract_leads")
        logger.info("Получение данных из task_id: data_processing.extract_leads")
    
    # Последняя попытка - получить из предыдущей задачи
    if not input_key:
        input_key = ti.xcom_pull()
        logger.info("Получение данных из предыдущей задачи (xcom_pull без параметров)")
    
    if not input_key:
        raise ValueError(
            f"Не удалось получить input_key из XCom. "
            f"Проверьте task_id: {input_task_id or 'extract_leads'}"
        )

    logger.info("Input raw key from XCom: %s", input_key)

    # Скачиваем сырые данные
    try:
        raw_csv = download_bytes_from_s3(s3_key=input_key, conn_id=s3_conn_id).decode("utf-8")
    except Exception as e:
        logger.error(f"Ошибка при скачивании данных из S3: {str(e)}")
        raise
    
    df = pd.read_csv(StringIO(raw_csv))
    
    if df.empty:
        raise ValueError("Raw CSV пустой — скоринг невозможен.")
    
    logger.info(f"Загружено {len(df)} записей для скоринга")
    logger.info(f"Колонки в данных: {', '.join(df.columns)}")

    # Скачиваем модель
    logger.info(f"Загрузка модели из S3: {model_key}")
    try:
        model_blob = download_bytes_from_s3(s3_key=model_key, conn_id=s3_conn_id)
        artifact = pickle.loads(model_blob)
    except Exception as e:
        logger.error(f"Ошибка при загрузке модели: {str(e)}")
        raise

    # Распаковываем артефакт модели
    if isinstance(artifact, dict):
        model = artifact.get("model")
        preprocess = artifact.get("preprocess")
    elif isinstance(artifact, (tuple, list)) and len(artifact) >= 2:
        model, preprocess = artifact[0], artifact[1]
    else:
        raise ValueError("Непонятный формат model artifact (ожидаем dict или tuple/list).")

    if model is None or preprocess is None:
        raise ValueError("В model artifact не хватает model/preprocess.")

    logger.info("Модель и препроцессор успешно загружены")
    logger.info(f"Тип препроцессора: {type(preprocess).__name__}")

    # Проверяем наличие колонки с ID перед препроцессингом
    if id_col not in df.columns:
        logger.error(f"Колонка '{id_col}' не найдена. Доступные колонки: {', '.join(df.columns)}")
        raise ValueError(
            f"Не нашли колонку '{id_col}'. "
            f"Доступные колонки: {', '.join(df.columns)}"
        )

    # Сохраняем ID для последующего использования
    customer_ids = df[id_col].astype(str)
    
    # Удаляем ID из данных перед препроцессингом
    df_for_scoring = df.drop(id_col, axis=1, errors='ignore')

    # Применяем препроцессинг и делаем предсказания
    try:
        X = preprocess.transform(df_for_scoring)
        proba = model.predict_proba(X)[:, 1]
        logger.info("Скоринг выполнен успешно")
    except Exception as e:
        logger.error(f"Ошибка при скоринге: {str(e)}")
        raise

    # Формируем результат
    out = (
        pd.DataFrame({
            "customer_id": customer_ids, 
            "churn_proba": proba
        })
        .sort_values("churn_proba", ascending=False)
        .head(int(top_n))
        .reset_index(drop=True)
    )

    logger.info(f"Отобрано топ-{len(out)} клиентов с наибольшей вероятностью оттока")
    logger.info(f"Диапазон вероятностей: [{out['churn_proba'].min():.4f}, {out['churn_proba'].max():.4f}]")

    # Сохраняем результат в S3
    buf = StringIO()
    out.to_csv(buf, index=False)

    upload_bytes_to_s3(
        buf.getvalue().encode("utf-8"), 
        s3_key=output_key, 
        conn_id=s3_conn_id
    )
    
    logger.info("Uploaded scored predictions to S3 key: %s", output_key)
    return output_key
