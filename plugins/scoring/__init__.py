"""
Scoring package
Модули для скоринга клиентов моделью CatBoost
"""

from scoring.s3_utils import check_model_on_s3, upload_to_s3, download_from_s3
from scoring.db_operations import extract_leads_from_postgres
from scoring.score import score_and_filter_top_clients
from scoring.load_to_postgres import load_s3_to_postgres, download_s3_and_prepare_sql

__all__ = [
    'check_model_on_s3',
    'upload_to_s3',
    'download_from_s3',
    'extract_leads_from_postgres',
    'score_and_filter_top_clients',
    'load_s3_to_postgres',
    'download_s3_and_prepare_sql',
]
