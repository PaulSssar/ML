# Airflow Churn Scoring Pipeline

Ежедневный пайплайн для скоринга клиентов моделью CatBoost.

## 📁 Структура проекта

```
airflow-churn-scoring/
├── dags/
│   └── lesson7_scoring_churn_user.py           # Главный DAG урок 7
│   └── lesson8_scoring_churn_user_agg_dag.py   # Главный DAG урок 8
├── plugins/scoring/
│   ├── preprocessor.py                         # Предобработка признаков
│   ├── s3_utils.py                             # Работа с S3
│   ├── db_operations.py                        # Работа с PostgreSQL
│   └── score.py                                # ML скоринг
├── config/
│   └── airflow_variables.json                  # Переменные
└── requirements.txt                            # Зависимости
```

## 🎯 Что делает DAG

1. **Проверяет модель** на S3
2. **Извлекает 1000 клиентов** из PostgreSQL
3. **Скорит** их моделью CatBoost
4. **Фильтрует топ-100** с наибольшей вероятностью ухода
5. **Сохраняет промежуточные результаты** на S3
6. ** Выгружает в витрину** в PostgreSQL

## 📊 Результаты

- **Исходные данные:** `s3://bucket/data/raw/churn_leads_YYYYMMDD.csv` (1000 записей)
- **Результаты:** `s3://bucket/data/scored/churn_predictions_YYYYMMDD.csv` (100 записей)
