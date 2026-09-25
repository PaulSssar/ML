"""
Препроцессор для данных телеком.
Этот файл должен быть размещен в plugins/scoring/preprocessor.py
"""

import pandas as pd
import numpy as np
from sklearn.base import BaseEstimator, TransformerMixin
from sklearn.preprocessing import LabelEncoder


import logging
logger = logging.getLogger(__name__)


class TelcomPreprocessor(BaseEstimator, TransformerMixin):
    """
    Простой препроцессор для данных telcom_churn.
    Обрабатывает категориальные и числовые признаки.
    """
    
    def __init__(self):
        self.label_encoders = {}
        self.feature_names = None
        self.categorical_features = []
        self.numerical_features = []
        
    def fit(self, X, y=None):
        """Обучение препроцессора на данных"""
        
        # Создаем копию для безопасности
        df = X.copy()
        
        # Приводим названия колонок к нижнему регистру для унификации
        df.columns = df.columns.str.lower()

        # Определяем типы признаков
        # Категориальные признаки
        self.categorical_features = [
            'gender', 'partner', 'dependents', 'phoneservice',
            'multiplelines', 'internetservice', 'onlinesecurity',
            'onlinebackup', 'deviceprotection', 'techsupport',
            'streamingtv', 'streamingmovies', 'contract',
            'paperlessbilling', 'paymentmethod'
        ]
        
        # Числовые признаки
        self.numerical_features = [
            'seniorcitizen', 'tenure', 'monthlycharges', 'totalcharges'
        ]
        
        # Обучаем LabelEncoder для каждого категориального признака
        for col in self.categorical_features:
            if col in df.columns:
                self.label_encoders[col] = LabelEncoder()
                # Заполняем пропуски перед обучением
                df[col] = df[col].fillna('Unknown')
                self.label_encoders[col].fit(df[col].astype(str))
        
        # Сохраняем список всех признаков
        self.feature_names = self.categorical_features + self.numerical_features
        
        return self
    
    def transform(self, X):
        """Трансформация данных"""
        
        df = X.copy()
        
        # Создаем отображение: имя_в_нижнем_регистре -> оригинальное_имя_в_данных
        original_columns = {col.lower(): col for col in df.columns}

        # Приводим названия колонок к нижнему регистру для обработки
        df.columns = df.columns.str.lower()

        # Приводим имена признаков к нижнему регистру для поиска в данных
        # (feature_names может быть в любом регистре в зависимости от того, как обучали)
        cat_features_lower = [col.lower() for col in self.categorical_features]
        num_features_lower = [col.lower() for col in self.numerical_features]
        feature_names_lower = [col.lower() for col in self.feature_names]

        # Обрабатываем категориальные признаки
        for i, col in enumerate(cat_features_lower):
            if col in df.columns:
                # Заполняем пропуски
                df[col] = df[col].fillna('Unknown')
                
                # Применяем LabelEncoder (ищем по оригинальному имени)
                orig_col_name = self.categorical_features[i]
                if orig_col_name in self.label_encoders:
                    # Обрабатываем новые категории
                    df[col] = df[col].astype(str).apply(
                        lambda x: x if x in self.label_encoders[orig_col_name].classes_ else 'Unknown'
                    )
                    df[col] = self.label_encoders[orig_col_name].transform(df[col])
                else:
                    df[col] = 0
        
        # Обрабатываем числовые признаки
        for col in num_features_lower:
            if col in df.columns:
                # Конвертируем TotalCharges в числовой формат
                if col == 'totalcharges':
                    df[col] = pd.to_numeric(df[col], errors='coerce')
                
                # Заполняем пропуски
                df[col] = df[col].fillna(0)
            else:
                df[col] = 0
        
        # Возвращаем только нужные признаки в правильном порядке (поиск по нижнему регистру)
        logger.info(f"Колонки в данных: {', '.join(df.columns)}")
        logger.info(f"Колонки в классе: {self.feature_names}")

        return df[feature_names_lower].values
