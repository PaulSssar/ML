"""
Препроцессор для данных телеком.
Этот файл должен быть размещен в plugins/scoring/preprocessor.py
"""

import pandas as pd
import numpy as np
from sklearn.base import BaseEstimator, TransformerMixin
from sklearn.preprocessing import LabelEncoder


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
        
        # Определяем типы признаков
        # Категориальные признаки
        self.categorical_features = [
            'gender', 'Partner', 'Dependents', 'PhoneService', 
            'MultipleLines', 'InternetService', 'OnlineSecurity',
            'OnlineBackup', 'DeviceProtection', 'TechSupport',
            'StreamingTV', 'StreamingMovies', 'Contract',
            'PaperlessBilling', 'PaymentMethod'
        ]
        
        # Числовые признаки
        self.numerical_features = [
            'SeniorCitizen', 'tenure', 'MonthlyCharges', 'TotalCharges'
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
        
        # Обрабатываем категориальные признаки
        for col in self.categorical_features:
            if col in df.columns:
                # Заполняем пропуски
                df[col] = df[col].fillna('Unknown')
                
                # Применяем LabelEncoder
                if col in self.label_encoders:
                    # Обрабатываем новые категории
                    df[col] = df[col].astype(str).apply(
                        lambda x: x if x in self.label_encoders[col].classes_ else 'Unknown'
                    )
                    df[col] = self.label_encoders[col].transform(df[col])
                else:
                    df[col] = 0
        
        # Обрабатываем числовые признаки
        for col in self.numerical_features:
            if col in df.columns:
                # Конвертируем TotalCharges в числовой формат
                if col == 'TotalCharges':
                    df[col] = pd.to_numeric(df[col], errors='coerce')
                
                # Заполняем пропуски медианой (или 0)
                df[col] = df[col].fillna(0)
            else:
                df[col] = 0
        
        # Возвращаем только нужные признаки в правильном порядке
        result = df[self.feature_names].values
        
        return result
