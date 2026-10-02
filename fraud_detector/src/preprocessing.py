# Import standard libraries
import json
import logging
import os

import numpy as np
import pandas as pd
from geopy.distance import great_circle

logger = logging.getLogger(__name__)

# Колонки, с которыми работает препроцессинг
TARGET_COL = 'target'
CATEGORICAL_COLS = ['gender', 'merch', 'cat_id', 'one_city', 'us_state', 'jobs']
TIME_COLS = ['hour', 'year', 'month', 'day_of_month', 'day_of_week']
CONTINUOUS_COLS = ['amount', 'population_city', 'distance']
DROP_COLS = ['name_1', 'name_2', 'street', 'post_code']
N_CATS = 50

ARTIFACTS_PATH = os.getenv('PREPROC_ARTIFACTS_PATH', './models/preproc_artifacts.json')


# ---------------------------------------------------------------------------
# Простые преобразования, одинаковые для train и для потока
# ---------------------------------------------------------------------------
def add_time_features(df):
    logger.debug('Adding time features...')
    df['transaction_time'] = pd.to_datetime(df['transaction_time'])
    dt = df['transaction_time'].dt
    df['hour'] = dt.hour
    df['year'] = dt.year
    df['month'] = dt.month
    df['day_of_month'] = dt.day
    df['day_of_week'] = dt.dayofweek
    df.drop(columns='transaction_time', inplace=True)
    return df


def add_distance_features(df):
    logger.debug('Calculating distances...')
    df['distance'] = df.apply(
        lambda x: great_circle(
            (x['lat'], x['lon']),
            (x['merchant_lat'], x['merchant_lon'])
        ).km,
        axis=1
    )
    return df.drop(columns=['lat', 'lon', 'merchant_lat', 'merchant_lon'])


# ---------------------------------------------------------------------------
# Офлайн-часть: считаем по train все статистики, которые нужны на inference.
# Запускается ОДИН раз скриптом scripts/build_artifacts.py, результат
# сохраняется в models/preproc_artifacts.json и кладётся в репозиторий.
# ---------------------------------------------------------------------------
def fit_artifacts(train: pd.DataFrame) -> dict:
    """По сырому train считает таблицы кодирования, mean-encoding и средние для импутации."""
    train = train.drop(columns=DROP_COLS, errors='ignore')
    logger.info('Raw train data. Shape: %s', train.shape)

    train = add_time_features(train)

    # 1. Топ-N категорий: значение -> 'cat_0', 'cat_1', ..., 'cat_50+', 'cat_NAN'
    cat_mappings = {}
    for col in CATEGORICAL_COLS:
        new_col = col + '_cat'
        temp_df = train \
            .groupby(col, dropna=False)[[TARGET_COL]] \
            .count() \
            .sort_values(TARGET_COL, ascending=False) \
            .reset_index() \
            .set_axis([col, 'count'], axis=1) \
            .reset_index()
        temp_df['index'] = temp_df.apply(lambda x: np.nan if pd.isna(x[col]) else x['index'], axis=1)
        temp_df[new_col] = [
            'cat_NAN' if pd.isna(x) else 'cat_' + str(x) if x < N_CATS else f'cat_{N_CATS}+'
            for x in temp_df['index']
        ]
        cat_mappings[col] = temp_df[[col, new_col]].drop_duplicates().reset_index(drop=True)
        train = train.merge(cat_mappings[col], how='left', on=col)

    train = add_distance_features(train)

    # 2. Mean target encoding для закодированных категорий и временных признаков
    mean_tables = {}
    for col in [c + '_cat' for c in CATEGORICAL_COLS] + TIME_COLS:
        mean_tables[col] = train.groupby(col)[[TARGET_COL]].mean() \
            .reset_index().rename(columns={TARGET_COL: f'{col}_mean_enc'})

    # 3. Средние для заполнения пропусков в непрерывных признаках
    impute_means = train[CONTINUOUS_COLS].mean()

    logger.info('Artifacts fitted on train of shape %s', train.shape)
    return {
        'cat_mappings': cat_mappings,
        'mean_tables': mean_tables,
        'impute_means': impute_means,
    }


def _df_to_records(df: pd.DataFrame) -> list:
    # NaN -> None, чтобы получился валидный JSON
    return [
        {k: (None if pd.isna(v) else (v.item() if hasattr(v, 'item') else v)) for k, v in row.items()}
        for row in df.to_dict(orient='records')
    ]


def save_artifacts(artifacts: dict, path: str) -> None:
    """Сохраняет артефакты в JSON: формат не зависит от версий pandas/numpy."""
    payload = {
        'cat_mappings': {col: _df_to_records(df) for col, df in artifacts['cat_mappings'].items()},
        'mean_tables': {col: _df_to_records(df) for col, df in artifacts['mean_tables'].items()},
        'impute_means': {col: float(v) for col, v in artifacts['impute_means'].items()},
    }
    with open(path, 'w', encoding='utf-8') as f:
        json.dump(payload, f, ensure_ascii=False)


def load_artifacts(path: str = ARTIFACTS_PATH) -> dict:
    """Загружается при старте контейнера: маленький файл вместо всего train.csv."""
    logger.info('Loading preprocessing artifacts from %s', path)
    with open(path, encoding='utf-8') as f:
        payload = json.load(f)
    return {
        'cat_mappings': {col: pd.DataFrame(rows) for col, rows in payload['cat_mappings'].items()},
        'mean_tables': {col: pd.DataFrame(rows) for col, rows in payload['mean_tables'].items()},
        'impute_means': pd.Series(payload['impute_means']),
    }


# ---------------------------------------------------------------------------
# Онлайн-часть: препроцессинг входящих транзакций
# ---------------------------------------------------------------------------
def run_preproc(artifacts: dict, input_df: pd.DataFrame) -> pd.DataFrame:
    input_df = input_df.drop(columns=DROP_COLS, errors='ignore')

    # Кодирование категорий в 'cat_k' по таблицам, посчитанным на train
    for col in CATEGORICAL_COLS:
        input_df = input_df.merge(artifacts['cat_mappings'][col], how='left', on=col).drop(columns=col)
    logger.debug('Categorical merging completed. Output shape: %s', input_df.shape)

    input_df = add_time_features(input_df)

    # Mean target encoding
    for col in [c + '_cat' for c in CATEGORICAL_COLS] + TIME_COLS:
        # Неизвестные/пустые категории -> 'cat_NAN'
        input_df[col] = input_df[col].fillna('cat_NAN')
        input_df = input_df.merge(artifacts['mean_tables'][col], how='left', on=col)
    logger.debug('Categorical mean encoding completed. Output shape: %s', input_df.shape)

    input_df = add_distance_features(input_df)

    # Заполнение пропусков средним по train + log-преобразование
    impute_means = artifacts['impute_means']
    output_df = input_df.drop(columns=CONTINUOUS_COLS)
    for col in CONTINUOUS_COLS:
        filled = input_df[col].astype(float).fillna(impute_means[col])
        output_df[col + '_log'] = np.log(filled + 1)

    logger.debug('Preprocessing completed. Output shape: %s', output_df.shape)
    return output_df
