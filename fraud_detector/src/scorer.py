import logging
import os

import pandas as pd
from catboost import CatBoostClassifier

logger = logging.getLogger(__name__)

MODEL_PATH = os.getenv('MODEL_PATH', './models/my_catboost.cbm')
# Порог, выше которого транзакция считается фродом
MODEL_THRESHOLD = float(os.getenv('MODEL_THRESHOLD', '0.98'))

# Категориальные признаки, которые CatBoost ожидает строками
CATEGORICAL_FEATURES = [
    'hour', 'year', 'month', 'day_of_month', 'day_of_week',
    'gender_cat', 'merch_cat', 'cat_id_cat', 'one_city_cat', 'us_state_cat', 'jobs_cat',
]

logger.info('Importing pretrained model from %s...', MODEL_PATH)
model = CatBoostClassifier()
model.load_model(MODEL_PATH)
logger.info('Pretrained model imported successfully')


def make_pred(dt: pd.DataFrame, source_info: str = 'kafka') -> pd.DataFrame:
    dt = dt.copy()
    for col in CATEGORICAL_FEATURES:
        if col in dt.columns:
            dt[col] = dt[col].astype(str)

    # Подаём признаки в том порядке, в котором модель обучалась
    # (если модель знает имена признаков и все они есть во входе)
    if set(model.feature_names_) <= set(dt.columns):
        dt = dt[model.feature_names_]

    scores = model.predict_proba(dt)[:, 1]
    submission = pd.DataFrame({
        'score': scores,
        'fraud_flag': (scores > MODEL_THRESHOLD).astype(int),
    })
    logger.debug('Prediction complete for data from %s', source_info)
    return submission
