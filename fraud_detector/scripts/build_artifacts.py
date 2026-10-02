"""
Офлайн-скрипт: один раз считает по train.csv статистики для препроцессинга
и сохраняет их в models/preproc_artifacts.json.

Запуск (из папки fraud_detector):
    python scripts/build_artifacts.py --train train_data/train.csv

Сервису скоринга train.csv после этого не нужен.
"""
import argparse
import logging
import os
import sys

import pandas as pd

sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'src'))
from preprocessing import fit_artifacts, save_artifacts  # noqa: E402

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--train', default='train_data/train.csv')
    parser.add_argument('--out', default='models/preproc_artifacts.json')
    args = parser.parse_args()

    train = pd.read_csv(args.train)
    artifacts = fit_artifacts(train)
    save_artifacts(artifacts, args.out)
    print(f'Saved artifacts to {args.out} ({os.path.getsize(args.out) / 1024:.1f} KB)')
