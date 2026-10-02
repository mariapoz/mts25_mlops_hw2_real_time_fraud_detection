# Real-Time Fraud Detection System

Сервис для скоринга мошеннических транзакций в реальном времени: транзакции приходят потоком через Kafka, ML-модель (CatBoost, CPU) считает скор и флаг фрода, результаты сохраняются в PostgreSQL и отображаются в UI.

Датасеты: соревнование [teta-ml-1-2025](https://www.kaggle.com/competitions/teta-ml-1-2025). Основа проекта: код семинара MLOps МТС ШАД 2025.

## Архитектура

```
 interface (Streamlit)
   │  строки test.csv → JSON
   ▼
 Kafka: топик transactions
   │
   ▼
 fraud_detector ── препроцессинг → CatBoost (только inference, CPU)
   │  {transaction_id, score, fraud_flag}
   ▼
 Kafka: топик scores
   │
   ▼
 db_writer ──► PostgreSQL: таблица scores
                    ▲
 interface, страница «Результаты» ── кнопка «Посмотреть результаты»
```

| Сервис | Что делает | Порт на хосте |
|---|---|---|
| `zookeeper`, `kafka` | брокер сообщений | 9095 (Kafka для хоста) |
| `kafka-setup` | создаёт топики `transactions` и `scores` и завершается | — |
| `kafka-ui` | веб-интерфейс для просмотра топиков | 8080 |
| `fraud_detector` | читает `transactions`, скорит, пишет в `scores` | — |
| `postgres` | база с витриной `scores` | 5433 |
| `db_writer` | читает `scores` и пишет в Postgres | — |
| `interface` | загрузка CSV в Kafka и страница результатов | 8501 |

### Сервис скоринга (`fraud_detector`)

Этапы разнесены по отдельным скриптам:

- `app/app.py`: чтение сообщений из топика `transactions` и запись результата в топик `scores`.
- `src/preprocessing.py`: препроцессинг: временные признаки, расстояние клиент–мерчант, кодирование топ-50 категорий, mean target encoding, заполнение пропусков и log-преобразование.
- `src/scorer.py`: загрузка модели `models/my_catboost.cbm` и скоринг. Если `score > 0.98`, то `fraud_flag = 1`.

Сервис делает только inference. Все статистики, которые препроцессинг берёт из обучающей выборки (таблицы кодирования категорий, средние таргета, средние для импутации), посчитаны заранее скриптом `scripts/build_artifacts.py` и лежат в `models/preproc_artifacts.json`. Поэтому контейнеру не нужен `train.csv`.

### Формат сообщений

Топик `transactions`:
```json
{"transaction_id": "d6b0f7a0-...", "data": {"transaction_time": "2023-01-01 12:30:00", "amount": 150.5, "...": "..."}}
```

Топик `scores`:
```json
{"transaction_id": "d6b0f7a0-...", "score": 0.0123, "fraud_flag": 0}
```

### Витрина в Postgres

Таблица `scores` создаётся скриптом `postgres/init.sql` при первом старте базы:

| Колонка | Тип |
|---|---|
| `id` | BIGSERIAL, PK |
| `transaction_id` | TEXT, UNIQUE |
| `score` | DOUBLE PRECISION |
| `fraud_flag` | SMALLINT |
| `created_at` | TIMESTAMPTZ, default now() |

## Запуск

Требования: Docker 20.10+ и Docker Compose v2. На хосте должны быть свободны порты 8080, 8501, 9095 и 5433.

```bash
git clone https://github.com/mariapoz/mts25_mlops_hw2_real_time_fraud_detection.git
cd mts25_mlops_hw2_real_time_fraud_detection

docker compose up --build -d
docker compose ps
```

Первый запуск занимает несколько минут: скачиваются образы и собираются сервисы. Когда всё поднялось, `kafka-setup` будет в статусе `exited (0)`, а остальные сервисы в статусе `running`.

После запуска:
- Streamlit UI: http://localhost:8501
- Kafka UI: http://localhost:8080

## Проверка работоспособности

1. Откройте http://localhost:8501 и загрузите `test.csv` из соревнования. Для быстрой проверки удобно взять первые 100–200 строк: `head -n 201 test.csv > test_sample.csv`.
2. Нажмите «Отправить <имя файла>».
3. В Kafka UI (http://localhost:8080 → Topics) в топике `transactions` появятся входящие сообщения, а в топике `scores` результаты скоринга.
4. В Streamlit откройте в боковом меню страницу **«📊 Результаты»** и нажмите **«Посмотреть результаты»**. Появятся последние 10 транзакций с `fraud_flag = 1` (если такие есть) и гистограмма скоров последних 100 транзакций.

Логи и база:
```bash
docker compose logs -f fraud_detector   # скоринг каждой транзакции
docker compose logs -f db_writer        # запись в Postgres

docker compose exec postgres psql -U fraud -d fraud \
  -c "SELECT count(*), sum(fraud_flag) FROM scores;"
```

Остановка:
```bash
docker compose down        # остановить
docker compose down -v     # остановить и удалить данные Postgres
```

## Пересборка артефактов препроцессинга (необязательно)

Нужна только при смене обучающей выборки:
```bash
cd fraud_detector
mkdir -p train_data && cp /path/to/train.csv train_data/
python scripts/build_artifacts.py --train train_data/train.csv --out models/preproc_artifacts.json
```

## Структура проекта

```
.
├── docker-compose.yaml
├── postgres/
│   └── init.sql                  # создание витрины scores
├── fraud_detector/
│   ├── app/app.py                # Kafka consumer/producer
│   ├── src/preprocessing.py      # препроцессинг
│   ├── src/scorer.py             # загрузка модели и скоринг
│   ├── scripts/build_artifacts.py# офлайн-расчёт статистик по train
│   ├── models/
│   │   ├── my_catboost.cbm       # модель
│   │   └── preproc_artifacts.json # статистики для препроцессинга
│   ├── requirements.txt
│   └── Dockerfile
├── db_writer/
│   ├── app.py                    # Kafka (scores) → Postgres
│   ├── requirements.txt
│   └── Dockerfile
└── interface/
    ├── app.py                    # загрузка CSV в Kafka
    ├── pages/1_📊_Результаты.py  # результаты из Postgres
    ├── requirements.txt
    └── Dockerfile
```
