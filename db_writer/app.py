"""
Сервис-писатель: читает результаты скоринга из топика Kafka `scores`
и складывает их в таблицу `scores` в Postgres.

Формат входящего сообщения:
    {"transaction_id": "...", "score": 0.0123, "fraud_flag": 0}
"""
import json
import logging
import os
import time

import psycopg2
from confluent_kafka import Consumer

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger("db_writer")

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
SCORES_TOPIC = os.getenv("KAFKA_SCORES_TOPIC", "scores")

PG_DSN = {
    "host": os.getenv("POSTGRES_HOST", "postgres"),
    "port": int(os.getenv("POSTGRES_PORT", "5432")),
    "dbname": os.getenv("POSTGRES_DB", "fraud"),
    "user": os.getenv("POSTGRES_USER", "fraud"),
    "password": os.getenv("POSTGRES_PASSWORD", "fraud"),
}

# Дублируем DDL из postgres/init.sql: если volume с базой уже существовал
# до появления init.sql, таблица всё равно будет создана.
CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS scores (
    id             BIGSERIAL PRIMARY KEY,
    transaction_id TEXT             NOT NULL UNIQUE,
    score          DOUBLE PRECISION NOT NULL,
    fraud_flag     SMALLINT         NOT NULL,
    created_at     TIMESTAMPTZ      NOT NULL DEFAULT now()
);
"""

# ON CONFLICT DO NOTHING делает запись идемпотентной: если сообщение
# прочитается повторно (например, после рестарта), дубля не будет.
INSERT_SQL = """
INSERT INTO scores (transaction_id, score, fraud_flag)
VALUES (%s, %s, %s)
ON CONFLICT (transaction_id) DO NOTHING;
"""


def connect_postgres(retries: int = 30, delay: float = 2.0):
    for attempt in range(1, retries + 1):
        try:
            conn = psycopg2.connect(**PG_DSN)
            conn.autocommit = True
            logger.info("Connected to Postgres")
            return conn
        except psycopg2.OperationalError as e:
            logger.warning("Postgres not ready (attempt %d/%d): %s", attempt, retries, e)
            time.sleep(delay)
    raise RuntimeError("Could not connect to Postgres")


def parse_message(raw: bytes) -> dict:
    payload = json.loads(raw.decode("utf-8"))
    # Совместимость со старым форматом семинара, где результат был списком [{...}]
    if isinstance(payload, list):
        payload = payload[0]
    return {
        "transaction_id": str(payload["transaction_id"]),
        "score": float(payload["score"]),
        "fraud_flag": int(payload["fraud_flag"]),
    }


def main():
    conn = connect_postgres()
    with conn.cursor() as cur:
        cur.execute(CREATE_TABLE_SQL)

    consumer = Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "group.id": "db-writer",
        "auto.offset.reset": "earliest",
    })
    consumer.subscribe([SCORES_TOPIC])
    logger.info("Listening topic '%s'...", SCORES_TOPIC)

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                logger.error("Kafka error: %s", msg.error())
                continue
            try:
                row = parse_message(msg.value())
                with conn.cursor() as cur:
                    cur.execute(INSERT_SQL, (row["transaction_id"], row["score"], row["fraud_flag"]))
                logger.info("Saved %s (score=%.4f, flag=%d)",
                            row["transaction_id"], row["score"], row["fraud_flag"])
            except (psycopg2.InterfaceError, psycopg2.OperationalError):
                # Соединение с базой оборвалось — переподключаемся
                logger.warning("Lost Postgres connection, reconnecting...")
                conn = connect_postgres()
            except Exception as e:
                logger.error("Error processing message: %s", e)
    finally:
        consumer.close()
        conn.close()


if __name__ == "__main__":
    main()
