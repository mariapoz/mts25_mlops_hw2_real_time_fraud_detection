"""
Сервис скоринга: читает транзакции из топика Kafka `transactions`,
делает препроцессинг и скоринг, пишет результат в топик `scores`.

Входящее сообщение:  {"transaction_id": "...", "data": {<строка test.csv>}}
Исходящее сообщение: {"transaction_id": "...", "score": 0.0123, "fraud_flag": 0}
"""
import json
import logging
import os
import sys

import pandas as pd
from confluent_kafka import Consumer, Producer

sys.path.append(os.path.abspath('./src'))

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('/app/logs/service.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

from preprocessing import load_artifacts, run_preproc  # noqa: E402
from scorer import make_pred  # noqa: E402

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TRANSACTIONS_TOPIC = os.getenv("KAFKA_TRANSACTIONS_TOPIC", "transactions")
SCORES_TOPIC = os.getenv("KAFKA_SCORES_TOPIC", "scores")


class ProcessingService:
    def __init__(self):
        self.consumer = Consumer({
            'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
            'group.id': 'ml-scorer',
            'auto.offset.reset': 'earliest',
        })
        self.consumer.subscribe([TRANSACTIONS_TOPIC])
        self.producer = Producer({'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS})

        # Статистики для препроцессинга, заранее посчитанные по train
        self.artifacts = load_artifacts()

    def score_message(self, raw: bytes) -> dict:
        data = json.loads(raw.decode('utf-8'))
        transaction_id = data['transaction_id']
        input_df = pd.DataFrame([data['data']])

        processed_df = run_preproc(self.artifacts, input_df)
        prediction = make_pred(processed_df, 'kafka_stream').iloc[0]

        return {
            'transaction_id': transaction_id,
            'score': float(prediction['score']),
            'fraud_flag': int(prediction['fraud_flag']),
        }

    def process_messages(self):
        logger.info("Listening topic '%s'...", TRANSACTIONS_TOPIC)
        while True:
            msg = self.consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                logger.error(f"Kafka error: {msg.error()}")
                continue
            try:
                result = self.score_message(msg.value())
                self.producer.produce(
                    SCORES_TOPIC,
                    key=result['transaction_id'],
                    value=json.dumps(result),
                )
                self.producer.flush()
                logger.info("Scored %s: score=%.4f, fraud_flag=%d",
                            result['transaction_id'], result['score'], result['fraud_flag'])
            except Exception as e:
                logger.error(f"Error processing message: {e}")


if __name__ == "__main__":
    logger.info('Starting Kafka ML scoring service...')
    service = ProcessingService()
    try:
        service.process_messages()
    except KeyboardInterrupt:
        logger.info('Service stopped by user')
