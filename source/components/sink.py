import json
from datetime import datetime, timezone
import logging
from pydantic import BaseModel
from source.infra import KafkaConsumerService, ScyllaService
from cassandra.query import BatchStatement

logger = logging.getLogger("sink")

class PredictionResults(BaseModel):
    key: str
    model_name: str
    ts: datetime
    network: str
    station: str
    sensor_code: str
    orientation_order: str
    trace1: float
    trace2: float
    trace3: float
    dd: float
    pp: float
    ss: float


def start_scylla_sink(consumerService: KafkaConsumerService, scyllaService: ScyllaService, batch_size: int = 500):
    logger.info("scylla sink started")
    consumer = consumerService.get_consumer()
    
    batch = []

    while True:
        msg = consumer.poll(1.0)

        if msg is None:
            continue

        if msg.error():
            logger.warning("Kafka error (skipping data):", msg.error())
            continue

        try:
            payload = json.loads(msg.value().decode("utf-8"))
            results = PredictionResults(**payload)
            batch.append(results)
        except Exception as e:
            logger.warning("Invalid message (skipping data):", e)
            continue

        if len(batch) >= batch_size:
            scyllaService.insert_predictions_batch(batch)
            batch.clear()  # 