import json
from datetime import datetime, timezone
import logging
import numpy as np
from pydantic import BaseModel, field_validator, ConfigDict
from source.infra import KafkaConsumerService, MinioClientService, ScyllaService

logger = logging.getLogger("sink")

class PredictionResults(BaseModel):
    key: str
    model_name: str
    network: str
    station: str
    sensor_code: str
    sampling_rate: float
    size_per_window: int
    orientation_order: str
    starttime: datetime
    endtime: datetime
    trace: np.ndarray
    dd: np.ndarray
    pp: np.ndarray
    ss: np.ndarray

    model_config = ConfigDict(arbitrary_types_allowed=True)

    @field_validator("trace", "dd", "pp", "ss", mode="before")
    @classmethod
    def to_numpy(cls, v):
        return np.asarray(v, dtype=np.float32)


def start_scylla_sink(consumerService: KafkaConsumerService, minioService: MinioClientService, scyllaService: ScyllaService):
        logger.info("scylla sink started")
        consumer = consumerService.get_consumer()
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
            except Exception as e:
                logger.warning("Invalid message (skipping data):", e)
                continue

            minio_ref = (
                f"{results.key}/"
                f"{results.model_name}/"
                f"{results.starttime.strftime('%Y%m%d%H%M%S')}_"
                f"{results.endtime.strftime('%Y%m%d%H%M%S')}.npz"
            )

            minioService.upload_npz(
                object_name=minio_ref,
                trace=results.trace,
                dd=results.dd,
                pp=results.pp,
                ss=results.ss,
            )
            scyllaService.insert_prediction(
                (
                    results.key,
                    results.starttime,
                    results.endtime,
                    results.network,
                    results.station,
                    results.sensor_code,
                    results.sampling_rate,
                    len(results.orientation_order),
                    results.size_per_window,
                    results.trace.size,
                    results.orientation_order,
                    minio_ref,
                    results.model_name,
                    datetime.now(timezone.utc),
                ),
            )
            logger.debug(f"insert to cassandra and MinIo {results.key}: {results.starttime} - {results.endtime}")
