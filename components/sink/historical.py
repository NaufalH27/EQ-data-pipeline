import io
import json
from typing import List, Optional
from datetime import datetime, timezone
import numpy as np
from pydantic import BaseModel, field_validator, ConfigDict

from confluent_kafka import Consumer
from minio import Minio
from cassandra.cluster import Cluster


KAFKA_BOOTSTRAP = "localhost:9092"
KAFKA_TOPIC = "seismic-predictions"
KAFKA_GROUP = "historical-db1"
MINIO_ENDPOINT = "localhost:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"
MINIO_BUCKET = "seismic"
SCYLLA_HOSTS = ["127.0.0.1"]
SCYLLA_PORT = 9042
KEYSPACE = "seismic_data"

class PredictionResults(BaseModel):
    key: str
    model_name: Optional[str] = "EqTransformer-conservative-tf29"
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


def upload_npz_to_minio(minio_client, bucket, object_name, **arrays):
    buffer = io.BytesIO()
    np.savez(buffer, **arrays)
    buffer.seek(0)

    data = buffer.getvalue()

    minio_client.put_object(
        bucket_name=bucket,
        object_name=object_name,
        data=io.BytesIO(data),
        length=len(data),
        content_type="application/octet-stream",
    )


def init_kafka():
    print("Connecting to Kafka")
    consumer = Consumer(
        {
            "bootstrap.servers": KAFKA_BOOTSTRAP,
            "group.id": KAFKA_GROUP,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    consumer.subscribe([KAFKA_TOPIC])
    return consumer


def init_minio():
    print("Connecting to MinIO")
    client = Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=False,
    )

    if not client.bucket_exists(MINIO_BUCKET):
        client.make_bucket(MINIO_BUCKET)

    return client


def init_scylla():
    print("Connecting to ScyllaDB")
    cluster = Cluster(SCYLLA_HOSTS, port=SCYLLA_PORT)
    session = cluster.connect()

    session.execute(
        f"""
        CREATE KEYSPACE IF NOT EXISTS {KEYSPACE}
        WITH replication = {{
            'class': 'SimpleStrategy',
            'replication_factor': 1
        }};
        """
    )

    session.set_keyspace(KEYSPACE)

    session.execute(
        """
        CREATE TABLE IF NOT EXISTS window_predictions (
            key text,
            starttime timestamp,
            endtime timestamp,
            network text,
            station text,
            sensor_code text,
            sampling_rate double,
            num_channels int,
            samples_per_channel int,
            total_samples int,
            orientation_order text,
            minio_ref text,
            model_name text,
            created_at timestamp,
            PRIMARY KEY ((key, model_name), starttime)
        ) WITH CLUSTERING ORDER BY (starttime ASC);
        """
    )

    insert_stmt = session.prepare(
        """
        INSERT INTO window_predictions (
            key, starttime, endtime,
            network, station, sensor_code,
            sampling_rate, num_channels,
            samples_per_channel, total_samples,
            orientation_order,
            minio_ref,
            model_name,
            created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
    )

    return session, insert_stmt


def main():
    try:
        consumer = init_kafka()
        minio_client = init_minio()
        session, insert_stmt = init_scylla()

        print("Consumer started")

        while True:
            msg = consumer.poll(1.0)

            if msg is None:
                continue

            if msg.error():
                print("Kafka error:", msg.error())
                continue

            try:
                payload = json.loads(msg.value().decode("utf-8"))
                results = PredictionResults(**payload)
            except Exception as e:
                print("Invalid message:", e)
                continue

            minio_ref = (
                f"{results.key}/"
                f"{results.model_name}/"
                f"{results.starttime.strftime('%Y%m%d%H%M%S')}_"
                f"{results.endtime.strftime('%Y%m%d%H%M%S')}.npz"
            )

            upload_npz_to_minio(
                minio_client,
                MINIO_BUCKET,
                minio_ref,
                trace=results.trace,
                dd=results.dd,
                pp=results.pp,
                ss=results.ss,
            )

            session.execute(
                insert_stmt,
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
                    "".join(results.orientation_order),
                    minio_ref,
                    results.model_name,
                    datetime.now(timezone.utc),
                ),
            )
            
            print(f"insert to cassandra and MinIo {results.key}: {results.starttime} - {results.endtime}")

    except KeyboardInterrupt:
        print("Shutting down")

    finally:
        try:
            consumer.close()
        except Exception:
            pass

if __name__ == "__main__":
    main()
