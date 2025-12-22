from dataclasses import dataclass
from dotenv import load_dotenv
import os

load_dotenv()

@dataclass(frozen=True)
class AppSettings:
    seedlink_endpoint: str

    bootstrap_kafka: str
    clickhouse_bootstrap_kafka: str

    window_seismic_topic: str
    prediction_seismic_topic: str

    raw_window_kafka_group: str
    scylla_kafka_group: str
    clickhouse_kafka_group: str

    model_path: str
    model_name: str

    clickhouse_host: str
    clickhouse_port: int
    clickhouse_user: str
    clickhouse_password: str

    scylla_hosts: list[str]
    scylla_port: int
    scylla_keyspace: str

    station_list_path: str
    window_size: int


    @staticmethod
    def from_env() -> "AppSettings":
        return AppSettings(
            seedlink_endpoint=os.environ["SEEDLINK_ENDPOINT"],

            bootstrap_kafka=os.environ["BOOTSTRAP_KAFKA"],
            clickhouse_bootstrap_kafka=os.environ["CLICKHOUSE_BOOTSTRAP_KAFKA"],

            window_seismic_topic=os.environ["WINDOW_SEISMIC_TOPIC"],
            prediction_seismic_topic=os.environ["PREDICTION_SEISMIC_TOPIC"],

            raw_window_kafka_group=os.environ["RAW_WINDOW_KAFKA_GROUP"],
            scylla_kafka_group=os.environ["SCYLLA_KAFKA_GROUP"],
            clickhouse_kafka_group=os.environ["CLICKHOUSE_KAFKA_GROUP"],

            model_path=os.environ["MODEL_PATH"],
            model_name=os.environ["MODEL_NAME"],

            clickhouse_host=os.environ["CLICKHOUSE_HOST"],
            clickhouse_port=int(os.environ["CLICKHOUSE_PORT"]),
            clickhouse_user=os.environ["CLICKHOUSE_USER"],
            clickhouse_password=os.environ["CLICKHOUSE_PASSWORD"],

            scylla_hosts=os.environ["SCYLLA_HOSTS"].split(","),
            scylla_port=int(os.environ["SCYLLA_PORT"]),
            scylla_keyspace=os.environ["SCYLLA_KEYSPACE"],
            window_size=int(os.environ["WINDOW_SIZE"]),
            station_list_path=os.environ["STATION_LIST_PATH"]
        )
