import sys
import time
import clickhouse_connect
from dataclasses import dataclass


from dataclasses import dataclass
import sys
import time
import clickhouse_connect


@dataclass(frozen=True)
class ClickHouseConfig:
    host: str
    port: int
    user: str
    password: str

    kafka_broker: str
    kafka_topic: str
    kafka_group: str

def get_client(
    config: ClickHouseConfig,
    retries: int = 10,
    delay: float = 2.0,
):
    for i in range(retries):
        try:
            client = clickhouse_connect.get_client(
                host=config.host,
                port=config.port,
                username=config.user,
                password=config.password,
            )
            client.command("SELECT 1")
            print("Connected to ClickHouse")
            return client
        except Exception as e:
            print(e)
            print(f"Waiting for ClickHouse... ({i + 1}/{retries})")
            time.sleep(delay)

    print("ERROR: Could not connect to ClickHouse")
    sys.exit(1)


def create_tables(client, config: ClickHouseConfig):
    print("Creating ClickHouse tables...")

    client.command(
    """
        CREATE TABLE IF NOT EXISTS predicted_samples
        (
            key String,
            model_name LowCardinality(String),
            network LowCardinality(String),
            station LowCardinality(String),
            sensor_code LowCardinality(String),
            orientation_order FixedString(3),
            ts DateTime64(3),

            uid UInt64 MATERIALIZED cityHash64(key, model_name, ts),

            trace1 Float32,
            trace2 Float32,
            trace3 Float32,
            dd Float32,
            pp Float32,
            ss Float32
        )
        ENGINE = ReplacingMergeTree(dd)
        PARTITION BY toYYYYMM(ts)
        ORDER BY uid;
    """
    )

    client.command(
        f"""
        CREATE TABLE IF NOT EXISTS kafka_seismic_predictions
        (
            key String,
            model_name String,
            ts DateTime64(3),
            network String,
            station String,
            sensor_code String,
            orientation_order String,
            trace1 Float32,
            trace2 Float32,
            trace3 Float32,
            dd Float32,
            pp Float32,
            ss Float32
        )
        ENGINE = Kafka
        SETTINGS
            kafka_broker_list = '{config.kafka_broker}',
            kafka_topic_list = '{config.kafka_topic}',
            kafka_group_name = '{config.kafka_group}',
            kafka_format = 'JSONEachRow',
            kafka_num_consumers = 1
        """
    )

    client.command(
        """
            CREATE MATERIALIZED VIEW IF NOT EXISTS mv_predictions
            TO predicted_samples
            AS
            SELECT
                key,
                model_name,
                network,
                station,
                sensor_code,
                orientation_order,
                ts,
                trace1,
                trace2,
                trace3,
                dd,
                pp,
                ss
            FROM kafka_seismic_predictions;
        """
    )
    print("ClickHouse objects ready")



def sanity_check(client):
    print("ClickHouse sanity check")

    tables = client.query(
        """
        SELECT name
        FROM system.tables
        WHERE database = currentDatabase()
        """
    )

    for row in tables.result_rows:
        print(" -", row[0])


def bootstrap_clickhouse(config :ClickHouseConfig):
    client = get_client(config)
    create_tables(client, config)
    sanity_check(client)
    print("ClickHouse bootstrap completed successfully")
