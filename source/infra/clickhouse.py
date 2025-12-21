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
            sampling_rate Float32,
            ts DateTime64(3),
            uid UInt64 MATERIALIZED cityHash64(key, model_name, ts),
            version UInt64,
            trace_1 Float32,
            trace_2 Float32,
            trace_3 Float32,
            dd Float32,
            pp Float32,
            ss Float32
        )
        ENGINE = ReplacingMergeTree(version)
        ORDER BY (uid)
        """
    )

    client.command(
        f"""
        CREATE TABLE IF NOT EXISTS kafka_seismic_predictions
        (
            key String,
            model_name String,
            network String,
            station String,
            sensor_code String,
            orientation_order String,
            sampling_rate Float32,
            size_per_window UInt32,
            starttime DateTime64(3),
            endtime DateTime64(3),
            trace Array(Array(Float32)),
            dd Array(Float32),
            pp Array(Float32),
            ss Array(Float32)
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
                substring(orientation_order, 1, 3) AS orientation_order,
                sampling_rate,

                starttime
                + toIntervalMicrosecond(
                        ((idx - 1) * 1000000) / sampling_rate
                    ) AS ts,


                trace_i[1] AS trace_1,
                trace_i[2] AS trace_2,
                trace_i[3] AS trace_3,
                dd_i AS dd,
                pp_i AS pp,
                ss_i AS ss,

                toInt64(-_offset) AS version
            FROM kafka_seismic_predictions
            ARRAY JOIN
                arrayEnumerate(trace) AS idx,
                trace AS trace_i,
                dd    AS dd_i,
                pp    AS pp_i,
                ss    AS ss_i;
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
