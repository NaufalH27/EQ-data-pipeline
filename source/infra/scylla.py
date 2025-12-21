from cassandra.cluster import Cluster
from cassandra.cluster import Session
from cassandra.query import PreparedStatement

class ScyllaService:
    def __init__(
        self,
        hosts: list[str],
        port: int,
        keyspace: str,
    ):
        print("Connecting to ScyllaDB")
        self._keyspace = keyspace
        self.cluster = Cluster(hosts, port=port)
        self.session: Session = self.cluster.connect()
        self._init_schema()
        self.insert_stmt: PreparedStatement = self._prepare_statements()

    def _init_schema(self) -> None:
        self.session.execute(
            f"""
            CREATE KEYSPACE IF NOT EXISTS {self._keyspace}
            WITH replication = {{
                'class': 'SimpleStrategy',
                'replication_factor': 1
            }};
            """
        )

        self.session.set_keyspace(self._keyspace)

        self.session.execute(
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

    def _prepare_statements(self) -> PreparedStatement:
        return self.session.prepare(
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
            IF NOT EXISTS
            """
        )

    def insert_prediction(self, values: tuple) -> None:
        self.session.execute(self.insert_stmt, values)

    def get_session(self) -> Session:
        return self.session

    def close(self) -> None:
        self.cluster.shutdown()
