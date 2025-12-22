from cassandra.cluster import Cluster
from cassandra.cluster import Cluster
from datetime import datetime, timezone
from cassandra.query import BatchStatement


class ScyllaService:
    def __init__(self, hosts: list[str], port: int, keyspace: str):
        print("Connecting to ScyllaDB")
        self._keyspace = keyspace
        self.cluster = Cluster(hosts, port=port)
        self.session = self.cluster.connect()
        self._init_schema()
        self._prepare_statements()

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
            CREATE TABLE IF NOT EXISTS predicted_samples (
                key text,
                model_name text,
                day date,
                ts timestamp,
                network text,
                station text,
                sensor_code text,
                orientation_order text,
                trace1 float,
                trace2 float,
                trace3 float,
                dd float,
                pp float,
                ss float,
                created_at timestamp,
                PRIMARY KEY ((key, model_name, day), ts)
            );
            """
        )

        self.session.execute(
            """
                CREATE TABLE IF NOT EXISTS predicted_sample_bucket_by_day (
                    key text,
                    model_name text,
                    day date,
                    PRIMARY KEY ((key, model_name), day)
                ) WITH CLUSTERING ORDER BY (day DESC);
            """
        )

    def _prepare_statements(self) -> None:
        self.insert_stmt = self.session.prepare(
            """
            INSERT INTO predicted_samples (
                key, model_name, day, ts, 
                network, station, sensor_code,
                orientation_order,
                trace1, trace2, trace3,
                dd, pp, ss,
                created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """
        )

        self.insert_day_index = self.session.prepare(
            """
            INSERT INTO predicted_sample_bucket_by_day (key, model_name, day)
            VALUES (?, ?, ?)
            """
        )

    def insert_predictions_batch(self, predictions) -> None:
        batch = BatchStatement()

        now = datetime.now(timezone.utc)
        
        for p in predictions:
            day = p.ts.date()
            bound = self.insert_stmt.bind((
                p.key,
                p.model_name,
                day,
                p.ts,
                p.network,
                p.station,
                p.sensor_code,
                p.orientation_order,
                p.trace1,
                p.trace2,
                p.trace3,
                p.dd,
                p.pp,
                p.ss,
                now
            ))

            base_ts = int(p.ts.timestamp() * 1_000_000)
            priority = int(p.dd * 1_000)
            bound.timestamp = base_ts + priority

            batch.add(bound)
            batch.add(self.insert_day_index.bind((p.key, p.model_name, day)))

        self.session.execute(batch)
