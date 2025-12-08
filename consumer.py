import json
from datetime import datetime
from confluent_kafka import Consumer
from cassandra.cluster import Cluster


consumer = Consumer({
    "bootstrap.servers": "localhost:9092",
    "group.id": "seismic-consumer",
    "auto.offset.reset": "earliest"
})

consumer.subscribe(["seismic"])  


cluster = Cluster(['127.0.0.1'], port=9042)
session = cluster.connect()

session.execute("""
    CREATE KEYSPACE IF NOT EXISTS seismic
    WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}
""")

session.execute("""
    CREATE TABLE IF NOT EXISTS seismic.waveform (
        network text,
        station text,
        location text,
        channel text,
        starttime timestamp,
        endtime timestamp,
        sampling_rate double,
        total_sample int,
        data list<double>,
        PRIMARY KEY ((network, station, channel), starttime, endtime)
    )
""")

insert_query = session.prepare("""
    INSERT INTO seismic.waveform (
        network, station, location, channel,
        starttime, endtime,
        sampling_rate, total_sample, data
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
""")


while True:
    msg = consumer.poll(1.0)

    if msg is None:
        continue

    message = json.loads(msg.value().decode("utf-8"))

    session.execute(
        insert_query, (
            message["network"],
            message["station"],
            message["location"],
            message["channel"],
            datetime.fromisoformat(message["starttime"].replace("Z", "")),
            datetime.fromisoformat(message["endtime"].replace("Z", "")),
            message["sampling_rate"],
            message["total_sample"],
            message["data"],
        )
    )

    print(
        f"Inserted: {message['station']} {message['channel']} "
        f"{message['starttime']} samples={message['total_sample']}"
    )
