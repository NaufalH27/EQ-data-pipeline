import json
import time
from obspy.clients.seedlink.easyseedlink import EasySeedLinkClient
import xml.etree.ElementTree as ET
from multiprocessing import Process
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka import Producer

admin_client = AdminClient({
    "bootstrap.servers": "localhost:9092"
})

producer = Producer({
    "bootstrap.servers": "localhost:9092"
})

topic_name = "seismic"
new_topic = NewTopic(topic_name, num_partitions=1, replication_factor=1)
fs = admin_client.create_topics([new_topic])

for topic, f in fs.items():
    try:
        f.result()
        print(f"Topic {topic} created")
    except Exception:
        print(f"INFO: Topic '{topic}' already exists")

class ObspyClient(EasySeedLinkClient):
    def on_data(self, trace):
        message = {
            "network": trace.stats.network,
            "station": trace.stats.station,
            "location": trace.stats.location,
            "channel": trace.stats.channel,
            "starttime": str(trace.stats.starttime),
            "endtime": str(trace.stats.endtime),
            "sampling_rate": trace.stats.sampling_rate,
            "total_sample": len(trace.data.tolist()),
            "data": trace.data.tolist()
        }
        producer.produce(topic_name, value=json.dumps(message).encode("utf-8"))
        producer.poll(0)
        print(f"data : \n {json.dumps(message)} \n sent to broker")

def process_station(station_name, seed):
    client = ObspyClient('geofon.gfz-potsdam.de:18000')
    client.select_stream('GE', station_name, seed)
    print(f"running client: GE {station_name} {seed}")
    client.run()


def main():
    root = ET.fromstring(ObspyClient('geofon.gfz-potsdam.de:18000').get_info("STREAMS"))
    tasks = []
    for station in root.findall("station"):
        if "indonesia" not in station.get("description", "").lower():
            continue

        station_name = station.get("name", "")

        for stream in station.findall("stream"):
            seed = stream.get("seedname", "")
            stream_type = stream.get("type", "")

            if seed.startswith("BH") and stream_type == "D":
                tasks.append((station_name, seed))
                print(f"created client: GE {station_name} {seed}")


    processes = []

    for station_name, seed in tasks:
        p = Process(target=process_station, args=(station_name, seed))
        p.start()
        processes.append(p)
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("Caught KeyboardInterrupt, stopping…")
    finally:
        print("Terminating processes…")
        for p in processes:
            p.terminate()
            p.join()
        producer.flush()

if __name__ == "__main__":
    main()
