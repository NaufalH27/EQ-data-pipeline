from dataclasses import dataclass
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka import Producer
from confluent_kafka import Consumer

@dataclass(frozen=True)
class KafkaConfig:
    bootstrap_servers: str
    group_id: str | None = None
    auto_offset_reset: str = "earliest"
    enable_auto_commit: bool = False

class KafkaAdmin:
    def __init__(self, bootstrap_servers: str):
        self.client = AdminClient(
            {"bootstrap.servers": bootstrap_servers}
        )

    def create_topic(
        self,
        name: str,
        partitions: int = 1,
        replication_factor: int = 1,
    ) -> None:
        topic = NewTopic(
            name,
            num_partitions=partitions,
            replication_factor=replication_factor,
        )

        futures = self.client.create_topics([topic])

        for topic, future in futures.items():
            try:
                future.result()
                print(f"Topic {topic} created")
            except Exception:
                print(f"INFO: Topic '{topic}' already exists")


class KafkaProducerService:
    def __init__(self, bootstrap_servers: str):
        self._producer = Producer(
            {"bootstrap.servers": bootstrap_servers}
        )
    def get_producer(self):
        return self._producer

    def close(self):
        self._producer.flush()


class KafkaConsumerService:
    def __init__(self, config: KafkaConfig, topics: list[str]):
        self._consumer = Consumer(
            {
                "bootstrap.servers": config.bootstrap_servers,
                "group.id": config.group_id,
                "auto.offset.reset": config.auto_offset_reset,
                "enable.auto.commit": config.enable_auto_commit,
            }
        )
        self._consumer.subscribe(topics)

    def get_consumer(self):
        return self._consumer

    def commit(self):
        self._consumer.commit()

    def close(self):
        self._consumer.close()
