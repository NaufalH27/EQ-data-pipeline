from dataclasses import dataclass
from source.infra import KafkaAdmin, KafkaProducerService, KafkaConsumerService, EqTransformerService, MinioClientService, ScyllaService

@dataclass
class InfraServices:
    kafka_admin: KafkaAdmin
    producer: KafkaProducerService
    window_consumer: KafkaConsumerService
    prediction_consumer_scylla: KafkaConsumerService
    minio: MinioClientService
    scylla: ScyllaService
    eqt: EqTransformerService

    def close(self):
        self.producer.close()
        self.window_consumer.close()
        self.prediction_consumer_scylla.close()
        self.scylla.close()

