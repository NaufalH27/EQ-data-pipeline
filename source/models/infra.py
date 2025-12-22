from dataclasses import dataclass
from source.infra import KafkaAdmin, KafkaProducerService, KafkaConsumerService, EqTransformerService, ScyllaService

@dataclass
class InfraServices:
    kafka_admin: KafkaAdmin
    producer: KafkaProducerService
    window_consumer: KafkaConsumerService
    prediction_consumer_scylla: KafkaConsumerService
    scylla: ScyllaService
    eqt: EqTransformerService

    def close(self):
        self.producer.close()
        self.window_consumer.close()
        self.prediction_consumer_scylla.close()
        self.scylla.close()

