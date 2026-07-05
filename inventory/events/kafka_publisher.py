import json
from confluent_kafka import Producer
from django.conf import settings


class KafkaEventPublisher:
    def __init__(self):
        config = {
            "bootstrap.servers": settings.KAFKA_BOOTSTRAP_SERVERS,
        }
        self.producer = Producer(config)

    def publish(self, topic: str, event: dict):
        self.producer.produce(topic, value=json.dumps(event).encode("utf-8"))
        self.producer.flush()
        print(f"Event sent to Kafka topic: {topic}")
