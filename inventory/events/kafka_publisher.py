import json
import logging

from confluent_kafka import Producer
from django.conf import settings

logger = logging.getLogger(__name__)


class KafkaEventPublisher:
    def __init__(self):
        config = {
            "bootstrap.servers": settings.KAFKA_BOOTSTRAP_SERVERS,
        }
        self.producer = Producer(config)

    def publish(self, topic: str, event: dict):
        self.producer.produce(topic, value=json.dumps(event).encode("utf-8"))
        self.producer.flush()
        logger.info("Event sent to Kafka topic: %s", topic)
