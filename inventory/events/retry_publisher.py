import json
import logging

from confluent_kafka import Producer
from django.conf import settings

logger = logging.getLogger(__name__)


class RetryPublisher:
    def __init__(self):
        config = {
            "bootstrap.servers": settings.KAFKA_BOOTSTRAP_SERVERS,
        }
        self.producer = Producer(config)

    def publish(self, topic: str, envelope_dict: dict, retry_count: int):
        envelope_dict["retry_count"] = retry_count
        payload = json.dumps(envelope_dict).encode("utf-8")
        self.producer.produce(topic, value=payload)
        self.producer.flush()
        logger.info(
            "Sent event to retry topic: %s (attempt %d)", topic, retry_count
        )
