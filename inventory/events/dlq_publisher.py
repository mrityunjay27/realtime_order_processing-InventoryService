import json
import logging
from datetime import datetime, timezone

from confluent_kafka import Producer
from django.conf import settings

logger = logging.getLogger(__name__)


class DLQPublisher:
    def __init__(self):
        config = {
            "bootstrap.servers": settings.KAFKA_BOOTSTRAP_SERVERS,
        }
        self.producer = Producer(config)

    def publish(self, topic: str, envelope_dict: dict, error: str):
        dlq_payload = {
            **envelope_dict,
            "error": error,
            "failed_at": datetime.now(timezone.utc).isoformat(),
            "service": "inventory-service",
        }
        payload = json.dumps(dlq_payload).encode("utf-8")
        self.producer.produce(topic, value=payload)
        self.producer.flush()
        logger.info("Sent event to DLQ topic: %s", topic)
