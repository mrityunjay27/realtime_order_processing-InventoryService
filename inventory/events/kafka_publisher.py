import json
import logging
from uuid import uuid4

from confluent_kafka import Producer
from django.conf import settings
from inventory.events.event_envelope import EventEnvelope

logger = logging.getLogger(__name__)


class KafkaEventPublisher:
    def __init__(self):
        config = {
            "bootstrap.servers": settings.KAFKA_BOOTSTRAP_SERVERS,
        }
        self.producer = Producer(config)

    def publish(self, topic: str, event: dict):
        envelope = EventEnvelope(
            event_type=topic,
            correlation_id=event.get("correlation_id", str(uuid4())),
            payload=event,
        )
        payload = json.dumps(envelope.to_dict()).encode("utf-8")
        self.producer.produce(topic, value=payload)
        self.producer.flush()
        logger.info("Event sent to Kafka topic: %s", topic)
