import json
import logging
from uuid import uuid4

from confluent_kafka import Producer
from django.conf import settings
from inventory.events.event_envelope import EventEnvelope

from core.logging import context as logging_context

logger = logging.getLogger(__name__)


class KafkaEventPublisher:
    def __init__(self):
        config = {
            "bootstrap.servers": settings.KAFKA_BOOTSTRAP_SERVERS,
        }
        self.producer = Producer(config)

    def publish(self, topic: str, event: dict):
        correlation_id = event.get("correlation_id") or str(uuid4())
        logging_context.set_context(
            correlation_id=correlation_id,
            event_id=event.get("event_id"),
            event_type=event.get("event_type", topic),
        )
        try:
            envelope = EventEnvelope(
                event_type=topic,
                correlation_id=correlation_id,
                payload=event,
            )
            payload = json.dumps(envelope.to_dict()).encode("utf-8")
            self.producer.produce(topic, value=payload)
            self.producer.flush()
            logger.info("Event sent to Kafka", extra={"topic": topic})
        finally:
            logging_context.clear_context()
