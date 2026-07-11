import logging

from confluent_kafka import Consumer, KafkaError
from django.conf import settings

logger = logging.getLogger(__name__)

from inventory.events.event_envelope import EventEnvelope
from inventory.events.inventory_events import ORDER_CREATED
from inventory.services.inventory_service import InventoryService


class KafkaEventConsumer:

    def __init__(self):
        config = {
            "bootstrap.servers": settings.KAFKA_BOOTSTRAP_SERVERS,
            "group.id": "inventory-service-group",
            "auto.offset.reset": "earliest",
        }
        self.consumer = Consumer(config)
        self.consumer.subscribe([ORDER_CREATED])

    def handle_order_created(self, envelope: EventEnvelope):
        event = envelope.payload

        logger.info("Received event [%s]: %s", envelope.correlation_id, event)

        for item in event["items"]:
            InventoryService.reserve_inventory(
                correlation_id=envelope.correlation_id,
                order_id=event["order_id"],
                product_id=item["product_id"],
                quantity=item["quantity"],
            )

    def start(self):

        logger.info("Inventory Consumer Started...")

        try:
            while True:
                msg = self.consumer.poll(timeout=1.0)
                if msg is None:
                    continue
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    else:
                        logger.error("Consumer error: %s", msg.error())
                        break

                envelope = EventEnvelope.from_json(msg.value().decode("utf-8"))

                if envelope.event_type == ORDER_CREATED:
                    self.handle_order_created(envelope)
        except KeyboardInterrupt:
            pass
        finally:
            self.consumer.close()
