# Inventory Kafka Consumer: Processes order-created events from Kafka,
# reserving stock for each item in the order.
#
# Idempotency guarantee:
#   Kafka delivers messages at-least-once. This consumer ensures each
#   event is processed exactly once by wrapping the business logic and
#   the ProcessedEvent insert in a single database transaction.
#
# Failure scenarios handled:
#   1. Duplicate delivery → already_processed() check skips it
#   2. Crash after processing but before mark_processed → Kafka
#      redelivers; already_processed() catches it on retry
#   3. Crash after mark_processed but before commit → transaction
#      rolls back; Kafka redelivers; clean retry
#   4. Two consumers process same event concurrently → one succeeds,
#      the other gets IntegrityError from unique constraint → caught
#      and logged, no data corruption

import logging

from confluent_kafka import Consumer, KafkaError
from django.conf import settings
from django.db import transaction, IntegrityError

logger = logging.getLogger(__name__)

from inventory.events.event_envelope import EventEnvelope
from inventory.events.idempotency import IdempotencyService
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

                try:
                    with transaction.atomic():
                        if IdempotencyService.already_processed(envelope.event_id):
                            logger.info("Event %s already processed, skipping", envelope.event_id)
                            continue

                        if envelope.event_type == ORDER_CREATED:
                            self.handle_order_created(envelope)
                            IdempotencyService.mark_processed(envelope.event_id, envelope.event_type)
                        else:
                            logger.warning("No handler for event_type %s", envelope.event_type)
                except IntegrityError:
                    logger.info("Event %s already processed by another consumer", envelope.event_id)

        except KeyboardInterrupt:
            pass
        finally:
            self.consumer.close()
