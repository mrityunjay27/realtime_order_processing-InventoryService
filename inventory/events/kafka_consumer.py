# Inventory Kafka Consumer: Processes order-created events from Kafka,
# reserving stock for each item in the order.
#
# Offset commit strategy:
#   Kafka auto-commit is DISABLED. The offset is committed manually only
#   AFTER the database transaction succeeds. This guarantees:
#
#   - DB commit + Kafka commit are both done → event fully processed
#   - DB fails → no Kafka commit → Kafka redelivers on restart
#   - Crash between DB commit and Kafka commit → Kafka redelivers,
#     but ProcessedEvent's unique constraint prevents re-processing
#
# This is the foundation for retry and DLQ: if processing fails,
# the offset is NOT committed, so Kafka will redeliver the message
# on consumer restart. A future retry limit can then route to DLQ.

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
            "enable.auto.commit": False,
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
                    logger.error("Consumer error: %s", msg.error())
                    continue

                envelope = EventEnvelope.from_json(msg.value().decode("utf-8"))

                try:
                    with transaction.atomic():
                        if IdempotencyService.already_processed(envelope.event_id):
                            logger.info("Event %s already processed", envelope.event_id)

                        elif envelope.event_type == ORDER_CREATED:
                            self.handle_order_created(envelope)
                            IdempotencyService.mark_processed(envelope.event_id, envelope.event_type)

                        else:
                            logger.warning("No handler for event_type %s", envelope.event_type)

                    # DB transaction succeeded — safe to commit Kafka offset
                    self.consumer.commit(msg)

                except IntegrityError:
                    # Database says duplicate (race condition) — safe to commit
                    logger.info("Duplicate event %s, committing offset", envelope.event_id)
                    self.consumer.commit(msg)

                except Exception:
                    # Processing failed — do NOT commit.
                    # Kafka will redeliver on consumer restart.
                    logger.exception("Failed processing event %s, will retry", envelope.event_id)

        except KeyboardInterrupt:
            pass
        finally:
            self.consumer.close()
