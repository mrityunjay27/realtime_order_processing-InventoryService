import logging

from confluent_kafka import Consumer, KafkaError
from django.conf import settings
from django.db import transaction, IntegrityError

logger = logging.getLogger(__name__)

from inventory.events.event_envelope import EventEnvelope
from inventory.events.idempotency import IdempotencyService
from inventory.events.inventory_events import ORDER_CREATED, ORDER_CREATED_RETRY, RELEASE_INVENTORY, RELEASE_INVENTORY_RETRY
from inventory.events.failure_handler import FailureHandler
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
        self.failure_handlers = {
            ORDER_CREATED: FailureHandler(
                retry_topic="orders.created.retry",
                dlq_topic="orders.created.dlq",
            ),
            RELEASE_INVENTORY: FailureHandler(
                retry_topic="inventory.release.retry",
                dlq_topic="inventory.release.dlq",
            ),
        }
        self.consumer.subscribe([ORDER_CREATED, ORDER_CREATED_RETRY, RELEASE_INVENTORY, RELEASE_INVENTORY_RETRY])

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

    def handle_release_inventory(self, envelope: EventEnvelope):
        event = envelope.payload

        logger.info("Received release-inventory event [%s]: %s", envelope.correlation_id, event)

        for item in event["items"]:
            InventoryService.release_inventory(
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
                envelope_dict = envelope.to_dict()

                try:
                    with transaction.atomic():
                        if IdempotencyService.already_processed(envelope.event_id):
                            logger.info("Event %s already processed", envelope.event_id)

                        elif envelope.event_type in (ORDER_CREATED, ORDER_CREATED_RETRY):
                            self.handle_order_created(envelope)
                            IdempotencyService.mark_processed(envelope.event_id, envelope.event_type)

                        elif envelope.event_type in (RELEASE_INVENTORY, RELEASE_INVENTORY_RETRY):
                            self.handle_release_inventory(envelope)
                            IdempotencyService.mark_processed(envelope.event_id, envelope.event_type)

                        else:
                            logger.warning("No handler for event_type %s", envelope.event_type)

                    # DB transaction succeeded — safe to commit Kafka offset
                    self.consumer.commit(msg)

                except IntegrityError:
                    # Database says duplicate (race condition) — safe to commit
                    logger.info("Duplicate event %s, committing offset", envelope.event_id)
                    self.consumer.commit(msg)

                except Exception as exc:
                    self.consumer.commit(msg)
                    # Route to the correct failure handler based on base event type
                    base_type = envelope.event_type.replace(".retry", "")
                    handler = self.failure_handlers.get(base_type)
                    if handler:
                        handler.handle(envelope_dict, exc)
                    else:
                        logger.exception("No failure handler for event_type %s", envelope.event_type)

        except KeyboardInterrupt:
            pass
        finally:
            self.consumer.close()
