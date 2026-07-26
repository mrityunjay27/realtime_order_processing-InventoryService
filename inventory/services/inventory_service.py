import logging
from uuid import uuid4

from django.db import transaction, DatabaseError

from inventory.models import Inventory
from inventory.events.event_envelope import EventEnvelope
from inventory.events.inventory_events import INVENTORY_RESERVED, INVENTORY_FAILED, INVENTORY_RELEASED
from inventory.events.outbox_service import OutboxService
from inventory.events.audit.services import EventHistoryService
from inventory.events.audit.constants import AGGREGATE_INVENTORY, format_aggregate_id
from inventory.events.exceptions import (
    RetryableEventException,
    NonRetryableEventException,
)

logger = logging.getLogger(__name__)


class InventoryService:

    @staticmethod
    @transaction.atomic
    def reserve_inventory(correlation_id: str, order_id: str, product_id: str, quantity: int):

        try:
            inventory = Inventory.objects.select_for_update().get(
                product_id=product_id
            )
        except Inventory.DoesNotExist:
            _publish_inventory_failed(correlation_id, order_id, product_id, "PRODUCT_NOT_FOUND")
            return False
        except DatabaseError as exc:
            raise RetryableEventException(
                f"Database error while fetching inventory: {exc}"
            ) from exc

        if inventory.available_quantity < quantity:
            _publish_inventory_failed(correlation_id, order_id, product_id, "OUT_OF_STOCK")
            return False

        inventory.available_quantity -= quantity
        inventory.save()

        _publish_inventory_reserved(correlation_id, order_id, product_id, quantity)

        logger.info("Reserved: %s x %s for order %s", product_id, quantity, order_id)

        return True

    @staticmethod
    @transaction.atomic
    def release_inventory(correlation_id: str, order_id: str, product_id: str, quantity: int):

        inventory = Inventory.objects.select_for_update().get(
            product_id=product_id
        )

        inventory.available_quantity += quantity
        inventory.save()

        _publish_inventory_released(correlation_id, order_id, product_id, quantity)

        logger.info("Released: %s x %s for order %s", product_id, quantity, order_id)


def _publish_inventory_reserved(correlation_id, order_id, product_id, quantity):
    envelope = EventEnvelope(
        event_type=INVENTORY_RESERVED,
        correlation_id=correlation_id,
        payload={
            "correlation_id": correlation_id,
            "order_id": order_id,
            "product_id": product_id,
            "quantity": quantity,
        },
    )
    OutboxService.create_event(
        event_id=envelope.event_id,
        event_type=envelope.event_type,
        payload=envelope.to_dict(),
    )
    EventHistoryService.record_published(
        event_id=envelope.event_id,
        event_type=envelope.event_type,
        correlation_id=correlation_id,
        aggregate_type=AGGREGATE_INVENTORY,
        aggregate_id=format_aggregate_id(AGGREGATE_INVENTORY, product_id),
        payload=envelope.to_dict(),
    )


def _publish_inventory_failed(correlation_id, order_id, product_id, reason):
    envelope = EventEnvelope(
        event_type=INVENTORY_FAILED,
        correlation_id=correlation_id,
        payload={
            "correlation_id": correlation_id,
            "order_id": order_id,
            "product_id": product_id,
            "reason": reason,
        },
    )
    OutboxService.create_event(
        event_id=envelope.event_id,
        event_type=envelope.event_type,
        payload=envelope.to_dict(),
    )
    EventHistoryService.record_published(
        event_id=envelope.event_id,
        event_type=envelope.event_type,
        correlation_id=correlation_id,
        aggregate_type=AGGREGATE_INVENTORY,
        aggregate_id=format_aggregate_id(AGGREGATE_INVENTORY, product_id),
        payload=envelope.to_dict(),
    )


def _publish_inventory_released(correlation_id, order_id, product_id, quantity):
    envelope = EventEnvelope(
        event_type=INVENTORY_RELEASED,
        correlation_id=correlation_id,
        payload={
            "correlation_id": correlation_id,
            "order_id": order_id,
            "product_id": product_id,
            "quantity": quantity,
        },
    )
    OutboxService.create_event(
        event_id=envelope.event_id,
        event_type=envelope.event_type,
        payload=envelope.to_dict(),
    )
    EventHistoryService.record_published(
        event_id=envelope.event_id,
        event_type=envelope.event_type,
        correlation_id=correlation_id,
        aggregate_type=AGGREGATE_INVENTORY,
        aggregate_id=format_aggregate_id(AGGREGATE_INVENTORY, product_id),
        payload=envelope.to_dict(),
    )