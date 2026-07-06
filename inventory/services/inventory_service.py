import logging

from django.db import transaction

from inventory.models import Inventory
from inventory.events.event_publisher import (
    publish_inventory_reserved,
    publish_inventory_failed,
)

logger = logging.getLogger(__name__)


class InventoryService:

    @staticmethod
    @transaction.atomic
    def reserve_inventory(order_id: str, product_id: str, quantity: int):

        try:
            inventory = Inventory.objects.select_for_update().get(
                product_id=product_id
            )

        except Inventory.DoesNotExist:
            publish_inventory_failed(
                order_id, product_id, "PRODUCT_NOT_FOUND"
            )
            return False

        if inventory.available_quantity < quantity:
            publish_inventory_failed(
                order_id, product_id, "OUT_OF_STOCK"
            )
            return False

        inventory.available_quantity -= quantity
        inventory.save()

        publish_inventory_reserved(
            order_id, product_id, quantity
        )

        logger.info("Reserved: %s x %s for order %s", product_id, quantity, order_id)

        return True

    @staticmethod
    @transaction.atomic
    def release_inventory(order_id: str, product_id: str, quantity: int):

        inventory = Inventory.objects.select_for_update().get(
            product_id=product_id
        )

        inventory.available_quantity += quantity
        inventory.save()

        logger.info("Released: %s x %s for order %s", product_id, quantity, order_id)