from inventory.events.kafka_publisher import KafkaEventPublisher
from inventory.events.inventory_events import (
    INVENTORY_RESERVED,
    INVENTORY_FAILED,
)

publisher = KafkaEventPublisher()


def publish_inventory_reserved(order_id, product_id, quantity):
    publisher.publish(
        INVENTORY_RESERVED,
        {
            "order_id": order_id,
            "product_id": product_id,
            "quantity": quantity,
        },
    )


def publish_inventory_failed(order_id, product_id, reason):
    publisher.publish(
        INVENTORY_FAILED,
        {
            "order_id": order_id,
            "product_id": product_id,
            "reason": reason,
        },
    )