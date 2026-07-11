from inventory.events.kafka_publisher import KafkaEventPublisher
from inventory.events.inventory_events import (
    INVENTORY_RESERVED,
    INVENTORY_FAILED,
)

publisher = KafkaEventPublisher()


def publish_inventory_reserved(correlation_id, order_id, product_id, quantity):
    publisher.publish(
        INVENTORY_RESERVED,
        {
            "correlation_id": correlation_id,
            "order_id": order_id,
            "product_id": product_id,
            "quantity": quantity,
        },
    )


def publish_inventory_failed(correlation_id, order_id, product_id, reason):
    publisher.publish(
        INVENTORY_FAILED,
        {
            "correlation_id": correlation_id,
            "order_id": order_id,
            "product_id": product_id,
            "reason": reason,
        },
    )