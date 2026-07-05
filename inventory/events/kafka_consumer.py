import json
from confluent_kafka import Consumer, KafkaError
from django.conf import settings

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

    def handle_order_created(self, event: dict):

        print(f"Received event: {event}")

        for item in event["items"]:
            InventoryService.reserve_inventory(
                order_id=event["order_id"],
                product_id=item["product_id"],
                quantity=item["quantity"],
            )

    def start(self):

        print("Inventory Consumer Started...")

        try:
            while True:
                msg = self.consumer.poll(timeout=1.0)
                if msg is None:
                    continue
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    else:
                        print(f"Consumer error: {msg.error()}")
                        break

                topic = msg.topic()
                event = json.loads(msg.value().decode("utf-8"))

                if topic == ORDER_CREATED:
                    self.handle_order_created(event)
        except KeyboardInterrupt:
            pass
        finally:
            self.consumer.close()
