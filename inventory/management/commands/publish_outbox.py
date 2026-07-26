import json
import logging
import time

from django.core.management.base import BaseCommand
from django.db import transaction
from django.utils import timezone

from confluent_kafka import Producer
from django.conf import settings

from inventory.models.outbox_event import OutboxEvent
from inventory.events.audit.services import EventHistoryService

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    help = "Publish pending outbox events to Kafka"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.producer = Producer({
            "bootstrap.servers": settings.KAFKA_BOOTSTRAP_SERVERS,
        })

    def handle(self, *args, **options):
        self.stdout.write("Outbox Publisher Started...")

        try:
            while True:
                published_count = self._publish_pending_events()
                if published_count > 0:
                    logger.info("Published %d outbox events", published_count)
                time.sleep(1)
        except KeyboardInterrupt:
            pass
        finally:
            self.producer.flush()
            self.stdout.write("Outbox Publisher Stopped.")

    def _publish_pending_events(self):
        count = 0

        with transaction.atomic():
            pending_events = (
                OutboxEvent.objects.select_for_update(skip_locked=True)
                .filter(status=OutboxEvent.Status.PENDING)
                .order_by("created_at")[:10]
            )

            for event in pending_events:
                try:
                    payload = json.dumps(event.payload).encode("utf-8")
                    self.producer.produce(event.event_type, value=payload)
                    self.producer.flush()

                    OutboxEvent.objects.filter(id=event.id).update(
                        status=OutboxEvent.Status.PUBLISHED,
                        published_at=timezone.now(),
                    )
                    EventHistoryService.mark_published_success(event.event_id)
                    count += 1

                except Exception:
                    logger.exception("Failed to publish outbox event %s", event.id)
                    OutboxEvent.objects.filter(id=event.id).update(
                        status=OutboxEvent.Status.FAILED,
                    )
                    EventHistoryService.mark_published_failed(event.event_id)

        return count
