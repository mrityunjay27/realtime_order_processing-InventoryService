import logging
from datetime import datetime, timezone

from inventory.models.outbox_event import OutboxEvent

logger = logging.getLogger(__name__)


class OutboxService:

    @staticmethod
    def create_event(event_id: str, event_type: str, payload: dict):
        OutboxEvent.objects.create(
            event_id=event_id,
            event_type=event_type,
            payload=payload,
        )
        logger.info("Outbox event created: %s [%s]", event_type, event_id)

    @staticmethod
    def mark_published(outbox_id):
        OutboxEvent.objects.filter(id=outbox_id).update(
            status=OutboxEvent.Status.PUBLISHED,
            published_at=datetime.now(timezone.utc),
        )
        logger.info("Outbox event marked as published: %s", outbox_id)

    @staticmethod
    def mark_failed(outbox_id):
        OutboxEvent.objects.filter(id=outbox_id).update(
            status=OutboxEvent.Status.FAILED,
        )
        logger.info("Outbox event marked as failed: %s", outbox_id)
