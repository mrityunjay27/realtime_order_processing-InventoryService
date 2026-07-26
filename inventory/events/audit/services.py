import logging

from inventory.events.audit.models import EventHistory
from inventory.events.audit.constants import STATUS_PENDING, STATUS_SUCCESS, STATUS_FAILED

logger = logging.getLogger(__name__)


class EventHistoryService:

    @staticmethod
    def record_published(event_id, event_type, correlation_id, aggregate_type, aggregate_id, payload):
        EventHistory.objects.create(
            event_id=event_id,
            event_type=event_type,
            service_name="inventory-service",
            correlation_id=correlation_id,
            aggregate_type=aggregate_type,
            aggregate_id=aggregate_id,
            direction=EventHistory.Direction.PUBLISHED,
            status=STATUS_PENDING,
            payload=payload,
        )
        logger.info(
            "Event history [PUBLISHED]: %s [%s] correlation=%s",
            event_type, event_id, correlation_id,
        )

    @staticmethod
    def record_consumed(event_id, event_type, correlation_id, aggregate_type, aggregate_id, payload):
        EventHistory.objects.create(
            event_id=event_id,
            event_type=event_type,
            service_name="inventory-service",
            correlation_id=correlation_id,
            aggregate_type=aggregate_type,
            aggregate_id=aggregate_id,
            direction=EventHistory.Direction.CONSUMED,
            status=STATUS_SUCCESS,
            payload=payload,
        )
        logger.info(
            "Event history [CONSUMED]: %s [%s] correlation=%s",
            event_type, event_id, correlation_id,
        )

    @staticmethod
    def record_consumed_failed(event_id, event_type, correlation_id, aggregate_type, aggregate_id, payload):
        EventHistory.objects.create(
            event_id=event_id,
            event_type=event_type,
            service_name="inventory-service",
            correlation_id=correlation_id,
            aggregate_type=aggregate_type,
            aggregate_id=aggregate_id,
            direction=EventHistory.Direction.CONSUMED,
            status=STATUS_FAILED,
            payload=payload,
        )
        logger.warning(
            "Event history [CONSUMED-FAILED]: %s [%s] correlation=%s",
            event_type, event_id, correlation_id,
        )

    @staticmethod
    def mark_published_success(event_id):
        EventHistory.objects.filter(
            event_id=event_id,
            direction=EventHistory.Direction.PUBLISHED,
            status=STATUS_PENDING,
        ).update(status=STATUS_SUCCESS)
        logger.info("Event history marked PUBLISHED-SUCCESS: %s", event_id)

    @staticmethod
    def mark_published_failed(event_id):
        EventHistory.objects.filter(
            event_id=event_id,
            direction=EventHistory.Direction.PUBLISHED,
            status=STATUS_PENDING,
        ).update(status=STATUS_FAILED)
        logger.warning("Event history marked PUBLISHED-FAILED: %s", event_id)
