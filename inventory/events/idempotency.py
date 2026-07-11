# IdempotencyService: Reusable duplicate-detection logic for all consumers.
#
# Why a separate service (not inline in the consumer):
#   - Every consumer (order, inventory, future services) needs the same
#     check-then-process-then-mark pattern.
#   - Centralizing it means bug fixes and optimizations (e.g., caching,
#     TTL cleanup) happen in one place.
#
# Usage in a consumer:
#   with transaction.atomic():
#       if IdempotencyService.already_processed(event_id):
#           return  # safe to skip
#       # ... process the event ...
#       IdempotencyService.mark_processed(event_id, event_type)
#
# The unique constraint on ProcessedEvent.event_id is the ultimate safety
# net. If two consumers race, the second INSERT raises IntegrityError,
# which the consumer catches and handles gracefully.

import logging

from inventory.models.processed_event import ProcessedEvent

logger = logging.getLogger(__name__)


class IdempotencyService:

    @staticmethod
    def already_processed(event_id: str) -> bool:
        return ProcessedEvent.objects.filter(event_id=event_id).exists()

    @staticmethod
    def mark_processed(event_id: str, event_type: str):
        ProcessedEvent.objects.create(
            event_id=event_id,
            event_type=event_type,
        )
        logger.info("Marked event %s [%s] as processed", event_id, event_type)
