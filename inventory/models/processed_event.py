# ProcessedEvent: The database guard against duplicate message processing.
#
# Kafka guarantees at-least-once delivery, meaning a message can be
# delivered multiple times (broker retries, consumer crashes, rebalances).
# Without this table, the same order-created event could reserve
# inventory twice, double-decrementing stock.
#
# How it works:
#   1. Consumer receives a message with event_id = "abc-123"
#   2. Before processing, it checks: does "abc-123" exist here?
#   3. If yes → skip (already handled by a previous delivery)
#   4. If no  → process the event, then INSERT "abc-123" here
#
# The unique=True constraint on event_id is the critical piece.
# Even if two consumers process the same event concurrently, only
# one INSERT will succeed; the other raises IntegrityError and rolls back.
#
# This is the "transactional outbox" pattern's simpler sibling:
# we're not publishing from inside a transaction, but we ARE recording
# consumption inside the same transaction as the business logic.

from django.db import models


class ProcessedEvent(models.Model):
    event_id = models.UUIDField(unique=True)
    event_type = models.CharField(max_length=100)
    processed_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        db_table = "processed_events"

    def __str__(self):
        return f"{self.event_type} [{self.event_id}]"
