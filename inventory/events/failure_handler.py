import logging

from inventory.events.exceptions import (
    RetryableEventException,
    NonRetryableEventException,
)
from inventory.events.retry_policy import RetryPolicy
from inventory.events.retry_publisher import RetryPublisher
from inventory.events.dlq_publisher import DLQPublisher

logger = logging.getLogger(__name__)


class FailureHandler:
    def __init__(self, retry_topic: str, dlq_topic: str):
        self.retry_topic = retry_topic
        self.dlq_topic = dlq_topic
        self.retry_publisher = RetryPublisher()
        self.dlq_publisher = DLQPublisher()

    def handle(self, envelope_dict: dict, exc: Exception):
        retry_count = envelope_dict.get("retry_count", 0)

        if isinstance(exc, NonRetryableEventException):
            logger.warning(
                "Non-retryable failure: %s — sending to DLQ", exc
            )
            self.dlq_publisher.publish(
                self.dlq_topic, envelope_dict, str(exc)
            )
            return

        if isinstance(exc, RetryableEventException):
            if retry_count >= RetryPolicy.MAX_RETRIES:
                logger.warning(
                    "Max retries (%d) exhausted: %s — sending to DLQ",
                    RetryPolicy.MAX_RETRIES,
                    exc,
                )
                self.dlq_publisher.publish(
                    self.dlq_topic, envelope_dict, str(exc)
                )
                return

            logger.info(
                "Retryable failure: %s — sending to retry topic", exc
            )
            self.retry_publisher.publish(
                self.retry_topic, envelope_dict, retry_count + 1
            )
            return

        # Unknown exception — treat as non-retryable
        logger.warning(
            "Unexpected exception type: %s — sending to DLQ", type(exc).__name__
        )
        self.dlq_publisher.publish(self.dlq_topic, envelope_dict, str(exc))
