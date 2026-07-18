class RetryableEventException(Exception):
    """Raised for transient failures that may succeed on retry.

    Examples: database timeout, network timeout, Kafka temporarily
    unavailable, deadlock detected.
    """


class NonRetryableEventException(Exception):
    """Raised for failures that will not succeed on retry.

    Examples: product not found, order not found, validation failed,
    malformed payload.
    """
