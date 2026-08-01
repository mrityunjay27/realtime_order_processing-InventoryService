"""
Logging filters.

ContextFilter attaches the execution-local tracing IDs to the LogRecord so
they are available to any handler/formatter (not just the JSON one).

HealthCheckFilter keeps noisy infrastructure probes out of the logs.
"""

import logging

from core.logging import context


class ContextFilter(logging.Filter):
    def filter(self, record):
        ctx = context.get_context()
        record.correlation_id = ctx.get("correlation_id")
        record.event_id = ctx.get("event_id")
        record.event_type = ctx.get("event_type")
        return True


class HealthCheckFilter(logging.Filter):
    """Drop HTTP access logs for health/readiness endpoints."""

    HEALTH_PATHS = {"/healthz", "/health", "/readyz", "/livez"}

    def filter(self, record):
        path = getattr(record, "path", None)
        if path in self.HEALTH_PATHS:
            return False
        return True
