"""
JSON formatter for structured logging.

Every log record becomes a single JSON line on stdout, enriched with the
current execution context (correlation_id / event_id / event_type) and any
extra fields passed via `logger.info("...", extra={...})`.

Developers can keep writing:

    logger.info("Order created")

and the IDs are attached automatically.
"""

import json
import logging
import os
import traceback
from datetime import datetime, timezone

from core.logging import context

# Attributes that Python's logging machinery owns. Anything else found on the
# LogRecord is treated as an application-supplied `extra` field and included
# in the JSON output.
_RESERVED_ATTRS = {
    "name",
    "msg",
    "args",
    "levelname",
    "levelno",
    "pathname",
    "filename",
    "module",
    "exc_info",
    "exc_text",
    "stack_info",
    "lineno",
    "funcName",
    "created",
    "msecs",
    "relativeCreated",
    "thread",
    "threadName",
    "processName",
    "process",
    "taskName",
    "message",
    "asctime",
}

_DEFAULT_SERVICE_NAME = "unknown-service"


class JsonFormatter(logging.Formatter):
    def __init__(self, fmt=None, datefmt=None, style="%", validate=True, service=None):
        super().__init__(fmt=fmt, datefmt=datefmt, style=style, validate=validate)
        self.service = service or os.getenv("SERVICE_NAME", _DEFAULT_SERVICE_NAME)

    def format(self, record):
        entry = {
            "timestamp": self._timestamp(),
            "level": record.levelname,
            "service": self.service,
            "message": record.getMessage(),
            "logger": record.name,
        }

        ctx = context.get_context()
        entry["correlation_id"] = ctx.get("correlation_id")
        entry["event_id"] = ctx.get("event_id")
        entry["event_type"] = ctx.get("event_type")

        kafka = ctx.get("kafka")
        if kafka:
            entry.update(
                {key: value for key, value in kafka.items() if value is not None}
            )

        for key, value in record.__dict__.items():
            if key not in _RESERVED_ATTRS and key not in entry:
                entry[key] = value

        if record.exc_info:
            exc_type, exc_value, _ = record.exc_info
            entry["exception_type"] = exc_type.__name__
            entry["exception_message"] = str(exc_value) if exc_value else None
            entry["traceback"] = "".join(
                traceback.format_exception(*record.exc_info)
            ).strip()

        return json.dumps(entry, default=str)

    @staticmethod
    def _timestamp() -> str:
        return (
            datetime.now(timezone.utc)
            .isoformat(timespec="milliseconds")
            .replace("+00:00", "Z")
        )
