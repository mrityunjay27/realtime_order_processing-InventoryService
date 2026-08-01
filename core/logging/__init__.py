"""
Structured logging infrastructure (cross-cutting, shared by every service).

Public surface:
    context      - execution-local tracing IDs (correlation_id, event_id, ...)
    config       - build the Django LOGGING dict / configure logging
    middleware   - Django middleware that owns correlation_id at the HTTP edge
"""

from core.logging import context  # noqa: F401
from core.logging.config import get_logging_config, setup_logging  # noqa: F401

__all__ = [
    "context",
    "get_logging_config",
    "setup_logging",
]
