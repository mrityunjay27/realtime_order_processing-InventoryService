"""
Logging configuration.

Services configure the standard Python logging stack via Django's LOGGING
setting (see config/settings.py):

    LOGGING = get_logging_config(service_name=SERVICE_NAME)

Every log record is emitted as a single JSON line to stdout so it can be
picked up by Docker/Kubernetes and shipped to Loki/Elasticsearch/etc.
"""

import os

from django.conf import settings

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
DB_LOG_LEVEL = os.getenv("DB_LOG_LEVEL", "INFO")


def setup_logging():
    """Apply logging config imperatively (useful outside Django setup)."""
    from django.utils.log import configure_logging

    configure_logging(settings.LOGGING, settings.LOGGING_CONFIG)


def get_logging_config(service_name: str = None) -> dict:
    return {
        "version": 1,
        "disable_existing_loggers": False,
        "filters": {
            "context": {
                "()": "core.logging.filters.ContextFilter",
            },
        },
        "formatters": {
            "json": {
                "()": "core.logging.formatter.JsonFormatter",
                "service": service_name,
            },
        },
        "handlers": {
            "console": {
                "class": "logging.StreamHandler",
                "formatter": "json",
                "filters": ["context"],
            },
        },
        "root": {
            "handlers": ["console"],
            "level": LOG_LEVEL,
        },
        "loggers": {
            "django.db.backends": {
                "level": DB_LOG_LEVEL,
            },
        },
    }
