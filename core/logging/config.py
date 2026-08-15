"""
Logging configuration.

Services configure the standard Python logging stack via Django's LOGGING
setting (see config/settings.py):

    LOGGING = get_logging_config(service_name=SERVICE_NAME)

Every log record is emitted as a single JSON line both to stdout and to
logs/application.log. The file is mounted read-only into the Alloy
container, which tails it and ships the JSON lines to Loki.
"""

import os

from django.conf import settings

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
DB_LOG_LEVEL = os.getenv("DB_LOG_LEVEL", "INFO")
LOG_FILE_PATH = os.getenv("LOG_FILE_PATH", None)

# Rotate the file once it reaches 10MB, keep 3 backups.
LOG_FILE_MAX_BYTES = int(os.getenv("LOG_FILE_MAX_BYTES", "10485760"))
LOG_FILE_BACKUP_COUNT = int(os.getenv("LOG_FILE_BACKUP_COUNT", "3"))


def setup_logging():
    """Apply logging config imperatively (useful outside Django setup)."""
    from django.utils.log import configure_logging

    configure_logging(settings.LOGGING, settings.LOGGING_CONFIG)


def get_logging_config(service_name: str = None) -> dict:
    log_file_path = LOG_FILE_PATH or str(settings.BASE_DIR / "logs" / "application.log")
    os.makedirs(os.path.dirname(log_file_path), exist_ok=True)

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
            "file": {
                "class": "logging.handlers.RotatingFileHandler",
                "filename": log_file_path,
                "maxBytes": LOG_FILE_MAX_BYTES,
                "backupCount": LOG_FILE_BACKUP_COUNT,
                "formatter": "json",
                "filters": ["context"],
            },
        },
        "root": {
            "handlers": ["console", "file"],
            "level": LOG_LEVEL,
        },
        "loggers": {
            "django.db.backends": {
                "level": DB_LOG_LEVEL,
            },
        },
    }
