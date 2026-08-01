"""
Django middleware that owns the correlation_id at the HTTP boundary.

Rules:
    - A valid `X-Correlation-ID` request header is trusted and reused.
    - A missing or invalid header gets a freshly generated UUID.
    - The value is stored in the execution-local logging context for the
      whole request and echoed back on the response as `X-Correlation-ID`.

This is the ONLY place where a new correlation_id is generated for an HTTP
workflow. Everything downstream (service layer, outbox, Kafka) reuses it.
"""

import logging
import time
import uuid

from core.logging import context

logger = logging.getLogger(__name__)

CORRELATION_HEADER = "X-Correlation-ID"


class CorrelationMiddleware:
    def __init__(self, get_response):
        self.get_response = get_response

    def __call__(self, request):
        correlation_id = self._extract_or_generate(request)
        context.set_context(correlation_id=correlation_id)
        request.correlation_id = correlation_id

        start = time.perf_counter()
        response = None
        try:
            response = self.get_response(request)
        except Exception:
            logger.exception(
                "Unhandled exception during request",
                extra={
                    "method": request.method,
                    "path": request.path,
                },
            )
            raise
        finally:
            duration_ms = round((time.perf_counter() - start) * 1000, 2)
            status_code = response.status_code if response is not None else None

            if response is not None:
                response[CORRELATION_HEADER] = correlation_id

            logger.info(
                "HTTP request completed",
                extra={
                    "method": request.method,
                    "path": request.path,
                    "status_code": status_code,
                    "duration_ms": duration_ms,
                },
            )
            context.clear_context()

        return response

    @staticmethod
    def _extract_or_generate(request) -> str:
        incoming = request.headers.get(CORRELATION_HEADER)
        if incoming and CorrelationMiddleware._is_valid_uuid(incoming):
            return incoming
        return str(uuid.uuid4())

    @staticmethod
    def _is_valid_uuid(value: str) -> bool:
        try:
            uuid.UUID(value)
            return True
        except (ValueError, TypeError, AttributeError):
            return False
