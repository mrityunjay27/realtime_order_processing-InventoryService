"""
Execution-local logging context backed by contextvars.

The context holds the tracing IDs (correlation_id, event_id, event_type)
plus optional Kafka metadata for the work item currently being processed.

It is set at execution boundaries:
    HTTP request     -> correlation_id (event_id/event_type stay None)
    Kafka consumer   -> correlation_id + event_id + event_type (+ kafka meta)
    Outbox publisher -> correlation_id + event_id + event_type

Because it uses contextvars, one thread handling several Kafka messages
will never leak one message's IDs into the next as long as every consumer
clears the context when it is done.
"""

import contextvars

_correlation_id: contextvars.ContextVar = contextvars.ContextVar(
    "correlation_id", default=None
)
_event_id: contextvars.ContextVar = contextvars.ContextVar(
    "event_id", default=None
)
_event_type: contextvars.ContextVar = contextvars.ContextVar(
    "event_type", default=None
)
_kafka: contextvars.ContextVar = contextvars.ContextVar(
    "kafka_context", default=None
)


def set_context(
    correlation_id=None,
    event_id=None,
    event_type=None,
    kafka=None,
):
    if correlation_id is not None:
        _correlation_id.set(correlation_id)
    if event_id is not None:
        _event_id.set(event_id)
    if event_type is not None:
        _event_type.set(event_type)
    if kafka is not None:
        _kafka.set(kafka)


def set_correlation_id(value):
    _correlation_id.set(value)


def set_event_id(value):
    _event_id.set(value)


def set_event_type(value):
    _event_type.set(value)


def set_kafka(topic=None, partition=None, offset=None):
    _kafka.set(
        {
            "topic": topic,
            "partition": partition,
            "offset": offset,
        }
    )


def get_context() -> dict:
    return {
        "correlation_id": _correlation_id.get(),
        "event_id": _event_id.get(),
        "event_type": _event_type.get(),
        "kafka": _kafka.get(),
    }


def get_correlation_id():
    return _correlation_id.get()


def get_event_id():
    return _event_id.get()


def get_event_type():
    return _event_type.get()


def clear_context():
    _correlation_id.set(None)
    _event_id.set(None)
    _event_type.set(None)
    _kafka.set(None)
