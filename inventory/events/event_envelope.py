import json
import logging
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from uuid import uuid4

logger = logging.getLogger(__name__)


@dataclass
class EventEnvelope:
    event_id: str = field(default_factory=lambda: str(uuid4()))
    event_type: str = ""
    correlation_id: str = field(default_factory=lambda: str(uuid4()))
    occurred_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    payload: dict = field(default_factory=dict)

    def to_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict) -> "EventEnvelope":
        return cls(
            event_id=data["event_id"],
            event_type=data["event_type"],
            correlation_id=data["correlation_id"],
            occurred_at=data["occurred_at"],
            payload=data["payload"],
        )

    def to_json(self) -> str:
        return json.dumps(self.to_dict())

    @classmethod
    def from_json(cls, json_str: str) -> "EventEnvelope":
        return cls.from_dict(json.loads(json_str))
