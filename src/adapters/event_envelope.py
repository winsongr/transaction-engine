from datetime import datetime
from typing import TYPE_CHECKING
from uuid import UUID

from pydantic import BaseModel

if TYPE_CHECKING:
    from src.adapters.outbox import OutboxEntry


class EventEnvelope(BaseModel):
    event_id: UUID
    aggregate_id: str
    aggregate_type: str
    event_type: str
    version: int
    occurred_at: datetime
    payload: dict

    def to_kafka_key(self) -> bytes:
        """Kafka message key (aggregate_id)."""
        return self.aggregate_id.encode("utf-8")

    def to_kafka_value(self) -> bytes:
        """Kafka message value as JSON bytes."""
        return self.model_dump_json().encode("utf-8")

    @classmethod
    def from_outbox_entry(cls, entry: "OutboxEntry") -> "EventEnvelope":
        """Create envelope from outbox entry."""

        return cls(
            event_id=entry.event_id,
            aggregate_id=entry.aggregate_id,
            aggregate_type=entry.aggregate_type,
            event_type=entry.event_type,
            version=entry.payload.get("version", 0),
            occurred_at=entry.created_at,
            payload=entry.payload,
        )
