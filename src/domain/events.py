from datetime import datetime, timezone
from typing import Any
from uuid import UUID, uuid4

from pydantic import BaseModel, ConfigDict, Field


class DomainEvent(BaseModel):
    """Base domain event."""

    event_id: UUID = Field(default_factory=uuid4)
    aggregate_id: str
    aggregate_type: str = "Transaction"
    version: int
    occurred_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

    model_config = ConfigDict(frozen=True)


class TransactionCreated(DomainEvent):
    event_type: str = "TransactionCreated"
    payload: dict[str, Any]
    idempotency_key: str


class TransactionStarted(DomainEvent):
    event_type: str = "TransactionStarted"
    started_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))


class TransactionCompleted(DomainEvent):
    event_type: str = "TransactionCompleted"
    completed_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    result: dict[str, Any] | None = None


class TransactionFailed(DomainEvent):
    event_type: str = "TransactionFailed"
    failed_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    error_code: str
    error_message: str


class TransactionCancelled(DomainEvent):
    event_type: str = "TransactionCancelled"
    cancelled_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    cancellation_reason: str | None = None
