from datetime import datetime, timezone
from typing import Any
from uuid import uuid4

from pydantic import BaseModel, Field, PrivateAttr

from src.domain.events import (
    DomainEvent,
    TransactionCancelled,
    TransactionCompleted,
    TransactionCreated,
    TransactionFailed,
    TransactionStarted,
)
from src.domain.fsm import TransactionState, is_terminal
from src.domain.invariants import validate_state_change


class Transaction(BaseModel):
    """Transaction aggregate."""

    id: str = Field(default_factory=lambda: str(uuid4()))
    idempotency_key: str
    state: TransactionState = TransactionState.CREATED
    version: int = 1
    payload: dict[str, Any] = Field(default_factory=dict)

    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    started_at: datetime | None = None
    completed_at: datetime | None = None
    failed_at: datetime | None = None
    cancelled_at: datetime | None = None

    result: dict[str, Any] | None = None
    error_code: str | None = None
    error_message: str | None = None
    cancellation_reason: str | None = None

    _pending_events: list[DomainEvent] = PrivateAttr(default_factory=list)

    def __init__(self, **data):
        super().__init__(**data)
        # Per-instance event list (avoids class-level mutable default)
        # Per-instance event list initialized via PrivateAttr

    def _apply_transition(self, new_state: TransactionState) -> None:
        validate_state_change(self, new_state)
        self.state = new_state
        self.version += 1

    def _record_event(self, event: DomainEvent) -> None:
        self._pending_events.append(event)

    def collect_events(self) -> list[DomainEvent]:
        """Collect and clear pending events."""
        events = self._pending_events.copy()
        self._pending_events.clear()
        return events

    # --- State Transitions ---

    @classmethod
    def create(cls, idempotency_key: str, payload: dict[str, Any]) -> "Transaction":
        txn = cls(idempotency_key=idempotency_key, payload=payload)
        txn._record_event(
            TransactionCreated(
                aggregate_id=txn.id,
                version=txn.version,
                payload=payload,
                idempotency_key=idempotency_key,
            )
        )
        return txn

    def start(self) -> None:
        self._apply_transition(TransactionState.PENDING)
        self.started_at = datetime.now(timezone.utc)
        self._record_event(
            TransactionStarted(
                aggregate_id=self.id,
                version=self.version,
                started_at=self.started_at,
            )
        )

    def complete(self, result: dict[str, Any] | None = None) -> None:
        self._apply_transition(TransactionState.COMPLETED)
        self.completed_at = datetime.now(timezone.utc)
        self.result = result
        self._record_event(
            TransactionCompleted(
                aggregate_id=self.id,
                version=self.version,
                completed_at=self.completed_at,
                result=result,
            )
        )

    def fail(self, error_code: str, error_message: str) -> None:
        self._apply_transition(TransactionState.FAILED)
        self.failed_at = datetime.now(timezone.utc)
        self.error_code = error_code
        self.error_message = error_message
        self._record_event(
            TransactionFailed(
                aggregate_id=self.id,
                version=self.version,
                failed_at=self.failed_at,
                error_code=error_code,
                error_message=error_message,
            )
        )

    def cancel(self, reason: str | None = None) -> None:
        self._apply_transition(TransactionState.CANCELLED)
        self.cancelled_at = datetime.now(timezone.utc)
        self.cancellation_reason = reason
        self._record_event(
            TransactionCancelled(
                aggregate_id=self.id,
                version=self.version,
                cancelled_at=self.cancelled_at,
                cancellation_reason=reason,
            )
        )

    @property
    def is_terminal(self) -> bool:
        """Check if transaction is in a terminal state."""
        return is_terminal(self.state)
