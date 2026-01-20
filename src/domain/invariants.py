from typing import TYPE_CHECKING, Any

from src.domain.fsm import (
    TransactionState,
    TERMINAL_STATES,
    validate_transition,
)

if TYPE_CHECKING:
    from src.domain.models import Transaction


class InvariantViolationError(Exception):
    """Raised when a domain invariant is violated."""

    def __init__(self, invariant: str, details: str):
        self.invariant = invariant
        self.details = details
        super().__init__(f"Invariant violated [{invariant}]: {details}")


def assert_single_settlement(
    current_state: TransactionState,
    completed_at: Any,
    transaction_id: str,
) -> None:
    """Invariant: Transaction can only reach COMPLETED once."""
    if current_state == TransactionState.COMPLETED and completed_at:
        raise InvariantViolationError(
            "SINGLE_SETTLEMENT",
            f"Transaction {transaction_id} is already completed",
        )


def assert_terminal_finality(
    current_state: TransactionState,
    transaction_id: str,
) -> None:
    """Invariant: Terminal states (COMPLETED, FAILED, CANCELLED) cannot transition."""
    if current_state in TERMINAL_STATES:
        raise InvariantViolationError(
            "TERMINAL_FINALITY",
            f"Transaction {transaction_id} is in terminal state {current_state.value}",
        )


def assert_version_monotonicity(current_version: int, expected_version: int) -> None:
    """Invariant: Aggregate version must strictly increase."""
    if current_version != expected_version:
        raise InvariantViolationError(
            "VERSION_MONOTONICITY",
            f"Expected version {expected_version}, got {current_version}",
        )


def assert_idempotency_uniqueness(existing_key: str | None, new_key: str) -> None:
    """Invariant: Idempotency keys must be unique."""
    if existing_key and existing_key == new_key:
        raise InvariantViolationError(
            "IDEMPOTENCY_UNIQUENESS",
            f"Idempotency key {new_key} already exists",
        )


# --- Composite Checks ---


def validate_state_change(
    transaction: "Transaction",
    target_state: TransactionState,
) -> None:
    """Validate all invariants and FSM rules."""
    # Check terminal finality first
    assert_terminal_finality(transaction.state, transaction.id)

    # Validate FSM transition
    validate_transition(transaction.state, target_state)

    # Specific checks per target state
    if target_state == TransactionState.COMPLETED:
        assert_single_settlement(
            transaction.state,
            transaction.completed_at,
            transaction.id,
        )
