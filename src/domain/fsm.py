from enum import Enum
from typing import Set


class TransactionState(str, Enum):
    CREATED = "CREATED"
    PENDING = "PENDING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"


# Terminal states cannot transition to any other state
TERMINAL_STATES: Set[TransactionState] = {
    TransactionState.COMPLETED,
    TransactionState.FAILED,
    TransactionState.CANCELLED,
}

# Allowed transitions
TRANSITIONS: dict[TransactionState, Set[TransactionState]] = {
    TransactionState.CREATED: {
        TransactionState.PENDING,
        TransactionState.CANCELLED,
    },
    TransactionState.PENDING: {
        TransactionState.COMPLETED,
        TransactionState.FAILED,
        TransactionState.CANCELLED,
    },
    TransactionState.COMPLETED: set(),  # Terminal
    TransactionState.FAILED: set(),  # Terminal
    TransactionState.CANCELLED: set(),  # Terminal
}


class InvalidTransitionError(Exception):
    """Raised when an illegal state transition is attempted."""

    def __init__(self, from_state: TransactionState, to_state: TransactionState):
        self.from_state = from_state
        self.to_state = to_state
        super().__init__(f"Invalid transition: {from_state.value} → {to_state.value}")


def can_transition(from_state: TransactionState, to_state: TransactionState) -> bool:
    """Check if a transition is allowed without raising."""
    return to_state in TRANSITIONS.get(from_state, set())


def validate_transition(
    from_state: TransactionState, to_state: TransactionState
) -> None:
    if not can_transition(from_state, to_state):
        raise InvalidTransitionError(from_state, to_state)


def is_terminal(state: TransactionState) -> bool:
    """Check if a state is terminal (no further transitions allowed)."""
    return state in TERMINAL_STATES
