from typing import Any

from fastapi import APIRouter, Depends, Header, HTTPException, status
from pydantic import BaseModel, Field

from src.domain.fsm import InvalidTransitionError
from src.domain.invariants import InvariantViolationError
from src.entrypoints.dependencies import get_uow
from src.service_layer.handlers import TransactionService
from src.service_layer.unit_of_work import UnitOfWork


router = APIRouter(prefix="/transactions", tags=["transactions"])


# --- Request/Response Schemas ---


class CreateTransactionRequest(BaseModel):
    type: str = Field(..., description="Transaction type")
    payload: dict[str, Any] = Field(
        default_factory=dict, description="Domain-specific data"
    )


class TransactionResponse(BaseModel):
    id: str
    status: str
    version: int
    created_at: str
    idempotency_key: str


class CompleteTransactionRequest(BaseModel):
    result: dict[str, Any] | None = None


class FailTransactionRequest(BaseModel):
    error_code: str
    error_message: str


class CancelTransactionRequest(BaseModel):
    reason: str | None = None


class ErrorResponse(BaseModel):
    error_code: str
    message: str
    request_id: str | None = None


# --- Dependency ---


async def get_transaction_service(
    uow: UnitOfWork = Depends(get_uow),
) -> TransactionService:
    return TransactionService(uow)


# --- Helper ---


def to_response(txn) -> TransactionResponse:
    return TransactionResponse(
        id=txn.id,
        status=txn.state.value,
        version=txn.version,
        created_at=txn.created_at.isoformat(),
        idempotency_key=txn.idempotency_key,
    )


# --- Endpoints ---


@router.post(
    "",
    status_code=status.HTTP_201_CREATED,
    response_model=TransactionResponse,
    responses={
        400: {"model": ErrorResponse, "description": "Missing idempotency key"},
        409: {"model": ErrorResponse, "description": "Idempotency conflict"},
    },
)
async def create_transaction(
    request: CreateTransactionRequest,
    x_idempotency_key: str = Header(..., alias="X-Idempotency-Key"),
    service: TransactionService = Depends(get_transaction_service),
) -> TransactionResponse:
    txn = await service.create_transaction(
        idempotency_key=x_idempotency_key,
        payload={"type": request.type, **request.payload},
    )
    return to_response(txn)


@router.post(
    "/{transaction_id}/start",
    response_model=TransactionResponse,
    responses={
        404: {"model": ErrorResponse, "description": "Transaction not found"},
        422: {"model": ErrorResponse, "description": "Invalid state transition"},
    },
)
async def start_transaction(
    transaction_id: str,
    service: TransactionService = Depends(get_transaction_service),
) -> TransactionResponse:
    try:
        txn = await service.start_transaction(transaction_id)
        return to_response(txn)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except (InvalidTransitionError, InvariantViolationError) as e:
        raise HTTPException(status_code=422, detail=str(e))


@router.post(
    "/{transaction_id}/complete",
    response_model=TransactionResponse,
    responses={
        404: {"model": ErrorResponse, "description": "Transaction not found"},
        422: {"model": ErrorResponse, "description": "Invalid state transition"},
    },
)
async def complete_transaction(
    transaction_id: str,
    request: CompleteTransactionRequest,
    service: TransactionService = Depends(get_transaction_service),
) -> TransactionResponse:
    try:
        txn = await service.complete_transaction(
            transaction_id,
            result=request.result,
        )
        return to_response(txn)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except (InvalidTransitionError, InvariantViolationError) as e:
        raise HTTPException(status_code=422, detail=str(e))


@router.post(
    "/{transaction_id}/fail",
    response_model=TransactionResponse,
    responses={
        404: {"model": ErrorResponse, "description": "Transaction not found"},
        422: {"model": ErrorResponse, "description": "Invalid state transition"},
    },
)
async def fail_transaction(
    transaction_id: str,
    request: FailTransactionRequest,
    service: TransactionService = Depends(get_transaction_service),
) -> TransactionResponse:
    try:
        txn = await service.fail_transaction(
            transaction_id,
            error_code=request.error_code,
            error_message=request.error_message,
        )
        return to_response(txn)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except (InvalidTransitionError, InvariantViolationError) as e:
        raise HTTPException(status_code=422, detail=str(e))


@router.post(
    "/{transaction_id}/cancel",
    response_model=TransactionResponse,
    responses={
        404: {"model": ErrorResponse, "description": "Transaction not found"},
        422: {"model": ErrorResponse, "description": "Invalid state transition"},
    },
)
async def cancel_transaction(
    transaction_id: str,
    request: CancelTransactionRequest,
    service: TransactionService = Depends(get_transaction_service),
) -> TransactionResponse:
    try:
        txn = await service.cancel_transaction(
            transaction_id,
            reason=request.reason,
        )
        return to_response(txn)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except (InvalidTransitionError, InvariantViolationError) as e:
        raise HTTPException(status_code=422, detail=str(e))


@router.get(
    "/{transaction_id}",
    response_model=TransactionResponse,
    responses={
        404: {"model": ErrorResponse, "description": "Transaction not found"},
    },
)
async def get_transaction(
    transaction_id: str,
    service: TransactionService = Depends(get_transaction_service),
) -> TransactionResponse:
    txn = await service.get_transaction(transaction_id)
    if not txn:
        raise HTTPException(
            status_code=404, detail=f"Transaction {transaction_id} not found"
        )
    return to_response(txn)
