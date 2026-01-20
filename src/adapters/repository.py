from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from src.domain.models import Transaction
from src.domain.fsm import TransactionState
from src.domain.invariants import InvariantViolationError


class TransactionRepository:
    def __init__(self, session: AsyncSession):
        self.session = session

    async def get_by_id(self, transaction_id: str) -> Transaction | None:
        from src.adapters.tables import transactions_table

        result = await self.session.execute(
            select(transactions_table).where(transactions_table.c.id == transaction_id)
        )
        row = result.fetchone()
        if not row:
            return None

        return Transaction(
            id=row.id,
            idempotency_key=row.idempotency_key,
            state=TransactionState(row.state),
            version=row.version,
            payload=row.payload or {},
            created_at=row.created_at,
            started_at=row.started_at,
            completed_at=row.completed_at,
            failed_at=row.failed_at,
            cancelled_at=row.cancelled_at,
            result=row.result,
            error_code=row.error_code,
            error_message=row.error_message,
            cancellation_reason=row.cancellation_reason,
        )

    async def get_by_idempotency_key(self, key: str) -> Transaction | None:
        from src.adapters.tables import transactions_table

        result = await self.session.execute(
            select(transactions_table).where(
                transactions_table.c.idempotency_key == key
            )
        )
        row = result.fetchone()
        if not row:
            return None

        return Transaction(
            id=row.id,
            idempotency_key=row.idempotency_key,
            state=TransactionState(row.state),
            version=row.version,
            payload=row.payload or {},
            created_at=row.created_at,
            started_at=row.started_at,
            completed_at=row.completed_at,
            failed_at=row.failed_at,
            cancelled_at=row.cancelled_at,
            result=row.result,
            error_code=row.error_code,
            error_message=row.error_message,
            cancellation_reason=row.cancellation_reason,
        )

    async def get_by_id_for_update(self, transaction_id: str) -> Transaction | None:
        from src.adapters.tables import transactions_table

        result = await self.session.execute(
            select(transactions_table)
            .where(transactions_table.c.id == transaction_id)
            .with_for_update()
        )
        row = result.fetchone()
        if not row:
            return None

        return Transaction(
            id=row.id,
            idempotency_key=row.idempotency_key,
            state=TransactionState(row.state),
            version=row.version,
            payload=row.payload or {},
            created_at=row.created_at,
            started_at=row.started_at,
            completed_at=row.completed_at,
            failed_at=row.failed_at,
            cancelled_at=row.cancelled_at,
            result=row.result,
            error_code=row.error_code,
            error_message=row.error_message,
            cancellation_reason=row.cancellation_reason,
        )

    async def add(self, transaction: Transaction) -> None:
        from src.adapters.tables import transactions_table

        await self.session.execute(
            transactions_table.insert().values(
                id=transaction.id,
                idempotency_key=transaction.idempotency_key,
                state=transaction.state.value,
                version=transaction.version,
                payload=transaction.payload,
                created_at=transaction.created_at,
                started_at=transaction.started_at,
                completed_at=transaction.completed_at,
                failed_at=transaction.failed_at,
                cancelled_at=transaction.cancelled_at,
                result=transaction.result,
                error_code=transaction.error_code,
                error_message=transaction.error_message,
                cancellation_reason=transaction.cancellation_reason,
            )
        )

    async def update(self, transaction: Transaction, expected_version: int) -> None:
        from src.adapters.tables import transactions_table

        result = await self.session.execute(
            transactions_table.update()
            .where(transactions_table.c.id == transaction.id)
            .where(transactions_table.c.version == expected_version)
            .values(
                state=transaction.state.value,
                version=transaction.version,
                started_at=transaction.started_at,
                completed_at=transaction.completed_at,
                failed_at=transaction.failed_at,
                cancelled_at=transaction.cancelled_at,
                result=transaction.result,
                error_code=transaction.error_code,
                error_message=transaction.error_message,
                cancellation_reason=transaction.cancellation_reason,
            )
        )

        if result.rowcount == 0:
            raise InvariantViolationError(
                "VERSION_MONOTONICITY",
                f"Concurrent modification detected for transaction {transaction.id}",
            )
