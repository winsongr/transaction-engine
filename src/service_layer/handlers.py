from typing import Any

from src.domain.models import Transaction
from src.service_layer.unit_of_work import UnitOfWork


class TransactionService:
    def __init__(self, uow: UnitOfWork):
        self.uow = uow

    async def create_transaction(
        self,
        idempotency_key: str,
        payload: dict[str, Any],
    ) -> Transaction:
        existing = await self.uow.transactions.get_by_idempotency_key(idempotency_key)
        if existing:
            return existing

        # Create new transaction
        txn = Transaction.create(idempotency_key=idempotency_key, payload=payload)

        # Persist aggregate
        await self.uow.transactions.add(txn)

        # Persist events
        events = txn.collect_events()
        await self.uow.persist_events(events)

        return txn

    async def start_transaction(self, transaction_id: str) -> Transaction:
        txn = await self.uow.transactions.get_by_id_for_update(transaction_id)
        if not txn:
            raise ValueError(f"Transaction {transaction_id} not found")

        expected_version = txn.version
        txn.start()  # Validation happens inside domain

        await self.uow.transactions.update(txn, expected_version)
        events = txn.collect_events()
        await self.uow.persist_events(events)

        return txn

    async def complete_transaction(
        self,
        transaction_id: str,
        result: dict[str, Any] | None = None,
    ) -> Transaction:
        txn = await self.uow.transactions.get_by_id_for_update(transaction_id)
        if not txn:
            raise ValueError(f"Transaction {transaction_id} not found")

        expected_version = txn.version
        txn.complete(result=result)  # Validation happens inside domain

        await self.uow.transactions.update(txn, expected_version)
        events = txn.collect_events()
        await self.uow.persist_events(events)

        return txn

    async def fail_transaction(
        self,
        transaction_id: str,
        error_code: str,
        error_message: str,
    ) -> Transaction:
        txn = await self.uow.transactions.get_by_id_for_update(transaction_id)
        if not txn:
            raise ValueError(f"Transaction {transaction_id} not found")

        expected_version = txn.version
        txn.fail(
            error_code=error_code, error_message=error_message
        )  # Validation inside domain

        await self.uow.transactions.update(txn, expected_version)
        events = txn.collect_events()
        await self.uow.persist_events(events)

        return txn

    async def cancel_transaction(
        self,
        transaction_id: str,
        reason: str | None = None,
    ) -> Transaction:
        txn = await self.uow.transactions.get_by_id_for_update(transaction_id)
        if not txn:
            raise ValueError(f"Transaction {transaction_id} not found")

        expected_version = txn.version
        txn.cancel(reason=reason)  # Validation inside domain

        await self.uow.transactions.update(txn, expected_version)
        events = txn.collect_events()
        await self.uow.persist_events(events)

        return txn

    async def get_transaction(self, transaction_id: str) -> Transaction | None:
        return await self.uow.transactions.get_by_id(transaction_id)
