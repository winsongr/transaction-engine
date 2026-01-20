from uuid import uuid4

import pytest
import pytest_asyncio
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from src.adapters.outbox import OutboxRepository
from src.adapters.repository import TransactionRepository
from src.adapters.tables import transactions_table, outbox_table
from src.adapters.event_envelope import EventEnvelope
from src.domain.fsm import TransactionState
from src.service_layer.handlers import TransactionService
from src.service_layer.unit_of_work import UnitOfWork


class TestTransactionLifecyclePublishesOutboxEvent:
    """
    Integration test: Full transaction lifecycle with outbox publishing.

    Scenario:
    - Create transaction → Start → Complete
    - Verify DB state is COMPLETED with correct version
    - Verify exactly 1 outbox row exists (unpublished)
    - Trigger outbox publisher
    - Verify Kafka producer called and row marked published
    """

    @pytest_asyncio.fixture
    async def setup_transaction(self, db_session: AsyncSession):
        """Create and complete a transaction, returning all test data."""
        idempotency_key = f"test-{uuid4()}"
        payload = {"type": "test_payment", "amount": 100}

        # Create repositories
        txn_repo = TransactionRepository(db_session)
        outbox_repo = OutboxRepository(db_session)

        # Create UoW manually for this test
        uow = UnitOfWork(db_session)
        uow.transactions = txn_repo
        uow.outbox = outbox_repo

        # Create service
        service = TransactionService(uow)

        # Step 1: Create transaction
        txn = await service.create_transaction(
            idempotency_key=idempotency_key,
            payload=payload,
        )
        initial_version = txn.version

        # Step 2: Start transaction
        txn = await service.start_transaction(txn.id)

        # Step 3: Complete transaction
        txn = await service.complete_transaction(
            txn.id,
            result={"status": "success"},
        )

        # Commit to persist
        await db_session.commit()

        return {
            "transaction_id": txn.id,
            "idempotency_key": idempotency_key,
            "initial_version": initial_version,
            "final_version": txn.version,
            "session": db_session,
        }

    @pytest.mark.asyncio
    async def test_transaction_commits_to_kafka_via_outbox(
        self,
        db_session: AsyncSession,
        mock_kafka_producer,
    ):
        idempotency_key = f"test-{uuid4()}"
        payload = {"type": "test_payment", "amount": 100}

        # Create repositories
        txn_repo = TransactionRepository(db_session)
        outbox_repo = OutboxRepository(db_session)

        # Create UoW manually for this test
        uow = UnitOfWork(db_session)
        uow.transactions = txn_repo
        uow.outbox = outbox_repo

        # Create service
        service = TransactionService(uow)

        # Create transaction
        txn = await service.create_transaction(
            idempotency_key=idempotency_key,
            payload=payload,
        )
        transaction_id = txn.id
        assert txn.state == TransactionState.CREATED
        assert txn.version == 1

        # Start transaction
        txn = await service.start_transaction(transaction_id)
        assert txn.state == TransactionState.PENDING
        assert txn.version == 2

        # Complete transaction
        txn = await service.complete_transaction(
            transaction_id,
            result={"status": "success"},
        )
        assert txn.state == TransactionState.COMPLETED
        assert txn.version == 3

        await db_session.flush()

        # DB state assertion
        result = await db_session.execute(
            select(transactions_table).where(transactions_table.c.id == transaction_id)
        )
        row = result.fetchone()
        assert row is not None
        assert row.state == "COMPLETED"
        assert row.version == 3

        # Outbox assertion
        outbox_result = await db_session.execute(
            select(outbox_table)
            .where(outbox_table.c.aggregate_id == transaction_id)
            .order_by(outbox_table.c.created_at)
        )
        outbox_rows = outbox_result.fetchall()
        assert len(outbox_rows) == 3

        completed_event = next(
            (r for r in outbox_rows if r.event_type == "TransactionCompleted"),
            None,
        )
        assert completed_event is not None
        assert completed_event.published_at is None

        # Simulate outbox publisher
        entries = await outbox_repo.get_unpublished(limit=10)
        assert len(entries) == 3

        for entry in entries:
            envelope = EventEnvelope.from_outbox_entry(entry)
            success = await mock_kafka_producer.publish(envelope)
            assert success is True
            await outbox_repo.mark_published(entry.id)

        await db_session.flush()

        assert mock_kafka_producer.publish.call_count == 3

        outbox_result = await db_session.execute(
            select(outbox_table).where(outbox_table.c.aggregate_id == transaction_id)
        )
        outbox_rows = outbox_result.fetchall()
        for row in outbox_rows:
            assert row.published_at is not None
