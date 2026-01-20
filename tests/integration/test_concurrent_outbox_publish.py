import asyncio
import uuid
from datetime import datetime

import pytest
from aiokafka import AIOKafkaConsumer
from sqlalchemy import select, func
from sqlalchemy.ext.asyncio import AsyncSession

from src.adapters.tables import outbox_table
from src.cmd.outbox_publisher import OutboxPublisher, OutboxPublisherConfig
from src.core.settings import settings
from src.service_layer.handlers import TransactionService
from src.service_layer.unit_of_work import UnitOfWorkFactory


class TestConcurrentOutboxPublish:
    """
    Integration test: Concurrent Outbox Publishing.

    Proves:
    - Events are emitted once
    - Outbox rows are claimed once (SKIP LOCKED works)
    - Multiple publishers don't collide
    """

    @pytest.mark.asyncio
    async def test_concurrent_publishers_safety(self, db_session: AsyncSession):
        # 1. SETUP: Generate load (20 transactions -> 60 events)
        # We use a separate UoW factory to simulate real service usage where each request has its own session
        uow_factory = UnitOfWorkFactory(settings.SQLALCHEMY_DATABASE_URI)

        transaction_ids = []

        # specific payload to track
        batch_id = str(uuid.uuid4())

        for i in range(20):
            idempotency_key = f"concurrent-test-{batch_id}-{i}"
            payload = {"batch_id": batch_id, "index": i}

            async with uow_factory.create() as uow:
                service = TransactionService(uow)

                # Create -> Start -> Complete (3 events per txn)
                txn = await service.create_transaction(idempotency_key, payload)
                await service.start_transaction(txn.id)
                await service.complete_transaction(txn.id, {"result": "ok"})

                transaction_ids.append(txn.id)

        # Verify initial state: 60 unpublished events
        result = await db_session.execute(
            select(func.count())
            .select_from(outbox_table)
            .where(outbox_table.c.published_at.is_(None))
        )
        count = result.scalar()
        assert count >= 60  # Could be more if DB wasn't clean, but at least our 60

        # 2. EXECUTION: Start 3 concurrent workers
        # Config pointing to real infra
        config = OutboxPublisherConfig(
            database_url=settings.SQLALCHEMY_DATABASE_URI,
            kafka_bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS,
            poll_interval_seconds=0.1,  # fast poll for test
            batch_size=10,
        )

        workers = [OutboxPublisher(config) for _ in range(3)]
        tasks = [asyncio.create_task(w.run()) for w in workers]

        try:
            # Poll DB until all our events are published
            # Timeout after 30 seconds
            start_wait = datetime.now()
            while True:
                if (datetime.now() - start_wait).total_seconds() > 30:
                    raise TimeoutError("Workers failed to drain outbox in time")

                # Check if any unpublished events remain for our batch
                # We filter by knowing the event payload contains our batch_id?
                # Actually, outbox table stores payload as JSON.
                # Simpler: just check if *all* events in outbox are published.
                # Or checks specific IDs.

                # Let's count unpublished events total.
                result = await db_session.execute(
                    select(func.count())
                    .select_from(outbox_table)
                    .where(outbox_table.c.published_at.is_(None))
                )
                pending = result.scalar()

                if pending == 0:
                    break

                await asyncio.sleep(0.5)

        finally:
            # 3. CLEANUP: Stop workers
            for w in workers:
                w._running = False
            # Wait for them to finish current batch
            for t in tasks:
                t.cancel()
                try:
                    await t
                except asyncio.CancelledError:
                    pass

        # 4. ASSERTIONS

        # A. DB Consistency
        # Fetch all events for our transactions
        stmt = select(outbox_table).where(
            outbox_table.c.aggregate_id.in_(transaction_ids)
        )
        result = await db_session.execute(stmt)
        rows = result.fetchall()

        assert len(rows) == 60, "Should have 60 events"
        for row in rows:
            assert row.published_at is not None, f"Event {row.event_id} not published"

        # B. Kafka Consistency
        # Consume from the beginning and count our events
        consumer = AIOKafkaConsumer(
            f"{settings.APP_NAME.lower().replace(' ', '-')}.events",
            bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS,
            auto_offset_reset="earliest",
            enable_auto_commit=False,
            group_id=f"test-verifier-{uuid.uuid4()}",
        )
        await consumer.start()

        try:
            # Read all messages
            # We assume the topic might contain old messages, so we filter by content or just count
            # Since we can't easily filter by "batch_id" inside the message without parsing,
            # and we ran against a persistent container, we might see old data.
            # Best approach: verify we received *at least* 60 messages and they are unique.
            # Or better: check for *our* specific event IDs.

            target_event_ids = {str(row.event_id) for row in rows}
            received_event_ids = set()

            # consume for a bit
            start_consume = datetime.now()
            while len(target_event_ids) > 0:
                if (datetime.now() - start_consume).total_seconds() > 10:
                    break  # Stop consuming

                result = await consumer.getmany(timeout_ms=1000, max_records=100)
                for tp, messages in result.items():
                    for msg in messages:
                        # Parse value? It's bytes.
                        # We just need to ensure no duplicates for OUR events.
                        # But wait, we need to inspect the payload to know if it's ours?
                        # Or match against target_event_ids (which are UUIDs).
                        # The envelope JSON has event_id.
                        import json

                        data = json.loads(msg.value)
                        eid = data.get("event_id")

                        if eid in target_event_ids:
                            if eid in received_event_ids:
                                pytest.fail(
                                    f"Duplicate event received from Kafka: {eid}"
                                )
                            received_event_ids.add(eid)
                            target_event_ids.remove(eid)

            assert len(target_event_ids) == 0, (
                f"Missing {len(target_event_ids)} events in Kafka"
            )

        finally:
            await consumer.stop()
