from typing import AsyncGenerator
from contextlib import asynccontextmanager

from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine, async_sessionmaker

from src.adapters.repository import TransactionRepository
from src.adapters.outbox import OutboxRepository
from src.domain.events import DomainEvent


class UnitOfWork:
    """Atomic transaction coordinator."""

    def __init__(self, session: AsyncSession):
        self.session = session
        self.transactions = TransactionRepository(session)
        self.outbox = OutboxRepository(session)
        self._committed = False

    async def commit(self) -> None:
        await self.session.commit()
        self._committed = True

    async def rollback(self) -> None:
        await self.session.rollback()

    async def persist_events(self, events: list[DomainEvent]) -> None:
        from src.adapters.tables import events_table

        for event in events:
            # Insert into events table (source of truth)
            await self.session.execute(
                events_table.insert().values(
                    aggregate_id=event.aggregate_id,
                    event_type=event.event_type,
                    version=event.version,
                    payload=event.model_dump(mode="json"),
                    created_at=event.occurred_at,
                )
            )
            # Insert into outbox (for publishing)
            await self.outbox.add(event)


class UnitOfWorkFactory:
    def __init__(self, database_url: str):
        self.engine = create_async_engine(database_url, echo=False)
        self.session_factory = async_sessionmaker(
            self.engine,
            class_=AsyncSession,
            expire_on_commit=False,
        )

    @asynccontextmanager
    async def create(self) -> AsyncGenerator[UnitOfWork, None]:
        """Transactional context manager. Auto-commit on success, rollback on error."""
        async with self.session_factory() as session:
            async with session.begin():
                uow = UnitOfWork(session)
                try:
                    yield uow
                except Exception:
                    await uow.rollback()
                    raise
