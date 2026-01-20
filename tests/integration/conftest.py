import asyncio
from typing import AsyncGenerator
from unittest.mock import AsyncMock, MagicMock

import pytest
import pytest_asyncio
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker

from src.adapters.outbox import OutboxRepository
from src.adapters.repository import TransactionRepository


import os

# Test database URL - uses same DB as dev but with rollback
TEST_DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql+asyncpg://postgres:4SFg5BhV50gg@localhost:5432/transaction_engine",
)


@pytest.fixture(scope="session")
def event_loop():
    """Create event loop for async tests."""
    loop = asyncio.new_event_loop()
    yield loop
    loop.close()


@pytest_asyncio.fixture(scope="function")
async def db_session() -> AsyncGenerator[AsyncSession, None]:
    """
    Async DB session with rollback per test.

    Each test runs in a transaction that is rolled back at the end.
    """
    engine = create_async_engine(TEST_DATABASE_URL, echo=False)
    session_factory = async_sessionmaker(
        engine,
        class_=AsyncSession,
        expire_on_commit=False,
    )

    async with session_factory() as session:
        async with session.begin():
            yield session
            # Rollback happens automatically when context exits without commit


@pytest_asyncio.fixture(scope="function")
async def transaction_repo(db_session: AsyncSession) -> TransactionRepository:
    """Transaction repository using test session."""
    return TransactionRepository(db_session)


@pytest_asyncio.fixture(scope="function")
async def outbox_repo(db_session: AsyncSession) -> OutboxRepository:
    """Outbox repository using test session."""
    return OutboxRepository(db_session)


@pytest.fixture
def mock_kafka_producer() -> MagicMock:
    """
    Mock Kafka producer for testing.

    Captures all published events without hitting real Kafka.
    """
    producer = MagicMock()
    producer.start = AsyncMock()
    producer.stop = AsyncMock()
    producer.publish = AsyncMock(return_value=True)
    return producer
