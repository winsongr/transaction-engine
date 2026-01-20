from datetime import datetime, timezone
from typing import Any
from uuid import UUID

from pydantic import BaseModel, Field
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select

from src.domain.events import DomainEvent


class OutboxEntry(BaseModel):
    id: int | None = None
    aggregate_id: str
    aggregate_type: str
    event_type: str
    event_id: UUID
    payload: dict[str, Any]
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    published_at: datetime | None = None


class OutboxRepository:
    """Transactional outbox operations."""

    def __init__(self, session: AsyncSession):
        self.session = session

    async def add(self, event: DomainEvent) -> None:
        from src.adapters.tables import outbox_table

        await self.session.execute(
            outbox_table.insert().values(
                aggregate_id=event.aggregate_id,
                aggregate_type=event.aggregate_type,
                event_type=event.event_type,
                event_id=event.event_id,
                payload=event.model_dump(mode="json"),
                created_at=event.occurred_at,
            )
        )

    async def get_unpublished(self, limit: int = 100) -> list[OutboxEntry]:
        """Get unpublished entries (SKIP LOCKED)."""
        from src.adapters.tables import outbox_table

        result = await self.session.execute(
            select(outbox_table)
            .where(outbox_table.c.published_at.is_(None))
            .order_by(outbox_table.c.created_at)
            .limit(limit)
            # NOTE: SKIP LOCKED prevents head-of-line blocking but ignores rows locked by long-running txns.
            # Acceptable trade-off for throughput.
            .with_for_update(skip_locked=True)
        )
        rows = result.fetchall()
        return [
            OutboxEntry(
                id=row.id,
                aggregate_id=row.aggregate_id,
                aggregate_type=row.aggregate_type,
                event_type=row.event_type,
                event_id=row.event_id,
                payload=row.payload,
                created_at=row.created_at,
                published_at=row.published_at,
            )
            for row in rows
        ]

    async def mark_published(self, entry_id: int) -> None:
        """Mark an outbox entry as published."""
        from src.adapters.tables import outbox_table

        await self.session.execute(
            outbox_table.update()
            .where(outbox_table.c.id == entry_id)
            .values(published_at=datetime.now(timezone.utc))
        )

    async def get_stats(self) -> dict:
        """
        Get outbox statistics for monitoring.

        Returns:
            - pending_count: number of unpublished entries
            - oldest_pending_age_seconds: age of oldest unpublished entry (or None)
        """
        from sqlalchemy import func

        from src.adapters.tables import outbox_table

        # Count unpublished
        count_result = await self.session.execute(
            select(func.count()).where(outbox_table.c.published_at.is_(None))
        )
        pending_count = count_result.scalar() or 0

        # Get oldest unpublished
        oldest_result = await self.session.execute(
            select(func.min(outbox_table.c.created_at)).where(
                outbox_table.c.published_at.is_(None)
            )
        )
        oldest_created_at = oldest_result.scalar()

        oldest_age_seconds = None
        if oldest_created_at:
            oldest_age_seconds = (
                datetime.now(timezone.utc) - oldest_created_at
            ).total_seconds()

        return {
            "pending_count": pending_count,
            "oldest_pending_age_seconds": oldest_age_seconds,
        }
