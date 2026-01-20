from src.core.database import get_session
from fastapi import APIRouter, Depends
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncSession

from src.adapters.outbox import OutboxRepository

router = APIRouter(
    prefix="/admin",
    tags=["admin"],
)


class OutboxStatsResponse(BaseModel):
    pending_count: int
    oldest_pending_age_seconds: float | None


@router.get("/outbox/stats", response_model=OutboxStatsResponse)
async def get_outbox_stats(session: AsyncSession = Depends(get_session)):
    repo = OutboxRepository(session)
    stats = await repo.get_stats()
    return OutboxStatsResponse(**stats)
