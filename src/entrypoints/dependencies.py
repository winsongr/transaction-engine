from typing import AsyncGenerator
from fastapi import Request
from src.service_layer.unit_of_work import UnitOfWork, UnitOfWorkFactory


async def get_uow(request: Request) -> AsyncGenerator[UnitOfWork, None]:
    uow_factory: UnitOfWorkFactory = request.app.state.uow_factory
    async with uow_factory.create() as uow:
        yield uow
