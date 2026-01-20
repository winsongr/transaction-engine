from contextlib import asynccontextmanager
from typing import AsyncGenerator
import logging

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.trustedhost import TrustedHostMiddleware

from src.core.settings import settings
from src.core.logging import setup_logging
from src.core.errors import setup_exception_handlers
from src.entrypoints.routes import router as transaction_router
from src.entrypoints.admin import router as admin_router
from src.service_layer.unit_of_work import UnitOfWorkFactory

setup_logging()
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    logger.info("Application starting up...")

    # Attach UoW factory for DI
    app.state.uow_factory = UnitOfWorkFactory(settings.SQLALCHEMY_DATABASE_URI)

    # Infra checks
    from redis.asyncio import Redis

    redis = Redis.from_url(settings.REDIS_URI)
    await redis.ping()
    await redis.close()

    from sqlalchemy.ext.asyncio import create_async_engine
    from sqlalchemy import text

    engine = create_async_engine(settings.SQLALCHEMY_DATABASE_URI)
    async with engine.connect() as conn:
        await conn.execute(text("SELECT 1"))
    await engine.dispose()

    yield
    logger.info("Application shutting down...")


def create_app() -> FastAPI:
    app = FastAPI(
        title=settings.APP_NAME,
        lifespan=lifespan,
        docs_url="/docs",
        redoc_url="/redoc",
    )

    app.add_middleware(
        TrustedHostMiddleware,
        allowed_hosts=settings.ALLOWED_HOSTS,
    )
    app.add_middleware(
        CORSMiddleware,
        allow_origins=settings.BACKEND_CORS_ORIGINS,
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    setup_exception_handlers(app)

    app.include_router(transaction_router, prefix="/api/v1")
    app.include_router(admin_router, prefix="/api/v1")

    @app.get("/health")
    async def health():
        return {"status": "ok"}

    return app


app = create_app()
