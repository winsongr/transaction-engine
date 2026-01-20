import asyncio
import logging
import signal
from datetime import datetime, timezone
from typing import NoReturn

from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker

from src.adapters.event_envelope import EventEnvelope
from src.adapters.kafka_producer import KafkaProducerAdapter, KafkaProducerConfig
from src.adapters.outbox import OutboxRepository

logger = logging.getLogger(__name__)


class OutboxPublisherConfig:
    def __init__(
        self,
        database_url: str,
        kafka_bootstrap_servers: str = "localhost:9092",
        kafka_topic_prefix: str = "transaction-engine",
        batch_size: int = 100,
        poll_interval_seconds: float = 1.0,
        backoff_seconds: float = 5.0,
    ):
        self.database_url = database_url
        self.kafka_bootstrap_servers = kafka_bootstrap_servers
        self.kafka_topic_prefix = kafka_topic_prefix
        self.batch_size = batch_size
        self.poll_interval_seconds = poll_interval_seconds
        self.backoff_seconds = backoff_seconds


class OutboxPublisher:
    def __init__(self, config: OutboxPublisherConfig):
        self.config = config
        self._running = False
        self._engine = create_async_engine(config.database_url, echo=False)
        self._session_factory = async_sessionmaker(
            self._engine,
            class_=AsyncSession,
            expire_on_commit=False,
        )
        self._producer: KafkaProducerAdapter | None = None

    async def start(self) -> None:
        kafka_config = KafkaProducerConfig(
            bootstrap_servers=self.config.kafka_bootstrap_servers,
            topic_prefix=self.config.kafka_topic_prefix,
        )
        self._producer = KafkaProducerAdapter(kafka_config)
        await self._producer.start()
        self._running = True
        logger.info(
            "Outbox publisher started",
            extra={
                "batch_size": self.config.batch_size,
                "poll_interval": self.config.poll_interval_seconds,
            },
        )

    async def stop(self) -> None:
        self._running = False
        if self._producer:
            await self._producer.stop()
        await self._engine.dispose()
        logger.info("Outbox publisher stopped")

    async def run(self) -> NoReturn:
        await self.start()

        try:
            while self._running:
                try:
                    published_count = await self._process_batch()

                    if published_count > 0:
                        logger.info(
                            "Batch processed",
                            extra={"published_count": published_count},
                        )
                        # No sleep if we published - check for more immediately
                        continue

                    # Nothing to publish - sleep before next poll
                    await asyncio.sleep(self.config.poll_interval_seconds)

                except Exception as e:
                    logger.error(
                        "Error processing batch",
                        extra={"error": str(e)},
                        exc_info=True,
                    )
                    # Backoff on error to avoid hot loop
                    await asyncio.sleep(self.config.backoff_seconds)

        finally:
            await self.stop()

    async def _process_batch(self) -> int:
        """Process one batch."""
        async with self._session_factory() as session:
            repo = OutboxRepository(session)

            # Get unpublished entries ordered by created_at
            entries = await repo.get_unpublished(limit=self.config.batch_size)

            if not entries:
                return 0

            # Log lag (oldest entry age)
            oldest = entries[0]
            lag_seconds = (
                datetime.now(timezone.utc) - oldest.created_at
            ).total_seconds()
            logger.debug(
                "Processing batch",
                extra={
                    "batch_size": len(entries),
                    "lag_seconds": round(lag_seconds, 2),
                },
            )

            published_count = 0

            for entry in entries:
                # Convert to wire format
                envelope = EventEnvelope.from_outbox_entry(entry)

                # Publish to Kafka
                success = await self._producer.publish(envelope)

                if success:
                    # Mark as published (commit per entry for safety)
                    await repo.mark_published(entry.id)
                    await session.commit()
                    published_count += 1
                else:
                    # Stop batch on first failure
                    # Remaining entries will be retried next poll
                    logger.warning(
                        "Stopping batch on publish failure",
                        extra={"event_id": str(entry.event_id)},
                    )
                    break

            return published_count


async def main() -> None:
    import os

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    # Configuration from environment
    database_url = os.getenv(
        "DATABASE_URL",
        "postgresql+asyncpg://postgres:4SFg5BhV50gg@localhost:5432/transaction_engine",
    )
    kafka_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:19092")

    config = OutboxPublisherConfig(
        database_url=database_url,
        kafka_bootstrap_servers=kafka_servers,
    )

    worker = OutboxPublisher(config)

    # Graceful shutdown on SIGINT/SIGTERM
    loop = asyncio.get_event_loop()

    def shutdown_handler():
        logger.info("Shutdown signal received")
        worker._running = False

    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, shutdown_handler)

    await worker.run()


if __name__ == "__main__":
    asyncio.run(main())
