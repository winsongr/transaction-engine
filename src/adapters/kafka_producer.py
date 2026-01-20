import logging

from aiokafka import AIOKafkaProducer
from aiokafka.errors import KafkaError

from src.adapters.event_envelope import EventEnvelope

logger = logging.getLogger(__name__)


class KafkaProducerConfig:
    def __init__(
        self,
        bootstrap_servers: str = "localhost:9092",
        topic_prefix: str = "transaction-engine",
        acks: str = "all",  # Wait for all replicas
        enable_idempotence: bool = True,  # Exactly-once semantics
        max_batch_size: int = 16384,
        linger_ms: int = 5,
    ):
        self.bootstrap_servers = bootstrap_servers
        self.topic_prefix = topic_prefix
        self.acks = acks
        self.enable_idempotence = enable_idempotence
        self.max_batch_size = max_batch_size
        self.linger_ms = linger_ms

    def get_topic(self, event_type: str) -> str:
        """Derive topic name from event type."""
        return f"{self.topic_prefix}.events"


class KafkaProducerAdapter:
    """
    Kafka producer with explicit failure handling.

    Not a singleton - create per worker lifecycle.
    """

    def __init__(self, config: KafkaProducerConfig):
        self.config = config
        self._producer: AIOKafkaProducer | None = None

    async def start(self) -> None:
        """Start the producer connection."""
        self._producer = AIOKafkaProducer(
            bootstrap_servers=self.config.bootstrap_servers,
            acks=self.config.acks,
            enable_idempotence=self.config.enable_idempotence,
            max_batch_size=self.config.max_batch_size,
            linger_ms=self.config.linger_ms,
        )
        await self._producer.start()
        logger.info(
            "Kafka producer started",
            extra={"bootstrap_servers": self.config.bootstrap_servers},
        )

    async def stop(self) -> None:
        """Stop the producer connection."""
        if self._producer:
            await self._producer.stop()
            logger.info("Kafka producer stopped")

    async def publish(self, envelope: EventEnvelope) -> bool:
        """Publish event to Kafka. Returns success status."""
        if not self._producer:
            logger.error("Producer not started")
            return False

        topic = self.config.get_topic(envelope.event_type)
        key = envelope.to_kafka_key()
        value = envelope.to_kafka_value()

        try:
            # send_and_wait ensures broker ACK before returning
            await self._producer.send_and_wait(
                topic=topic,
                key=key,
                value=value,
            )
            logger.info(
                "Event published",
                extra={
                    "event_id": str(envelope.event_id),
                    "aggregate_id": envelope.aggregate_id,
                    "event_type": envelope.event_type,
                    "topic": topic,
                },
            )
            return True

        except KafkaError as e:
            logger.error(
                "Failed to publish event",
                extra={
                    "event_id": str(envelope.event_id),
                    "aggregate_id": envelope.aggregate_id,
                    "error": str(e),
                },
            )
            return False

    async def __aenter__(self) -> "KafkaProducerAdapter":
        await self.start()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        await self.stop()
