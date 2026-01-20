"""
SQLAlchemy Table Definitions.

These are used by repositories and migrations.
"""

from sqlalchemy import (
    Column,
    DateTime,
    Integer,
    MetaData,
    String,
    Table,
    Text,
    UniqueConstraint,
    Index,
)
from sqlalchemy.dialects.postgresql import JSONB, UUID as PG_UUID

metadata = MetaData()

# Transactions table (aggregate state)
transactions_table = Table(
    "transactions",
    metadata,
    Column("id", String, primary_key=True),
    Column("idempotency_key", String, unique=True, nullable=False),
    Column("state", String, nullable=False),
    Column("version", Integer, nullable=False),
    Column("payload", JSONB, nullable=True),
    Column("created_at", DateTime(timezone=True), nullable=False),
    Column("started_at", DateTime(timezone=True), nullable=True),
    Column("completed_at", DateTime(timezone=True), nullable=True),
    Column("failed_at", DateTime(timezone=True), nullable=True),
    Column("cancelled_at", DateTime(timezone=True), nullable=True),
    Column("result", JSONB, nullable=True),
    Column("error_code", String, nullable=True),
    Column("error_message", Text, nullable=True),
    Column("cancellation_reason", Text, nullable=True),
)

# Outbox table (transactional event publishing)
outbox_table = Table(
    "outbox",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("aggregate_id", String, nullable=False),
    Column("aggregate_type", String, nullable=False),
    Column("event_type", String, nullable=False),
    Column("event_id", PG_UUID(as_uuid=True), nullable=False, unique=True),
    Column("payload", JSONB, nullable=False),
    Column("created_at", DateTime(timezone=True), nullable=False),
    Column("published_at", DateTime(timezone=True), nullable=True),
    Index(
        "ix_outbox_unpublished", "created_at", postgresql_where="published_at IS NULL"
    ),
)

# Events table (append-only event log)
events_table = Table(
    "events",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("aggregate_id", String, nullable=False),
    Column("event_type", String, nullable=False),
    Column("version", Integer, nullable=False),
    Column("payload", JSONB, nullable=False),
    Column("created_at", DateTime(timezone=True), nullable=False),
    UniqueConstraint("aggregate_id", "version", name="uq_events_aggregate_version"),
    Index("ix_events_aggregate_id", "aggregate_id"),
)
