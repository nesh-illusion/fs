from dataclasses import dataclass
import os
from typing import Optional


@dataclass
class AppSettings:
    storage_backend: str = "memory"
    table_connection_string: Optional[str] = None
    jobs_table: str = "jobs"
    steps_table: str = "steps"
    outbox_table: str = "outbox"
    ledger_table: str = "job-ledger"
    idempotency_table: str = "job-idempotency"
    job_index_table: str = "job-index"
    admin_enabled: bool = False
    admin_api_key: Optional[str] = None
    admin_publish_enabled: bool = False
    service_bus_connection: Optional[str] = None
    outbox_publish_timeout_seconds: float = 2.0
    outbox_retry_enabled: bool = True
    outbox_retry_interval_seconds: float = 5.0
    outbox_retry_batch_size: int = 100
    max_inflight_per_tenant: int = 100
    inflight_table: str = "tenant-inflight"

    @classmethod
    def from_env(cls) -> "AppSettings":
        return cls(
            storage_backend=os.getenv("LUCIUS_STORAGE_BACKEND", "memory"),
            table_connection_string=os.getenv("LUCIUS_TABLE_CONNECTION"),
            jobs_table=os.getenv("LUCIUS_JOBS_TABLE", "jobs"),
            steps_table=os.getenv("LUCIUS_STEPS_TABLE", "steps"),
            outbox_table=os.getenv("LUCIUS_OUTBOX_TABLE", "outbox"),
            ledger_table=os.getenv("LUCIUS_LEDGER_TABLE", "job-ledger"),
            idempotency_table=os.getenv("LUCIUS_IDEMPOTENCY_TABLE", "job-idempotency"),
            job_index_table=os.getenv("LUCIUS_JOB_INDEX_TABLE", "job-index"),
            admin_enabled=os.getenv("LUCIUS_ADMIN_ENABLED", "false").lower() == "true",
            admin_api_key=os.getenv("LUCIUS_ADMIN_API_KEY"),
            admin_publish_enabled=os.getenv("LUCIUS_ADMIN_PUBLISH", "false").lower() == "true",
            service_bus_connection=os.getenv("LUCIUS_SERVICEBUS_CONNECTION"),
            outbox_publish_timeout_seconds=float(os.getenv("LUCIUS_OUTBOX_PUBLISH_TIMEOUT", "2.0")),
            outbox_retry_enabled=os.getenv("LUCIUS_OUTBOX_RETRY_ENABLED", "true").lower() == "true",
            outbox_retry_interval_seconds=float(os.getenv("LUCIUS_OUTBOX_RETRY_INTERVAL", "5.0")),
            outbox_retry_batch_size=int(os.getenv("LUCIUS_OUTBOX_RETRY_BATCH_SIZE", "100")),
            max_inflight_per_tenant=int(os.getenv("LUCIUS_MAX_INFLIGHT_PER_TENANT", "100")),
            inflight_table=os.getenv("LUCIUS_INFLIGHT_TABLE", "tenant-inflight"),
        )
