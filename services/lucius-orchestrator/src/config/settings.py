from dataclasses import dataclass
import os
from typing import Optional


@dataclass
class AppSettings:
    storage_backend: str = "memory"
    table_connection_string: Optional[str] = None
    jobs_table: str = "jobs"
    steps_table: str = "steps"
    ledger_table: str = "job-ledger"
    idempotency_table: str = "job-idempotency"
    job_index_table: str = "job-index"
    max_inflight_per_tenant: int = 100
    inflight_table: str = "tenant-inflight"
    temporal_enabled: bool = True
    temporal_address: str = "localhost:7233"
    temporal_namespace: str = "default"
    temporal_task_queue: str = "lucius"
    temporal_workflow_execution_timeout_seconds: int = 3600

    @classmethod
    def from_env(cls) -> "AppSettings":
        return cls(
            storage_backend=os.getenv("LUCIUS_STORAGE_BACKEND", "memory"),
            table_connection_string=os.getenv("LUCIUS_TABLE_CONNECTION"),
            jobs_table=os.getenv("LUCIUS_JOBS_TABLE", "jobs"),
            steps_table=os.getenv("LUCIUS_STEPS_TABLE", "steps"),
            ledger_table=os.getenv("LUCIUS_LEDGER_TABLE", "job-ledger"),
            idempotency_table=os.getenv("LUCIUS_IDEMPOTENCY_TABLE", "job-idempotency"),
            job_index_table=os.getenv("LUCIUS_JOB_INDEX_TABLE", "job-index"),
            max_inflight_per_tenant=int(os.getenv("LUCIUS_MAX_INFLIGHT_PER_TENANT", "100")),
            inflight_table=os.getenv("LUCIUS_INFLIGHT_TABLE", "tenant-inflight"),
            temporal_enabled=os.getenv("LUCIUS_TEMPORAL_ENABLED", "true").lower() == "true",
            temporal_address=os.getenv("LUCIUS_TEMPORAL_ADDRESS", "localhost:7233"),
            temporal_namespace=os.getenv("LUCIUS_TEMPORAL_NAMESPACE", "default"),
            temporal_task_queue=os.getenv("LUCIUS_TEMPORAL_TASK_QUEUE", "lucius"),
            temporal_workflow_execution_timeout_seconds=int(
                os.getenv("LUCIUS_TEMPORAL_WORKFLOW_EXECUTION_TIMEOUT", "3600")
            ),
        )
