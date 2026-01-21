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
    idempotency_table: str = "job-idempotency"
    job_index_table: str = "job-index"

    @classmethod
    def from_env(cls) -> "AppSettings":
        return cls(
            storage_backend=os.getenv("LUCIUS_STORAGE_BACKEND", "memory"),
            table_connection_string=os.getenv("LUCIUS_TABLE_CONNECTION"),
            jobs_table=os.getenv("LUCIUS_JOBS_TABLE", "jobs"),
            steps_table=os.getenv("LUCIUS_STEPS_TABLE", "steps"),
            outbox_table=os.getenv("LUCIUS_OUTBOX_TABLE", "outbox"),
            idempotency_table=os.getenv("LUCIUS_IDEMPOTENCY_TABLE", "job-idempotency"),
            job_index_table=os.getenv("LUCIUS_JOB_INDEX_TABLE", "job-index"),
        )
