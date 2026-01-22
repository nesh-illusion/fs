from typing import List, Optional, Protocol

from .models import Job, JobIdempotency, JobIndex, OutboxEntry, Step


class JobsStore(Protocol):
    def create_job(self, job: Job) -> Job:
        ...

    def get_job(self, job_id: str, tenant_id: str, tenant_bucket: Optional[str] = None) -> Optional[Job]:
        ...

    def update_job(self, job: Job, etag: str) -> Job:
        ...

    def find_by_idempotency(self, tenant_id: str, idempotency_hash: str) -> Optional[str]:
        ...


class StepsStore(Protocol):
    def create_steps(self, job_id: str, steps: List[Step]) -> None:
        ...

    def get_steps(self, job_id: str) -> List[Step]:
        ...

    def get_active_step(self, job_id: str) -> Optional[Step]:
        ...

    def update_step(self, step: Step, etag: str) -> Step:
        ...


class OutboxStore(Protocol):
    def create_outbox(self, entry: OutboxEntry) -> OutboxEntry:
        ...

    def list_pending(self, partition_key: str, limit: int) -> List[OutboxEntry]:
        ...

    def mark_sent(self, outbox_id: str, partition_key: str, etag: str) -> OutboxEntry:
        ...

    def update_outbox(self, entry: OutboxEntry, etag: str) -> OutboxEntry:
        ...


class IdempotencyStore(Protocol):
    def put(self, record: JobIdempotency) -> None:
        ...

    def get(self, tenant_id: str, idempotency_hash: str) -> Optional[JobIdempotency]:
        ...


class JobIndexStore(Protocol):
    def put(self, record: JobIndex) -> None:
        ...

    def get(self, job_id: str) -> Optional[JobIndex]:
        ...


class TenantInflightStore(Protocol):
    def try_acquire(self, tenant_id: str, limit: int) -> bool:
        ...

    def release(self, tenant_id: str) -> None:
        ...
