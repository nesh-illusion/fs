from dataclasses import replace
from datetime import datetime, timezone
from typing import Dict, List, Optional

from .interfaces import IdempotencyStore, JobIndexStore, JobsStore, OutboxStore, StepsStore, TenantInflightStore
from .models import Job, JobIdempotency, JobIndex, OutboxEntry, Step


class MemoryJobsStore(JobsStore):
    def __init__(self) -> None:
        self._by_job_id: Dict[str, Job] = {}
        self._by_idempotency: Dict[str, str] = {}

    def create_job(self, job: Job) -> Job:
        job = replace(job, etag="1")
        self._by_job_id[job.job_id] = job
        self._by_idempotency[f"{job.tenant_id}:{job.idempotency_hash}"] = job.job_id
        return job

    def get_job(self, job_id: str, tenant_id: str, tenant_bucket: Optional[str] = None) -> Optional[Job]:
        job = self._by_job_id.get(job_id)
        if job is None or job.tenant_id != tenant_id:
            return None
        return job

    def update_job(self, job: Job, etag: str) -> Job:
        current = self._by_job_id.get(job.job_id)
        if current is None or current.etag != etag:
            raise ValueError("etag mismatch")
        next_etag = str(int(etag) + 1)
        updated = replace(job, etag=next_etag)
        self._by_job_id[job.job_id] = updated
        return updated

    def find_by_idempotency(self, tenant_id: str, idempotency_hash: str) -> Optional[str]:
        return self._by_idempotency.get(f"{tenant_id}:{idempotency_hash}")


class MemoryStepsStore(StepsStore):
    def __init__(self) -> None:
        self._by_job: Dict[str, List[Step]] = {}

    def create_steps(self, job_id: str, steps: List[Step]) -> None:
        self._by_job[job_id] = [replace(step, etag="1") for step in steps]

    def get_steps(self, job_id: str) -> List[Step]:
        steps = list(self._by_job.get(job_id, []))
        return sorted(steps, key=lambda step: step.step_index)

    def get_active_step(self, job_id: str) -> Optional[Step]:
        steps = self.get_steps(job_id)
        for step in steps:
            if step.state not in {"SUCCEEDED", "FAILED_FINAL", "CANCELLED"}:
                return step
        return None

    def update_step(self, step: Step, etag: str) -> Step:
        steps = self._by_job.get(step.job_id, [])
        for idx, current in enumerate(steps):
            if current.step_id == step.step_id:
                if current.etag != etag:
                    raise ValueError("etag mismatch")
                next_etag = str(int(etag) + 1)
                updated = replace(step, etag=next_etag)
                steps[idx] = updated
                return updated
        raise ValueError("step not found")


class MemoryOutboxStore(OutboxStore):
    def __init__(self) -> None:
        self._entries: Dict[str, OutboxEntry] = {}

    def create_outbox(self, entry: OutboxEntry) -> OutboxEntry:
        entry = replace(entry, etag="1")
        self._entries[entry.outbox_id] = entry
        return entry

    def list_pending(self, partition_key: str, limit: int) -> List[OutboxEntry]:
        pending = []
        for entry in self._entries.values():
            if entry.state != "PENDING":
                continue
            if partition_key != "*" and entry.tenant_bucket != partition_key:
                continue
            pending.append(entry)
        return pending[:limit]

    def mark_sent(self, outbox_id: str, partition_key: str, etag: str) -> OutboxEntry:
        entry = self._entries.get(outbox_id)
        if entry is None or entry.etag != etag:
            raise ValueError("etag mismatch")
        now = datetime.now(timezone.utc).isoformat()
        updated = replace(
            entry,
            state="SENT",
            sent_at=now,
            updated_at=now,
            etag=str(int(etag) + 1),
        )
        self._entries[outbox_id] = updated
        return updated

    def update_outbox(self, entry: OutboxEntry, etag: str) -> OutboxEntry:
        current = self._entries.get(entry.outbox_id)
        if current is None or current.etag != etag:
            raise ValueError("etag mismatch")
        next_etag = str(int(etag) + 1)
        updated = replace(entry, etag=next_etag)
        self._entries[entry.outbox_id] = updated
        return updated


class MemoryIdempotencyStore(IdempotencyStore):
    def __init__(self) -> None:
        self._records: Dict[str, JobIdempotency] = {}

    def put(self, record: JobIdempotency) -> None:
        key = f"{record.tenant_id}:{record.idempotency_hash}"
        self._records[key] = record

    def get(self, tenant_id: str, idempotency_hash: str) -> Optional[JobIdempotency]:
        return self._records.get(f"{tenant_id}:{idempotency_hash}")


class MemoryJobIndexStore(JobIndexStore):
    def __init__(self) -> None:
        self._records: Dict[str, JobIndex] = {}

    def put(self, record: JobIndex) -> None:
        self._records[record.job_id] = record

    def get(self, job_id: str) -> Optional[JobIndex]:
        return self._records.get(job_id)


class MemoryTenantInflightStore(TenantInflightStore):
    def __init__(self) -> None:
        self._counts: Dict[str, int] = {}

    def try_acquire(self, tenant_id: str, limit: int) -> bool:
        count = self._counts.get(tenant_id, 0)
        if count >= limit:
            return False
        self._counts[tenant_id] = count + 1
        return True

    def release(self, tenant_id: str) -> None:
        count = self._counts.get(tenant_id, 0)
        if count <= 1:
            self._counts.pop(tenant_id, None)
            return
        self._counts[tenant_id] = count - 1
