from datetime import datetime, timedelta, timezone
from typing import List

from lucius_orchestrator.ledger.interfaces import JobsStore, StepsStore
from lucius_orchestrator.ledger.models import Step


TERMINAL_STATES = {"SUCCEEDED", "FAILED_FINAL", "CANCELLED"}


def _parse_iso(value: str) -> datetime:
    return datetime.fromisoformat(value)


def _now() -> datetime:
    return datetime.now(timezone.utc)


class AckSweeper:
    def __init__(self, steps_store: StepsStore, ack_timeout_seconds: int):
        self._steps_store = steps_store
        self._ack_timeout = timedelta(seconds=ack_timeout_seconds)

    def sweep(self, job_id: str) -> List[Step]:
        updated: List[Step] = []
        for step in self._steps_store.get_steps(job_id):
            if step.state != "AWAITING_ACK":
                continue
            if not step.updated_at:
                continue
            if _now() - _parse_iso(step.updated_at) < self._ack_timeout:
                continue
            if step.attempt_no >= 3:
                step.state = "FAILED_FINAL"
            else:
                step.state = "FAILED_RETRY"
            step.updated_at = _now().isoformat()
            updated.append(self._steps_store.update_step(step, step.etag or ""))
        return updated


class LeaseSweeper:
    def __init__(self, steps_store: StepsStore, lease_timeout_seconds: int):
        self._steps_store = steps_store
        self._lease_timeout = timedelta(seconds=lease_timeout_seconds)

    def sweep(self, job_id: str) -> List[Step]:
        updated: List[Step] = []
        for step in self._steps_store.get_steps(job_id):
            if step.state != "IN_PROGRESS":
                continue
            if not step.lease_expires_at:
                continue
            if _parse_iso(step.lease_expires_at) > _now():
                continue
            if step.attempt_no >= 3:
                step.state = "FAILED_FINAL"
            else:
                step.state = "FAILED_RETRY"
            step.updated_at = _now().isoformat()
            updated.append(self._steps_store.update_step(step, step.etag or ""))
        return updated


class JobDriftSweeper:
    def __init__(self, jobs_store: JobsStore, steps_store: StepsStore):
        self._jobs_store = jobs_store
        self._steps_store = steps_store

    def sweep(self, job_id: str, tenant_id: str, tenant_bucket: str) -> None:
        job = self._jobs_store.get_job(job_id, tenant_id, tenant_bucket)
        if job is None or job.state in TERMINAL_STATES:
            return
        steps = self._steps_store.get_steps(job_id)
        active = next((step for step in steps if step.state not in TERMINAL_STATES), None)
        if active is None:
            job.state = "SUCCEEDED"
            job.updated_at = _now().isoformat()
            job.completed_at = job.updated_at
            self._jobs_store.update_job(job, job.etag or "")
