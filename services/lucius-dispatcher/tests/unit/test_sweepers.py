from datetime import datetime, timedelta, timezone

from lucius_dispatcher.app.sweepers import AckSweeper, JobDriftSweeper, LeaseSweeper
from ledger.memory_store import MemoryJobsStore, MemoryStepsStore
from ledger.models import Job, Step


def _iso_ago(seconds: int) -> str:
    return (datetime.now(timezone.utc) - timedelta(seconds=seconds)).isoformat()


def _iso_ahead(seconds: int) -> str:
    return (datetime.now(timezone.utc) + timedelta(seconds=seconds)).isoformat()


def _step(job_id: str, state: str, attempt_no: int, lease_id: str) -> Step:
    return Step(
        job_id=job_id,
        step_index=0,
        step_id="ocr",
        step_type="OCR",
        service="platform-ocr",
        state=state,
        attempt_no=attempt_no,
        lease_id=lease_id,
        lease_expires_at=_iso_ago(5),
        input_ref="in",
        workspace_ref={},
        output_ref="out",
        payload={},
        resolved_mode="DEFAULT",
        lane=0,
        routing_key_used="t1",
        decision_source="REQUEST",
        decision_reason="request.mode",
        last_error_code=None,
        last_error_message=None,
        created_at=_iso_ago(120),
        updated_at=_iso_ago(120),
        completed_at=None,
    )


def test_ack_sweeper_marks_retry():
    steps_store = MemoryStepsStore()
    step = _step("job1", "AWAITING_ACK", 1, "lease1")
    steps_store.create_steps("job1", [step])

    sweeper = AckSweeper(steps_store, ack_timeout_seconds=30)
    updated = sweeper.sweep("job1")
    assert updated
    assert updated[0].state == "FAILED_RETRY"


def test_lease_sweeper_marks_retry():
    steps_store = MemoryStepsStore()
    step = _step("job1", "IN_PROGRESS", 1, "lease1")
    step.lease_expires_at = _iso_ago(5)
    steps_store.create_steps("job1", [step])

    sweeper = LeaseSweeper(steps_store, lease_timeout_seconds=30)
    updated = sweeper.sweep("job1")
    assert updated
    assert updated[0].state == "FAILED_RETRY"


def test_job_drift_marks_success_when_all_steps_terminal():
    jobs_store = MemoryJobsStore()
    steps_store = MemoryStepsStore()
    job = Job(
        job_id="job1",
        tenant_id="t1",
        tenant_bucket="t1#202401",
        request_type="OCR",
        doc_id=None,
        protocol_id="atomic-ocr.v1",
        mode="DEFAULT",
        state="IN_PROGRESS",
        idempotency_key=None,
        idempotency_hash="hash",
        current_step_id="ocr",
        current_step_index=0,
        attempts_total=1,
        created_at=_iso_ago(120),
        updated_at=_iso_ago(120),
        completed_at=None,
        error_code=None,
        error_message=None,
        correlation_id=None,
        traceparent=None,
    )
    jobs_store.create_job(job)

    step = _step("job1", "SUCCEEDED", 1, "lease1")
    step.completed_at = _iso_ago(60)
    steps_store.create_steps("job1", [step])

    sweeper = JobDriftSweeper(jobs_store, steps_store)
    sweeper.sweep("job1", "t1", "t1#202401")

    updated_job = jobs_store.get_job("job1", "t1", "t1#202401")
    assert updated_job is not None
    assert updated_job.state == "SUCCEEDED"
