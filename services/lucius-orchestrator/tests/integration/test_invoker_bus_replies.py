import asyncio
from dataclasses import replace
from types import SimpleNamespace

from invoker import app as invoker_app
from ledger.memory_store import MemoryJobsStore, MemoryStepsStore, MemoryTenantInflightStore
from ledger.models import Job, Step


class _TemporalStub:
    def __init__(self) -> None:
        self.signals = []

    async def signal_workflow(self, workflow_id: str, signal: str, payload: dict) -> None:
        self.signals.append((workflow_id, signal, payload))


def _build_app_state(job: Job, step: Step) -> SimpleNamespace:
    jobs_store = MemoryJobsStore()
    steps_store = MemoryStepsStore()
    inflight_store = MemoryTenantInflightStore()

    job = jobs_store.create_job(job)
    steps_store.create_steps(job.job_id, [step])
    inflight_store.try_acquire(job.tenant_id, limit=10)

    return SimpleNamespace(
        jobs_store=jobs_store,
        steps_store=steps_store,
        inflight_store=inflight_store,
        temporal_client=_TemporalStub(),
    )


def _job(job_id: str = "job1") -> Job:
    return Job(
        job_id=job_id,
        tenant_id="t1",
        tenant_bucket="t1",
        request_type="OCR",
        doc_id=None,
        protocol_id="p1",
        mode="DEFAULT",
        state="QUEUED",
        idempotency_key=None,
        idempotency_hash="hash1",
        current_step_id="step1",
        current_step_index=0,
        attempts_total=0,
        created_at="2026-01-01T00:00:00Z",
        updated_at="2026-01-01T00:00:00Z",
        completed_at=None,
        error_code=None,
        error_message=None,
        correlation_id=None,
        traceparent=None,
        final_output=None,
    )


def _step(job_id: str = "job1") -> Step:
    return Step(
        job_id=job_id,
        step_index=0,
        step_id="step1",
        step_type="OCR",
        service="distributed-ocr",
        state="INITIATED",
        attempt_no=1,
        lease_id="lease1",
        lease_expires_at="2026-01-01T00:10:00Z",
        input_ref="in",
        workspace_ref=None,
        output_ref="out",
        payload={},
        resolved_mode="DEFAULT",
        lane=0,
        routing_key_used="t1",
        decision_source="DEFAULT",
        decision_reason="",
        last_error_code=None,
        last_error_message=None,
        created_at="2026-01-01T00:00:00Z",
        updated_at="2026-01-01T00:00:00Z",
        completed_at=None,
    )


def test_invoker_ack_updates_step_and_job():
    state = _build_app_state(_job(), _step())
    app = SimpleNamespace(state=state)

    payload = {
        "type": "ACK",
        "jobId": "job1",
        "stepId": "step1",
        "tenant_id": "t1",
        "attempt_no": 1,
        "lease_id": "lease1",
    }

    asyncio.run(invoker_app._handle_ack(app, payload))

    job = state.jobs_store.get_job("job1", "t1")
    step = state.steps_store.get_steps("job1")[0]
    assert job is not None
    assert step is not None
    assert job.state == "IN_PROGRESS"
    assert step.state == "PROCESSING"


def test_invoker_result_success_sets_final_output():
    step = _step()
    step = replace(step, state="PROCESSING")
    state = _build_app_state(_job(), step)
    app = SimpleNamespace(state=state)

    payload = {
        "type": "RESULT",
        "jobId": "job1",
        "stepId": "step1",
        "tenant_id": "t1",
        "attempt_no": 1,
        "lease_id": "lease1",
        "status": "SUCCEEDED",
        "output_ref": {"uri": "s3://bucket/output.json"},
    }

    asyncio.run(invoker_app._handle_result(app, payload))

    job = state.jobs_store.get_job("job1", "t1")
    step = state.steps_store.get_steps("job1")[0]
    assert job is not None
    assert step is not None
    assert job.state == "SUCCEEDED"
    assert job.final_output == "s3://bucket/output.json"
    assert step.state == "SUCCEEDED"


def test_invoker_result_failed_final_sets_error():
    step = _step()
    step = replace(step, state="PROCESSING")
    state = _build_app_state(_job(), step)
    app = SimpleNamespace(state=state)

    payload = {
        "type": "RESULT",
        "jobId": "job1",
        "stepId": "step1",
        "tenant_id": "t1",
        "attempt_no": 1,
        "lease_id": "lease1",
        "status": "FAILED_FINAL",
        "error": {"code": "E1", "message": "bad"},
    }

    asyncio.run(invoker_app._handle_result(app, payload))

    job = state.jobs_store.get_job("job1", "t1")
    step = state.steps_store.get_steps("job1")[0]
    assert job is not None
    assert step is not None
    assert job.state == "FAILED_FINAL"
    assert job.error_code == "E1"
    assert job.error_message == "bad"
    assert step.state == "FAILED_FINAL"
