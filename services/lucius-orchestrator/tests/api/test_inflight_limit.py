from datetime import datetime, timezone
from uuid import uuid4

from fastapi.testclient import TestClient

from api.app import create_app


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _base_payload(tenant_id: str, input_ref: str) -> dict:
    return {
        "tenant_id": tenant_id,
        "request_type": "OCR",
        "input_ref": input_ref,
        "output_ref": "out",
        "payload": {},
        "schema_version": "v1",
    }


def _create_command(client: TestClient, payload: dict) -> str:
    response = client.post("/v1/orchestrate", json=payload)
    assert response.status_code == 202
    return response.json()["jobId"]


def _start_step(app, job_id: str, tenant_id: str):
    # Invoker-owned updates: simulate the attempt/lease record directly in the store.
    steps = app.state.steps_store.get_steps(job_id)
    step = steps[0]
    lease_id = uuid4().hex
    now = _now_iso()
    step.attempt_no = 1
    step.lease_id = lease_id
    step.lease_expires_at = now
    step.state = "INITIATED"
    step.updated_at = now
    app.state.steps_store.update_step(step, step.etag or "")

    job = app.state.jobs_store.get_job(
        job_id, tenant_id, app.state.job_index_store.get(job_id).tenant_bucket
    )
    job.current_step_id = step.step_id
    job.current_step_index = step.step_index
    job.attempts_total = (job.attempts_total or 0) + 1
    job.updated_at = now
    app.state.jobs_store.update_job(job, job.etag or "")
    return step.step_id, lease_id


def _complete_job(app, job_id: str, tenant_id: str, step_id: str) -> None:
    job = app.state.jobs_store.get_job(job_id, tenant_id, app.state.job_index_store.get(job_id).tenant_bucket)
    steps = app.state.steps_store.get_steps(job_id)
    step = next(step for step in steps if step.step_id == step_id)
    now = _now_iso()
    step.state = "SUCCEEDED"
    step.completed_at = now
    step.updated_at = now
    app.state.steps_store.update_step(step, step.etag or "")
    job.state = "SUCCEEDED"
    job.completed_at = now
    job.updated_at = now
    app.state.jobs_store.update_job(job, job.etag or "")
    app.state.inflight_store.release(job.tenant_id)


def test_inflight_limit_enforced(monkeypatch):
    monkeypatch.setenv("LUCIUS_MAX_INFLIGHT_PER_TENANT", "1")
    app = create_app()
    client = TestClient(app)

    job1 = _create_command(client, _base_payload("t1", "in-1"))
    response = client.post("/v1/orchestrate", json=_base_payload("t1", "in-2"))
    assert response.status_code == 429

    step_id, lease_id = _start_step(app, job1, "t1")
    _complete_job(app, job1, "t1", step_id)

    job3 = _create_command(client, _base_payload("t1", "in-3"))
    assert job3
