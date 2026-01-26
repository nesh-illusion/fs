from __future__ import annotations

from datetime import datetime, timezone
from uuid import uuid4

from fastapi.testclient import TestClient

from api.app import create_app
import consumer


def _create_command(client: TestClient, callback_urls: dict) -> str:
    payload = {
        "tenant_id": "t1",
        "request_type": "OCR",
        "input_ref": "in",
        "output_ref": "out",
        "payload": {},
        "schema_version": "v1",
        "callback_urls": callback_urls,
    }
    response = client.post("/v1/commands", json=payload)
    assert response.status_code == 202
    return response.json()["jobId"]


def _create_multi_step_command(client: TestClient, callback_urls: dict) -> str:
    payload = {
        "tenant_id": "t1",
        "request_type": "OCR->EMBEDDING",
        "input_ref": "in",
        "output_ref": "out",
        "payload": {
            "steps": {
                "ocr": {"output_format": "TEXT"},
                "embedding": {"model": "text-embedding-3-large"},
            }
        },
        "schema_version": "v1",
        "callback_urls": callback_urls,
    }
    response = client.post("/v1/commands", json=payload)
    assert response.status_code == 202
    return response.json()["jobId"]


def _start_step(client: TestClient, app, job_id: str, step_id: str) -> str:
    lease_id = uuid4().hex
    response = client.post(
        "/v1/internal/steps/attempt",
        json={
            "jobId": job_id,
            "stepId": step_id,
            "tenant_id": "t1",
            "attempt_no": 1,
            "lease_id": lease_id,
            "lease_expires_at": _now_iso(),
        },
    )
    assert response.status_code == 200
    return lease_id


def _build_directive(app, job_id: str, step_id: str, lease_id: str, attempt_no: int = 1) -> dict:
    job_index = app.state.job_index_store.get(job_id)
    job = app.state.jobs_store.get_job(job_id, "t1", job_index.tenant_bucket)
    step = next(step for step in app.state.steps_store.get_steps(job_id) if step.step_id == step_id)
    return {
        "jobId": job_id,
        "tenant_id": "t1",
        "stepId": step_id,
        "protocol_id": job.protocol_id,
        "step_type": step.step_type,
        "attempt_no": attempt_no,
        "lease_id": lease_id,
        "input_ref": step.input_ref,
        "workspace_ref": step.workspace_ref,
        "output_ref": step.output_ref,
        "payload": step.payload,
        "callback_urls": {"ack": "http://testserver/v1/callbacks/ack", "result": "http://testserver/v1/callbacks/result"},
    }


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def test_e2e_dispatch_and_worker_callbacks(monkeypatch):
    app = create_app()
    client = TestClient(app)

    ack_url = "http://testserver/v1/callbacks/ack"
    result_url = "http://testserver/v1/callbacks/result"
    job_id = _create_command(client, {"ack": ack_url, "result": result_url})
    steps = app.state.steps_store.get_steps(job_id)
    lease_id = _start_step(client, app, job_id, steps[0].step_id)
    directive_payload = _build_directive(app, job_id, steps[0].step_id, lease_id)

    def send_ack(directive) -> None:
        client.post(
            "/v1/callbacks/ack",
            json={
                "jobId": directive.jobId,
                "stepId": directive.stepId,
                "tenant_id": directive.tenant_id,
                "attempt_no": directive.attempt_no,
                "lease_id": directive.lease_id,
                "status": "ACKED",
                "timestamp": _now_iso(),
            },
        )

    def send_result(directive, **_kwargs) -> None:
        client.post(
            "/v1/callbacks/result",
            json={
                "jobId": directive.jobId,
                "stepId": directive.stepId,
                "tenant_id": directive.tenant_id,
                "attempt_no": directive.attempt_no,
                "lease_id": directive.lease_id,
                "status": "SUCCEEDED",
                "timestamp": _now_iso(),
            },
        )

    monkeypatch.setattr(consumer, "send_ack", send_ack)
    monkeypatch.setattr(consumer, "send_result", send_result)

    consumer.handle_message(directive_payload)
    job = app.state.jobs_store.get_job(job_id, "t1", app.state.job_index_store.get(job_id).tenant_bucket)
    steps = app.state.steps_store.get_steps(job_id)

    assert job.state == "SUCCEEDED"
    assert steps[0].state == "SUCCEEDED"


def test_e2e_manual_ack_and_result():
    app = create_app()
    client = TestClient(app)

    job_id = _create_command(
        client,
        {"ack": "http://testserver/v1/callbacks/ack", "result": "http://testserver/v1/callbacks/result"},
    )
    steps = app.state.steps_store.get_steps(job_id)
    lease_id = _start_step(client, app, job_id, steps[0].step_id)

    ack_payload = {
        "jobId": job_id,
        "stepId": steps[0].step_id,
        "tenant_id": "t1",
        "attempt_no": 1,
        "lease_id": lease_id,
        "status": "ACKED",
        "timestamp": _now_iso(),
    }
    result_payload = {
        "jobId": job_id,
        "stepId": steps[0].step_id,
        "tenant_id": "t1",
        "attempt_no": 1,
        "lease_id": lease_id,
        "status": "SUCCEEDED",
        "timestamp": _now_iso(),
    }

    ack_response = client.post("/v1/callbacks/ack", json=ack_payload)
    assert ack_response.status_code == 200
    result_response = client.post("/v1/callbacks/result", json=result_payload)
    assert result_response.status_code == 200

    job = app.state.jobs_store.get_job(job_id, "t1", app.state.job_index_store.get(job_id).tenant_bucket)
    steps = app.state.steps_store.get_steps(job_id)

    assert job.state == "SUCCEEDED"
    assert steps[0].state == "SUCCEEDED"


def test_e2e_multi_step_initialization_and_first_step_result():
    app = create_app()
    client = TestClient(app)

    job_id = _create_multi_step_command(
        client,
        {"ack": "http://testserver/v1/callbacks/ack", "result": "http://testserver/v1/callbacks/result"},
    )
    steps = app.state.steps_store.get_steps(job_id)
    lease_id = _start_step(client, app, job_id, steps[0].step_id)

    assert [step.step_id for step in steps] == ["ocr", "embedding"]
    assert steps[0].state == "INITIATED"
    assert steps[1].state == "PENDING"

    ack_payload = {
        "jobId": job_id,
        "stepId": steps[0].step_id,
        "tenant_id": "t1",
        "attempt_no": 1,
        "lease_id": lease_id,
        "status": "ACKED",
        "timestamp": _now_iso(),
    }
    result_payload = {
        "jobId": job_id,
        "stepId": steps[0].step_id,
        "tenant_id": "t1",
        "attempt_no": 1,
        "lease_id": lease_id,
        "status": "SUCCEEDED",
        "timestamp": _now_iso(),
    }

    ack_response = client.post("/v1/callbacks/ack", json=ack_payload)
    assert ack_response.status_code == 200
    result_response = client.post("/v1/callbacks/result", json=result_payload)
    assert result_response.status_code == 200

    job = app.state.jobs_store.get_job(job_id, "t1", app.state.job_index_store.get(job_id).tenant_bucket)
    steps = app.state.steps_store.get_steps(job_id)
    assert job.state == "IN_PROGRESS"
    assert steps[0].state == "SUCCEEDED"
    assert steps[1].state == "PENDING"
