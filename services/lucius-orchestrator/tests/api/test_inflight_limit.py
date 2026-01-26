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
        "callback_urls": {
            "ack": "http://testserver/v1/callbacks/ack",
            "result": "http://testserver/v1/callbacks/result",
        },
    }


def _create_command(client: TestClient, payload: dict) -> str:
    response = client.post("/v1/commands", json=payload)
    assert response.status_code == 202
    return response.json()["jobId"]


def _start_step(client: TestClient, app, job_id: str, tenant_id: str):
    steps = app.state.steps_store.get_steps(job_id)
    step = steps[0]
    lease_id = uuid4().hex
    response = client.post(
        "/v1/internal/steps/attempt",
        json={
            "jobId": job_id,
            "stepId": step.step_id,
            "tenant_id": tenant_id,
            "attempt_no": 1,
            "lease_id": lease_id,
            "lease_expires_at": _now_iso(),
        },
    )
    assert response.status_code == 200
    return step.step_id, lease_id


def _complete_job(client: TestClient, job_id: str, step_id: str, tenant_id: str, lease_id: str) -> None:
    ack_payload = {
        "jobId": job_id,
        "stepId": step_id,
        "tenant_id": tenant_id,
        "attempt_no": 1,
        "lease_id": lease_id,
        "status": "ACKED",
        "timestamp": _now_iso(),
    }
    result_payload = {
        "jobId": job_id,
        "stepId": step_id,
        "tenant_id": tenant_id,
        "attempt_no": 1,
        "lease_id": lease_id,
        "status": "SUCCEEDED",
        "timestamp": _now_iso(),
    }
    ack_response = client.post("/v1/callbacks/ack", json=ack_payload)
    assert ack_response.status_code == 200
    result_response = client.post("/v1/callbacks/result", json=result_payload)
    assert result_response.status_code == 200


def test_inflight_limit_enforced(monkeypatch):
    monkeypatch.setenv("LUCIUS_MAX_INFLIGHT_PER_TENANT", "1")
    app = create_app()
    client = TestClient(app)

    job1 = _create_command(client, _base_payload("t1", "in-1"))
    response = client.post("/v1/commands", json=_base_payload("t1", "in-2"))
    assert response.status_code == 429

    step_id, lease_id = _start_step(client, app, job1, "t1")
    _complete_job(client, job1, step_id, "t1", lease_id)

    job3 = _create_command(client, _base_payload("t1", "in-3"))
    assert job3
