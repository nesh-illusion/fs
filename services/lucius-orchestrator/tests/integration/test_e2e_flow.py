from __future__ import annotations

from datetime import datetime, timezone

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


def _publish_once(app, job_id: str):
    entries = [
        entry for entry in app.state.outbox_store._entries.values()
        if entry.job_id == job_id
    ]
    assert entries
    entry = entries[0]
    updated = app.state.outbox_store.mark_sent(entry.outbox_id, entry.tenant_bucket, entry.etag or "")
    steps = app.state.steps_store.get_steps(job_id)
    step = next(step for step in steps if step.step_id == entry.step_id)
    step.state = "INITIATED"
    step.updated_at = _now_iso()
    app.state.steps_store.update_step(step, step.etag or "")
    index = app.state.job_index_store.get(job_id)
    job = app.state.jobs_store.get_job(job_id, entry.tenant_id, index.tenant_bucket)
    job.state = "IN_PROGRESS"
    job.updated_at = _now_iso()
    app.state.jobs_store.update_job(job, job.etag or "")
    return updated


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def test_e2e_dispatch_and_worker_callbacks(monkeypatch):
    app = create_app()
    client = TestClient(app)

    ack_url = "http://testserver/v1/callbacks/ack"
    result_url = "http://testserver/v1/callbacks/result"
    job_id = _create_command(client, {"ack": ack_url, "result": result_url})
    outbox_entry = _publish_once(app, job_id)

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

    consumer.handle_message(outbox_entry.payload)
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
    outbox_entry = _publish_once(app, job_id)

    ack_payload = {
        "jobId": outbox_entry.job_id,
        "stepId": outbox_entry.step_id,
        "tenant_id": outbox_entry.tenant_id,
        "attempt_no": outbox_entry.attempt_no,
        "lease_id": outbox_entry.lease_id,
        "status": "ACKED",
        "timestamp": _now_iso(),
    }
    result_payload = {
        "jobId": outbox_entry.job_id,
        "stepId": outbox_entry.step_id,
        "tenant_id": outbox_entry.tenant_id,
        "attempt_no": outbox_entry.attempt_no,
        "lease_id": outbox_entry.lease_id,
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
    outbox_entry = _publish_once(app, job_id)

    steps = app.state.steps_store.get_steps(job_id)
    assert [step.step_id for step in steps] == ["ocr", "embedding"]
    assert steps[0].state == "INITIATED"
    assert steps[1].state == "PENDING"

    ack_payload = {
        "jobId": outbox_entry.job_id,
        "stepId": outbox_entry.step_id,
        "tenant_id": outbox_entry.tenant_id,
        "attempt_no": outbox_entry.attempt_no,
        "lease_id": outbox_entry.lease_id,
        "status": "ACKED",
        "timestamp": _now_iso(),
    }
    result_payload = {
        "jobId": outbox_entry.job_id,
        "stepId": outbox_entry.step_id,
        "tenant_id": outbox_entry.tenant_id,
        "attempt_no": outbox_entry.attempt_no,
        "lease_id": outbox_entry.lease_id,
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
