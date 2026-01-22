from datetime import datetime, timezone, timedelta

from fastapi.testclient import TestClient

from api.app import create_app
from outbox.publisher import AsyncServiceBusPublisher


def _past_iso(seconds: int = 60) -> str:
    return (datetime.now(timezone.utc) - timedelta(seconds=seconds)).isoformat()


def test_retry_limit_marks_failed(monkeypatch):
    monkeypatch.setenv("LUCIUS_ADMIN_ENABLED", "true")
    monkeypatch.setenv("LUCIUS_OUTBOX_MAX_ATTEMPTS", "2")
    monkeypatch.setenv("LUCIUS_SERVICEBUS_CONNECTION", "Endpoint=sb://fake/")

    async def _noop_publish(self, _entry) -> None:
        return None

    monkeypatch.setattr(AsyncServiceBusPublisher, "publish", _noop_publish)

    app = create_app()
    client = TestClient(app)

    payload = {
        "tenant_id": "t1",
        "request_type": "OCR",
        "input_ref": "in",
        "output_ref": "out",
        "payload": {},
        "schema_version": "v1",
        "callback_urls": {
            "ack": "http://testserver/v1/callbacks/ack",
            "result": "http://testserver/v1/callbacks/result",
        },
    }
    response = client.post("/v1/commands", json=payload)
    assert response.status_code == 202
    job_id = response.json()["jobId"]

    entries = list(app.state.outbox_store._entries.values())
    outbox_entry = entries[0]

    steps = app.state.steps_store.get_steps(job_id)
    step = steps[0]
    step.attempt_no = 2
    step.lease_id = "lease-2"
    step = app.state.steps_store.update_step(step, step.etag or "")

    outbox_entry.attempt_no = 2
    outbox_entry.lease_id = step.lease_id
    outbox_entry.next_attempt_at = _past_iso()
    outbox_entry.state = "PENDING"
    outbox_entry = app.state.outbox_store.update_outbox(outbox_entry, outbox_entry.etag or "")

    admin_response = client.post("/v1/admin/outbox/retry")
    assert admin_response.status_code == 200

    job = app.state.jobs_store.get_job(job_id, "t1", app.state.job_index_store.get(job_id).tenant_bucket)
    steps = app.state.steps_store.get_steps(job_id)

    assert job.state == "FAILED_FINAL"
    assert steps[0].state == "FAILED_FINAL"
