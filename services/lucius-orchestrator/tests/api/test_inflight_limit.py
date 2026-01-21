from datetime import datetime, timezone

from fastapi.testclient import TestClient

from worker.dispatcher import OutboxDispatcher
from worker.publisher import NoopPublisher
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


def _dispatch_once(app, job_id: str):
    entries = [
        entry for entry in app.state.outbox_store._entries.values()
        if entry.job_id == job_id
    ]
    assert entries
    partition_key = entries[0].tenant_bucket
    dispatcher = OutboxDispatcher(app.state.outbox_store, app.state.steps_store)
    dispatcher.dispatch_once(partition_key, 10, NoopPublisher())
    assert len(entries) == 1
    return entries[0]


def _complete_job(client: TestClient, outbox_entry) -> None:
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


def test_inflight_limit_enforced(monkeypatch):
    monkeypatch.setenv("LUCIUS_MAX_INFLIGHT_PER_TENANT", "1")
    app = create_app()
    client = TestClient(app)

    job1 = _create_command(client, _base_payload("t1", "in-1"))
    response = client.post("/v1/commands", json=_base_payload("t1", "in-2"))
    assert response.status_code == 429

    outbox = _dispatch_once(app, job1)
    _complete_job(client, outbox)

    job3 = _create_command(client, _base_payload("t1", "in-3"))
    assert job3
