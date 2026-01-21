import zlib

from fastapi.testclient import TestClient

from lucius_orchestrator.api.app import create_app


def _create_command(client: TestClient, payload: dict) -> str:
    response = client.post("/v1/commands", json=payload)
    assert response.status_code == 202
    return response.json()["jobId"]


def _get_outbox_entry(app, job_id: str):
    entries = [
        entry for entry in app.state.outbox_store._entries.values()
        if entry.job_id == job_id
    ]
    assert len(entries) == 1
    return entries[0]


def _base_payload() -> dict:
    return {
        "tenant_id": "TenantA",
        "request_type": "OCR",
        "input_ref": "in",
        "output_ref": "out",
        "payload": {},
        "schema_version": "v1",
    }


def test_default_mode_routes_by_tenant_id():
    app = create_app()
    client = TestClient(app)
    payload = _base_payload()
    payload["mode"] = "DEFAULT"
    job_id = _create_command(client, payload)

    outbox = _get_outbox_entry(app, job_id)
    routing_key = "tenanta"
    expected_lane = zlib.crc32(routing_key.encode("utf-8")) % 16

    assert outbox.routing_key_used == routing_key
    assert outbox.lane == expected_lane
    assert outbox.topic == f"global-bus-p{expected_lane}"


def test_burst_mode_routes_by_tenant_and_doc_id():
    app = create_app()
    client = TestClient(app)
    payload = _base_payload()
    payload["mode"] = "BURST"
    payload["doc_id"] = "Doc-99"
    job_id = _create_command(client, payload)

    outbox = _get_outbox_entry(app, job_id)
    routing_key = "tenantadoc-99".lower()
    expected_lane = zlib.crc32(routing_key.encode("utf-8")) % 16

    assert outbox.routing_key_used == routing_key
    assert outbox.lane == expected_lane
    assert outbox.topic == f"global-bus-p{expected_lane}"
