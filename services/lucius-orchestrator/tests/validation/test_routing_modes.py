import zlib

from fastapi.testclient import TestClient

from api.app import create_app


def _create_command(client: TestClient, payload: dict) -> str:
    response = client.post("/v1/orchestrate", json=payload)
    assert response.status_code == 202
    return response.json()["jobId"]


def _get_first_step(app, job_id: str):
    steps = app.state.steps_store.get_steps(job_id)
    assert steps
    return steps[0]


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

    step = _get_first_step(app, job_id)
    routing_key = "tenanta"
    expected_lane = zlib.crc32(routing_key.encode("utf-8")) % 16

    assert step.routing_key_used == routing_key
    assert step.lane == expected_lane


def test_burst_mode_routes_by_tenant_and_doc_id():
    app = create_app()
    client = TestClient(app)
    payload = _base_payload()
    payload["mode"] = "BURST"
    payload["doc_id"] = "Doc-99"
    job_id = _create_command(client, payload)

    step = _get_first_step(app, job_id)
    routing_key = "tenantadoc-99".lower()
    expected_lane = zlib.crc32(routing_key.encode("utf-8")) % 16

    assert step.routing_key_used == routing_key
    assert step.lane == expected_lane
