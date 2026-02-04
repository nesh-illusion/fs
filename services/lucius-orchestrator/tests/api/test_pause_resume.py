from fastapi.testclient import TestClient

from api.app import create_app


def _create_job(client: TestClient) -> str:
    payload = {
        "tenant_id": "t1",
        "request_type": "OCR",
        "input_ref": "in",
        "output_ref": "out",
        "payload": {},
        "schema_version": "v1",
    }
    response = client.post("/v1/orchestrate", json=payload)
    assert response.status_code == 202
    return response.json()["jobId"]


def test_pause_and_resume_job(monkeypatch):
    monkeypatch.setenv("LUCIUS_TEMPORAL_ENABLED", "false")
    app = create_app()
    client = TestClient(app)

    job_id = _create_job(client)
    pause_response = client.post(f"/v1/jobs/{job_id}:pause", params={"tenant_id": "t1"})
    assert pause_response.status_code == 200

    paused_job = pause_response.json()
    # With Temporal disabled, API does not mutate ledger state.
    assert paused_job["state"] == "QUEUED"

    resume_response = client.post(f"/v1/jobs/{job_id}:resume", params={"tenant_id": "t1"})
    assert resume_response.status_code == 200

    resumed_job = resume_response.json()
    assert resumed_job["state"] == "QUEUED"
