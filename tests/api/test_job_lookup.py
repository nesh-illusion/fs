import pytest
from fastapi.testclient import TestClient

from lucius.api.app import create_app


@pytest.fixture()
def client():
    app = create_app()
    return TestClient(app)


def test_job_lookup_without_tenant_bucket(client):
    create_resp = client.post(
        "/v1/commands",
        json={
            "tenant_id": "t1",
            "request_type": "OCR",
            "input_ref": "https://example.com/in.pdf",
            "output_ref": "https://example.com/out.txt",
            "payload": {},
            "schema_version": "v1",
        },
    )
    assert create_resp.status_code == 202
    job_id = create_resp.json()["jobId"]

    get_resp = client.get(f"/v1/jobs/{job_id}", params={"tenant_id": "t1"})
    assert get_resp.status_code == 200
    assert get_resp.json()["job_id"] == job_id
