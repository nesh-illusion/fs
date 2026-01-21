import pytest
from fastapi.testclient import TestClient

from lucius.api.app import create_app


@pytest.fixture()
def client():
    app = create_app()
    return TestClient(app)


def test_duplicate_create_returns_same_job_id(client):
    payload = {
        "tenant_id": "t1",
        "request_type": "OCR",
        "input_ref": "https://example.com/in.pdf",
        "output_ref": "https://example.com/out.txt",
        "payload": {"foo": "bar"},
        "schema_version": "v1",
    }
    first = client.post("/v1/commands", json=payload)
    assert first.status_code == 202
    second = client.post("/v1/commands", json=payload)
    assert second.status_code == 202
    assert second.json()["jobId"] == first.json()["jobId"]


def test_schema_validation_error_returns_422(client):
    payload = {
        "tenant_id": "t1",
        "request_type": "OCR",
        "input_ref": "https://example.com/in.pdf",
        "output_ref": "https://example.com/out.txt",
        "payload": {"output_format": "XML"},
        "schema_version": "v1",
    }
    response = client.post("/v1/commands", json=payload)
    assert response.status_code == 422
