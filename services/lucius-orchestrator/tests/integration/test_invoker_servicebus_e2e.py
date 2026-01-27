import json
import os
import time
from types import SimpleNamespace

import pytest
from fastapi.testclient import TestClient

from azure.servicebus import ServiceBusClient, ServiceBusMessage

from invoker import app as invoker_app
from ledger.memory_store import MemoryJobsStore, MemoryStepsStore, MemoryTenantInflightStore
from ledger.models import Job, Step


def _job(job_id: str = "job1") -> Job:
    return Job(
        job_id=job_id,
        tenant_id="t1",
        tenant_bucket="t1",
        request_type="OCR",
        doc_id=None,
        protocol_id="p1",
        mode="DEFAULT",
        state="QUEUED",
        idempotency_key=None,
        idempotency_hash="hash1",
        current_step_id="step1",
        current_step_index=0,
        attempts_total=0,
        created_at="2026-01-01T00:00:00Z",
        updated_at="2026-01-01T00:00:00Z",
        completed_at=None,
        error_code=None,
        error_message=None,
        correlation_id=None,
        traceparent=None,
        final_output=None,
    )


def _step(job_id: str = "job1") -> Step:
    return Step(
        job_id=job_id,
        step_index=0,
        step_id="step1",
        step_type="OCR",
        service="distributed-ocr",
        state="INITIATED",
        attempt_no=1,
        lease_id="lease1",
        lease_expires_at="2026-01-01T00:10:00Z",
        input_ref="in",
        workspace_ref=None,
        output_ref="out",
        payload={},
        resolved_mode="DEFAULT",
        lane=0,
        routing_key_used="t1",
        decision_source="DEFAULT",
        decision_reason="",
        last_error_code=None,
        last_error_message=None,
        created_at="2026-01-01T00:00:00Z",
        updated_at="2026-01-01T00:00:00Z",
        completed_at=None,
    )


def _build_memory_state(job: Job, step: Step) -> SimpleNamespace:
    jobs_store = MemoryJobsStore()
    steps_store = MemoryStepsStore()
    inflight_store = MemoryTenantInflightStore()
    job = jobs_store.create_job(job)
    steps_store.create_steps(job.job_id, [step])
    inflight_store.try_acquire(job.tenant_id, limit=10)
    return SimpleNamespace(
        jobs_store=jobs_store,
        steps_store=steps_store,
        inflight_store=inflight_store,
        temporal_client=None,
    )


def _publish(connection: str, topic: str, payload: dict) -> None:
    message = ServiceBusMessage(json.dumps(payload))
    with ServiceBusClient.from_connection_string(connection) as client:
        sender = client.get_topic_sender(topic_name=topic)
        with sender:
            sender.send_messages(message)


def _await_state(state, job_id: str, expected_job_state: str, timeout_s: int = 10) -> None:
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        job = state.jobs_store.get_job(job_id, "t1")
        if job is not None and job.state == expected_job_state:
            return
        time.sleep(0.2)
    raise AssertionError(f"job state did not reach {expected_job_state} within timeout")


def test_invoker_servicebus_replies_e2e(monkeypatch):
    connection = os.getenv("LUCIUS_SERVICEBUS_CONNECTION")
    reply_prefix = os.getenv("LUCIUS_SERVICEBUS_REPLY_PREFIX", "global-bus-replies-p")
    subscription = os.getenv("LUCIUS_SERVICEBUS_REPLY_SUBSCRIPTION")
    if not connection or not subscription:
        pytest.skip("Service Bus env not set for real SDK test")

    # Restrict to lane 0 for test.
    monkeypatch.setenv("LUCIUS_SERVICEBUS_LANES", "1")
    monkeypatch.setenv("LUCIUS_SERVICEBUS_REPLY_PREFIX", reply_prefix)
    monkeypatch.setenv("LUCIUS_SERVICEBUS_REPLY_SUBSCRIPTION", subscription)
    monkeypatch.setenv("LUCIUS_SERVICEBUS_CONNECTION", connection)

    state = _build_memory_state(_job(), _step())
    monkeypatch.setattr(invoker_app, "_build_stores", lambda _settings: (state.jobs_store, state.steps_store, state.inflight_store))

    with TestClient(invoker_app.create_app()):
        reply_topic = f"{reply_prefix}0"

        ack_payload = {
            "type": "ACK",
            "jobId": "job1",
            "stepId": "step1",
            "tenant_id": "t1",
            "attempt_no": 1,
            "lease_id": "lease1",
        }
        result_payload = {
            "type": "RESULT",
            "jobId": "job1",
            "stepId": "step1",
            "tenant_id": "t1",
            "attempt_no": 1,
            "lease_id": "lease1",
            "status": "SUCCEEDED",
            "output_ref": {"uri": "s3://bucket/output.json"},
        }

        _publish(connection, reply_topic, ack_payload)
        _publish(connection, reply_topic, result_payload)

        _await_state(state, "job1", "SUCCEEDED")

    job = state.jobs_store.get_job("job1", "t1")
    step = state.steps_store.get_steps("job1")[0]
    assert job is not None
    assert step is not None
    assert job.state == "SUCCEEDED"
    assert job.final_output == "s3://bucket/output.json"
    assert step.state == "SUCCEEDED"
