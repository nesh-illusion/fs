from typing import List

import consumer
from models.directive_model import Directive
from idempotency.store import MemoryIdempotencyStore


def _payload() -> dict:
    return {
        "jobId": "job1",
        "tenant_id": "t1",
        "stepId": "ocr",
        "protocol_id": "p1",
        "step_type": "OCR",
        "attempt_no": 1,
        "lease_id": "lease1",
        "input_ref": "in",
        "workspace_ref": None,
        "output_ref": "out",
        "payload": {},
        "lane": 0,
    }


def test_consumer_sends_ack_and_result(monkeypatch):
    calls: List[str] = []

    def fake_ack(_directive: Directive) -> None:
        calls.append("ack")

    def fake_result(_directive: Directive, **_kwargs) -> None:
        calls.append("result")

    monkeypatch.setattr(consumer, "send_ack", fake_ack)
    monkeypatch.setattr(consumer, "send_result", fake_result)
    monkeypatch.setattr(consumer, "build_idempotency_store", MemoryIdempotencyStore)

    consumer.handle_message(_payload())
    assert calls == ["ack", "result"]


def test_consumer_idempotent(monkeypatch):
    calls: List[str] = []
    store = MemoryIdempotencyStore()

    def fake_ack(_directive: Directive) -> None:
        calls.append("ack")

    def fake_result(_directive: Directive, **_kwargs) -> None:
        calls.append("result")

    def fake_store() -> MemoryIdempotencyStore:
        return store

    monkeypatch.setattr(consumer, "send_ack", fake_ack)
    monkeypatch.setattr(consumer, "send_result", fake_result)
    monkeypatch.setattr(consumer, "build_idempotency_store", fake_store)

    consumer.handle_message(_payload())
    consumer.handle_message(_payload())
    assert calls == ["ack", "result"]
