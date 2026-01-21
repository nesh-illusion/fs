import os
import pytest

from idempotency.store import MemoryIdempotencyStore, build_idempotency_store
from models.directive_model import Directive


def _directive() -> Directive:
    return Directive(
        jobId="job1",
        tenant_id="t1",
        stepId="ocr",
        protocol_id="p1",
        step_type="OCR",
        attempt_no=1,
        lease_id="lease1",
        input_ref="in",
        workspace_ref=None,
        output_ref="out",
        payload={},
        callback_urls={},
    )


def test_memory_idempotency_roundtrip():
    store = MemoryIdempotencyStore()
    directive = _directive()
    assert store.seen(directive) is False
    store.mark_seen(directive)
    assert store.seen(directive) is True


def test_table_backend_requires_connection(monkeypatch):
    monkeypatch.setenv("IDEMPOTENCY_BACKEND", "table")
    monkeypatch.delenv("IDEMPOTENCY_TABLE_CONNECTION", raising=False)
    with pytest.raises(RuntimeError):
        build_idempotency_store()
