from datetime import datetime, timezone

from lucius_dispatcher.app.dispatcher import OutboxDispatcher
from ledger.memory_store import MemoryOutboxStore, MemoryStepsStore
from ledger.models import OutboxEntry, Step


class _CapturePublisher:
    def __init__(self) -> None:
        self.entries = []

    def publish(self, entry: OutboxEntry) -> None:
        self.entries.append(entry)


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def test_dispatch_marks_outbox_sent_and_step_awaiting_ack():
    outbox_store = MemoryOutboxStore()
    steps_store = MemoryStepsStore()
    dispatcher = OutboxDispatcher(outbox_store, steps_store)
    publisher = _CapturePublisher()

    job_id = "job1"
    step_id = "ocr"
    lease_id = "lease1"
    steps_store.create_steps(
        job_id,
        [
            Step(
                job_id=job_id,
                step_index=0,
                step_id=step_id,
                step_type="OCR",
                service="platform-ocr",
                state="DISPATCHING",
                attempt_no=1,
                lease_id=lease_id,
                lease_expires_at=_now_iso(),
                input_ref="in",
                workspace_ref={},
                output_ref="out",
                payload={},
                resolved_mode="DEFAULT",
                lane=0,
                routing_key_used="t1",
                decision_source="REQUEST",
                decision_reason="request.mode",
                last_error_code=None,
                last_error_message=None,
                created_at=_now_iso(),
                updated_at=_now_iso(),
                completed_at=None,
            )
        ],
    )

    outbox = outbox_store.create_outbox(
        OutboxEntry(
            outbox_id="out1",
            tenant_id="t1",
            tenant_bucket="t1#202401",
            job_id=job_id,
            step_id=step_id,
            attempt_no=1,
            lease_id=lease_id,
            state="PENDING",
            topic="global-bus-p0",
            partition="0",
            payload={},
            resolved_mode="DEFAULT",
            lane=0,
            routing_key_used="t1",
            decision_source="REQUEST",
            decision_reason="request.mode",
            created_at=_now_iso(),
            sent_at=None,
            updated_at=_now_iso(),
        )
    )

    sent = dispatcher.dispatch_once("t1#202401", 10, publisher)
    assert sent
    assert publisher.entries

    updated_outbox = sent[0]
    assert updated_outbox.state == "SENT"
    assert updated_outbox.outbox_id == outbox.outbox_id

    updated_step = steps_store.get_steps(job_id)[0]
    assert updated_step.state == "AWAITING_ACK"
