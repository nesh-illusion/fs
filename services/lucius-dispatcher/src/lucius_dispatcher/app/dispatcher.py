from datetime import datetime, timezone
from typing import Callable, List

from ledger.interfaces import OutboxStore, StepsStore
from ledger.models import OutboxEntry

from .publisher import Publisher
from shared.logging import get_logger, log_event


class OutboxDispatcher:
    def __init__(self, outbox_store: OutboxStore, steps_store: StepsStore):
        self._outbox_store = outbox_store
        self._steps_store = steps_store
        self._logger = get_logger("lucius_dispatcher")

    def _mark_step_awaiting_ack(self, entry: OutboxEntry) -> None:
        steps = self._steps_store.get_steps(entry.job_id)
        step = next((item for item in steps if item.step_id == entry.step_id), None)
        if step is None:
            return
        if step.state != "DISPATCHING":
            return
        if step.attempt_no != entry.attempt_no or step.lease_id != entry.lease_id:
            return
        step.state = "AWAITING_ACK"
        step.updated_at = now_iso()
        self._steps_store.update_step(step, step.etag or "")

    def dispatch_once(
        self,
        partition_key: str,
        limit: int,
        publish: Publisher,
    ) -> List[OutboxEntry]:
        sent: List[OutboxEntry] = []
        for entry in self._outbox_store.list_pending(partition_key, limit):
            log_event(
                self._logger,
                "outbox.publish",
                outbox_id=entry.outbox_id,
                job_id=entry.job_id,
                step_id=entry.step_id,
                tenant_id=entry.tenant_id,
                topic=entry.topic,
                partition=entry.partition,
                attempt_no=entry.attempt_no,
                lease_id=entry.lease_id,
            )
            publish.publish(entry)
            updated = self._outbox_store.mark_sent(entry.outbox_id, partition_key, entry.etag or "")
            self._mark_step_awaiting_ack(updated)
            log_event(
                self._logger,
                "outbox.sent",
                outbox_id=updated.outbox_id,
                job_id=updated.job_id,
                step_id=updated.step_id,
                tenant_id=updated.tenant_id,
                topic=updated.topic,
                partition=updated.partition,
            )
            sent.append(updated)
        return sent


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()
