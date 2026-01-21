from datetime import datetime, timezone
from typing import Callable, List

from lucius_orchestrator.ledger.interfaces import OutboxStore, StepsStore
from lucius_orchestrator.ledger.models import OutboxEntry

from .publisher import Publisher


class OutboxDispatcher:
    def __init__(self, outbox_store: OutboxStore, steps_store: StepsStore):
        self._outbox_store = outbox_store
        self._steps_store = steps_store

    def dispatch_once(
        self,
        partition_key: str,
        limit: int,
        publish: Publisher,
    ) -> List[OutboxEntry]:
        sent: List[OutboxEntry] = []
        for entry in self._outbox_store.list_pending(partition_key, limit):
            publish.publish(entry)
            updated = self._outbox_store.mark_sent(entry.outbox_id, partition_key, entry.etag or "")
            sent.append(updated)
        return sent


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()
