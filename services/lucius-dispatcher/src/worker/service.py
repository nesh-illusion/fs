from dataclasses import dataclass

from ledger.interfaces import JobsStore, OutboxStore, StepsStore
from .dispatcher import OutboxDispatcher
from .publisher import Publisher
from .sweepers import AckSweeper, JobDriftSweeper, LeaseSweeper


@dataclass
class ReconciliationConfig:
    ack_timeout_seconds: int = 30
    lease_timeout_seconds: int = 900
    outbox_batch_size: int = 100


class ReconciliationService:
    def __init__(
        self,
        jobs_store: JobsStore,
        steps_store: StepsStore,
        outbox_store: OutboxStore,
        config: ReconciliationConfig,
    ) -> None:
        self._jobs_store = jobs_store
        self._steps_store = steps_store
        self._outbox_store = outbox_store
        self._dispatcher = OutboxDispatcher(outbox_store, steps_store)
        self._ack_sweeper = AckSweeper(steps_store, config.ack_timeout_seconds)
        self._lease_sweeper = LeaseSweeper(steps_store, config.lease_timeout_seconds)
        self._config = config

    def dispatch_partition(self, partition_key: str, publish: Publisher) -> None:
        self._dispatcher.dispatch_once(partition_key, self._config.outbox_batch_size, publish)

    def sweep_job(self, job_id: str, tenant_id: str, tenant_bucket: str) -> None:
        self._ack_sweeper.sweep(job_id)
        self._lease_sweeper.sweep(job_id)
        JobDriftSweeper(self._jobs_store, self._steps_store).sweep(job_id, tenant_id, tenant_bucket)
