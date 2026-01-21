from dataclasses import replace
from typing import Any, Dict, List, Optional

from .interfaces import IdempotencyStore, JobIndexStore, JobsStore, OutboxStore, StepsStore, TenantInflightStore
from .models import Job, JobIdempotency, JobIndex, OutboxEntry, Step

try:
    from azure.data.tables import TableServiceClient, UpdateMode
except ImportError:  # pragma: no cover - dependency not installed yet
    TableServiceClient = None
    UpdateMode = None


class TableStorageError(RuntimeError):
    pass


def _require_sdk() -> None:
    if TableServiceClient is None:
        raise TableStorageError("azure-data-tables dependency is not installed")


def _job_entity(job: Job) -> Dict[str, Any]:
    return {
        "PartitionKey": job.tenant_bucket,
        "RowKey": job.job_id,
        "tenant_id": job.tenant_id,
        "tenant_bucket": job.tenant_bucket,
        "request_type": job.request_type,
        "doc_id": job.doc_id,
        "protocol_id": job.protocol_id,
        "mode": job.mode,
        "state": job.state,
        "idempotency_key": job.idempotency_key,
        "idempotency_hash": job.idempotency_hash,
        "current_step_id": job.current_step_id,
        "current_step_index": job.current_step_index,
        "attempts_total": job.attempts_total,
        "created_at": job.created_at,
        "updated_at": job.updated_at,
        "completed_at": job.completed_at,
        "error_code": job.error_code,
        "error_message": job.error_message,
        "correlation_id": job.correlation_id,
        "traceparent": job.traceparent,
    }


def _job_from_entity(entity: Dict[str, Any]) -> Job:
    return Job(
        job_id=entity["RowKey"],
        tenant_id=entity["tenant_id"],
        tenant_bucket=entity.get("tenant_bucket", entity["PartitionKey"]),
        request_type=entity["request_type"],
        doc_id=entity.get("doc_id"),
        protocol_id=entity["protocol_id"],
        mode=entity["mode"],
        state=entity["state"],
        idempotency_key=entity.get("idempotency_key"),
        idempotency_hash=entity["idempotency_hash"],
        current_step_id=entity.get("current_step_id"),
        current_step_index=entity.get("current_step_index"),
        attempts_total=entity.get("attempts_total", 0),
        created_at=entity["created_at"],
        updated_at=entity["updated_at"],
        completed_at=entity.get("completed_at"),
        error_code=entity.get("error_code"),
        error_message=entity.get("error_message"),
        correlation_id=entity.get("correlation_id"),
        traceparent=entity.get("traceparent"),
        etag=entity.get("etag") or entity.get("odata.etag"),
    )


class TableJobsStore(JobsStore):
    def __init__(self, service_client: TableServiceClient, table_name: str) -> None:
        _require_sdk()
        self._table = service_client.get_table_client(table_name)

    def create_job(self, job: Job) -> Job:
        entity = _job_entity(job)
        self._table.create_entity(entity)
        created = replace(job, etag=None)
        return created

    def get_job(self, job_id: str, tenant_id: str, tenant_bucket: Optional[str] = None) -> Optional[Job]:
        try:
            partition_key = tenant_bucket or tenant_id
            entity = self._table.get_entity(partition_key=partition_key, row_key=job_id)
        except Exception:
            return None
        return _job_from_entity(entity)

    def update_job(self, job: Job, etag: str) -> Job:
        entity = _job_entity(job)
        self._table.update_entity(entity, mode=UpdateMode.REPLACE, etag=etag)
        return replace(job, etag=None)

    def find_by_idempotency(self, tenant_id: str, idempotency_hash: str) -> Optional[str]:
        raise TableStorageError("use IdempotencyStore for idempotency lookups")


class TableStepsStore(StepsStore):
    def __init__(self, service_client: TableServiceClient, table_name: str) -> None:
        _require_sdk()
        self._table = service_client.get_table_client(table_name)

    def create_steps(self, job_id: str, steps: List[Step]) -> None:
        for step in steps:
            entity = {
                "PartitionKey": job_id,
                "RowKey": f"{step.step_index:04d}#{step.step_id}",
                "step_index": step.step_index,
                "step_id": step.step_id,
                "step_type": step.step_type,
                "service": step.service,
                "state": step.state,
                "attempt_no": step.attempt_no,
                "lease_id": step.lease_id,
                "lease_expires_at": step.lease_expires_at,
                "input_ref": step.input_ref,
                "workspace_ref": step.workspace_ref,
                "output_ref": step.output_ref,
                "payload": step.payload,
                "resolved_mode": step.resolved_mode,
                "lane": step.lane,
                "routing_key_used": step.routing_key_used,
                "decision_source": step.decision_source,
                "decision_reason": step.decision_reason,
                "last_error_code": step.last_error_code,
                "last_error_message": step.last_error_message,
                "created_at": step.created_at,
                "updated_at": step.updated_at,
                "completed_at": step.completed_at,
            }
            self._table.create_entity(entity)

    def get_steps(self, job_id: str) -> List[Step]:
        entities = self._table.query_entities(f"PartitionKey eq '{job_id}'")
        steps: List[Step] = []
        for entity in entities:
            steps.append(
                Step(
                    job_id=job_id,
                    step_index=entity.get("step_index", 0),
                    step_id=entity["step_id"],
                    step_type=entity["step_type"],
                    service=entity["service"],
                    state=entity["state"],
                    attempt_no=entity.get("attempt_no", 0),
                    lease_id=entity.get("lease_id", ""),
                    lease_expires_at=entity.get("lease_expires_at", ""),
                    input_ref=entity.get("input_ref"),
                    workspace_ref=entity.get("workspace_ref"),
                    output_ref=entity.get("output_ref"),
                    payload=entity.get("payload", {}),
                    resolved_mode=entity.get("resolved_mode", "DEFAULT"),
                    lane=entity.get("lane", 0),
                    routing_key_used=entity.get("routing_key_used", ""),
                    decision_source=entity.get("decision_source", "UNKNOWN"),
                    decision_reason=entity.get("decision_reason", ""),
                    last_error_code=entity.get("last_error_code"),
                    last_error_message=entity.get("last_error_message"),
                    created_at=entity["created_at"],
                    updated_at=entity["updated_at"],
                    completed_at=entity.get("completed_at"),
                    etag=entity.get("etag") or entity.get("odata.etag"),
                )
            )
        return sorted(steps, key=lambda step: step.step_index)

    def get_active_step(self, job_id: str) -> Optional[Step]:
        steps = self.get_steps(job_id)
        for step in steps:
            if step.state not in {"SUCCEEDED", "FAILED_FINAL", "CANCELLED"}:
                return step
        return None

    def update_step(self, step: Step, etag: str) -> Step:
        entity = {
            "PartitionKey": step.job_id,
            "RowKey": f"{step.step_index:04d}#{step.step_id}",
            "step_index": step.step_index,
            "step_id": step.step_id,
            "step_type": step.step_type,
            "service": step.service,
            "state": step.state,
            "attempt_no": step.attempt_no,
            "lease_id": step.lease_id,
            "lease_expires_at": step.lease_expires_at,
            "input_ref": step.input_ref,
            "workspace_ref": step.workspace_ref,
            "output_ref": step.output_ref,
            "payload": step.payload,
            "resolved_mode": step.resolved_mode,
            "lane": step.lane,
            "routing_key_used": step.routing_key_used,
            "decision_source": step.decision_source,
            "decision_reason": step.decision_reason,
            "last_error_code": step.last_error_code,
            "last_error_message": step.last_error_message,
            "created_at": step.created_at,
            "updated_at": step.updated_at,
            "completed_at": step.completed_at,
        }
        self._table.update_entity(entity, mode=UpdateMode.REPLACE, etag=etag)
        return replace(step, etag=None)


class TableOutboxStore(OutboxStore):
    def __init__(self, service_client: TableServiceClient, table_name: str) -> None:
        _require_sdk()
        self._table = service_client.get_table_client(table_name)

    def create_outbox(self, entry: OutboxEntry) -> OutboxEntry:
        entity = {
            "PartitionKey": entry.tenant_bucket,
            "RowKey": entry.outbox_id,
            "jobId": entry.job_id,
            "stepId": entry.step_id,
            "tenant_id": entry.tenant_id,
            "tenant_bucket": entry.tenant_bucket,
            "attempt_no": entry.attempt_no,
            "lease_id": entry.lease_id,
            "state": entry.state,
            "topic": entry.topic,
            "partition": entry.partition,
            "payload": entry.payload,
            "resolved_mode": entry.resolved_mode,
            "lane": entry.lane,
            "routing_key_used": entry.routing_key_used,
            "decision_source": entry.decision_source,
            "decision_reason": entry.decision_reason,
            "created_at": entry.created_at,
            "sent_at": entry.sent_at,
            "updated_at": entry.updated_at,
        }
        self._table.create_entity(entity)
        return replace(entry, etag=None)

    def list_pending(self, partition_key: str, limit: int) -> List[OutboxEntry]:
        entities = self._table.query_entities(
            f"PartitionKey eq '{partition_key}' and state eq 'PENDING'"
        )
        entries: List[OutboxEntry] = []
        for entity in entities:
            entries.append(
                OutboxEntry(
                    outbox_id=entity["RowKey"],
                    tenant_id=entity.get("tenant_id", entity["PartitionKey"]),
                    tenant_bucket=entity.get("tenant_bucket", entity["PartitionKey"]),
                    job_id=entity["jobId"],
                    step_id=entity["stepId"],
                    attempt_no=entity.get("attempt_no", 0),
                    lease_id=entity.get("lease_id", ""),
                    state=entity["state"],
                    topic=entity.get("topic", ""),
                    partition=entity.get("partition", ""),
                    payload=entity.get("payload", {}),
                    resolved_mode=entity.get("resolved_mode", "DEFAULT"),
                    lane=entity.get("lane", 0),
                    routing_key_used=entity.get("routing_key_used", ""),
                    decision_source=entity.get("decision_source", "UNKNOWN"),
                    decision_reason=entity.get("decision_reason", ""),
                    created_at=entity["created_at"],
                    sent_at=entity.get("sent_at"),
                    updated_at=entity["updated_at"],
                    etag=entity.get("etag") or entity.get("odata.etag"),
                )
            )
            if len(entries) >= limit:
                break
        return entries

    def mark_sent(self, outbox_id: str, partition_key: str, etag: str) -> OutboxEntry:
        entity = self._table.get_entity(partition_key=partition_key, row_key=outbox_id)
        entity["state"] = "SENT"
        self._table.update_entity(entity, mode=UpdateMode.REPLACE, etag=etag)
        return OutboxEntry(
            outbox_id=entity["RowKey"],
            tenant_id=entity.get("tenant_id", entity["PartitionKey"]),
            tenant_bucket=entity.get("tenant_bucket", entity["PartitionKey"]),
            job_id=entity["jobId"],
            step_id=entity["stepId"],
            attempt_no=entity.get("attempt_no", 0),
            lease_id=entity.get("lease_id", ""),
            state=entity["state"],
            topic=entity.get("topic", ""),
            partition=entity.get("partition", ""),
            payload=entity.get("payload", {}),
            resolved_mode=entity.get("resolved_mode", "DEFAULT"),
            lane=entity.get("lane", 0),
            routing_key_used=entity.get("routing_key_used", ""),
            decision_source=entity.get("decision_source", "UNKNOWN"),
            decision_reason=entity.get("decision_reason", ""),
            created_at=entity["created_at"],
            sent_at=entity.get("sent_at"),
            updated_at=entity["updated_at"],
            etag=entity.get("etag") or entity.get("odata.etag"),
        )


class TableIdempotencyStore(IdempotencyStore):
    def __init__(self, service_client: TableServiceClient, table_name: str) -> None:
        _require_sdk()
        self._table = service_client.get_table_client(table_name)

    def put(self, record: JobIdempotency) -> None:
        entity = {
            "PartitionKey": record.tenant_id,
            "RowKey": record.idempotency_hash,
            "jobId": record.job_id,
            "created_at": record.created_at,
        }
        self._table.create_entity(entity)

    def get(self, tenant_id: str, idempotency_hash: str) -> Optional[JobIdempotency]:
        try:
            entity = self._table.get_entity(partition_key=tenant_id, row_key=idempotency_hash)
        except Exception:
            return None
        return JobIdempotency(
            tenant_id=entity["PartitionKey"],
            idempotency_hash=entity["RowKey"],
            job_id=entity["jobId"],
            created_at=entity["created_at"],
        )


class TableJobIndexStore(JobIndexStore):
    def __init__(self, service_client: TableServiceClient, table_name: str) -> None:
        _require_sdk()
        self._table = service_client.get_table_client(table_name)

    def put(self, record: JobIndex) -> None:
        entity = {
            "PartitionKey": record.job_id,
            "RowKey": record.job_id,
            "tenant_id": record.tenant_id,
            "tenant_bucket": record.tenant_bucket,
            "created_at": record.created_at,
        }
        self._table.create_entity(entity)

    def get(self, job_id: str) -> Optional[JobIndex]:
        try:
            entity = self._table.get_entity(partition_key=job_id, row_key=job_id)
        except Exception:
            return None
        return JobIndex(
            job_id=job_id,
            tenant_id=entity["tenant_id"],
            tenant_bucket=entity["tenant_bucket"],
            created_at=entity["created_at"],
        )


class TableTenantInflightStore(TenantInflightStore):
    def __init__(self, service_client: TableServiceClient, table_name: str) -> None:
        _require_sdk()
        self._table = service_client.get_table_client(table_name)

    def try_acquire(self, tenant_id: str, limit: int) -> bool:
        for _ in range(5):
            try:
                entity = self._table.get_entity(partition_key=tenant_id, row_key="inflight")
                count = int(entity.get("count", 0))
                etag = entity.get("etag") or entity.get("odata.etag")
            except Exception:
                count = 0
                etag = None
                entity = {
                    "PartitionKey": tenant_id,
                    "RowKey": "inflight",
                    "count": 0,
                }

            if count >= limit:
                return False

            entity["count"] = count + 1
            try:
                if etag:
                    self._table.update_entity(entity, mode=UpdateMode.REPLACE, etag=etag)
                else:
                    self._table.create_entity(entity)
                return True
            except Exception:
                continue
        raise TableStorageError("failed to acquire inflight slot after retries")

    def release(self, tenant_id: str) -> None:
        for _ in range(5):
            try:
                entity = self._table.get_entity(partition_key=tenant_id, row_key="inflight")
            except Exception:
                return
            count = int(entity.get("count", 0))
            etag = entity.get("etag") or entity.get("odata.etag")
            if count <= 1:
                entity["count"] = 0
            else:
                entity["count"] = count - 1
            try:
                self._table.update_entity(entity, mode=UpdateMode.REPLACE, etag=etag)
                return
            except Exception:
                continue
        raise TableStorageError("failed to release inflight slot after retries")
