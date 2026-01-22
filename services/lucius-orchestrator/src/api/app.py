import asyncio
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Iterable
from uuid import uuid4
import zlib

from fastapi import FastAPI, Header, HTTPException
from fastapi.responses import JSONResponse

from config.protocols import ProtocolRegistry
from config.settings import AppSettings
from ledger.memory_store import (
    MemoryJobIndexStore,
    MemoryIdempotencyStore,
    MemoryJobsStore,
    MemoryOutboxStore,
    MemoryStepsStore,
    MemoryTenantInflightStore,
)
from ledger.models import Job, JobIdempotency, JobIndex, OutboxEntry, Step
from ledger.table_storage import (
    TableIdempotencyStore,
    TableJobIndexStore,
    TableJobsStore,
    TableLedgerJobsStore,
    TableLedgerOutboxStore,
    TableLedgerStepsStore,
    TableOutboxStore,
    TableStepsStore,
    TableTenantInflightStore,
)
from outbox.publisher import AsyncServiceBusPublisher, Publisher
from validation.idempotency import idempotency_hash
from validation.validator import SchemaValidator
from shared.logging import get_logger, log_event

try:
    import jsonschema
except ImportError:  # pragma: no cover - dependency not installed yet
    jsonschema = None


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _tenant_bucket(tenant_id: str, created_at: str) -> str:
    bucket = created_at[:7].replace("-", "")
    return f"{tenant_id}#{bucket}"


def _lease_expires_at(minutes: int = 15) -> str:
    return (datetime.now(timezone.utc) + timedelta(minutes=minutes)).isoformat()


def _next_attempt_at(delay_seconds: float) -> str:
    return (datetime.now(timezone.utc) + timedelta(seconds=delay_seconds)).isoformat()


def _normalize(value: str) -> str:
    return value.strip().lower()


def _routing_decision(envelope: Dict[str, Any]) -> Dict[str, Any]:
    requested_mode = envelope.get("mode", "DEFAULT")
    resolved_mode = requested_mode
    decision_source = "REQUEST" if "mode" in envelope else "GLOBAL_CONFIG"
    decision_reason = "request.mode" if "mode" in envelope else "default"

    tenant_norm = _normalize(envelope["tenant_id"])
    doc_id = envelope.get("doc_id")
    if resolved_mode == "BURST":
        if not doc_id:
            raise HTTPException(status_code=400, detail="doc_id required for BURST mode")
        doc_norm = _normalize(doc_id)
        routing_key_used = tenant_norm + doc_norm
    else:
        routing_key_used = tenant_norm

    lane = zlib.crc32(routing_key_used.encode("utf-8")) % 16
    return {
        "resolved_mode": resolved_mode,
        "lane": lane,
        "routing_key_used": routing_key_used,
        "decision_source": decision_source,
        "decision_reason": decision_reason,
    }


def _outbox_partitions(settings: AppSettings, count: int = 16) -> Iterable[str]:
    if settings.storage_backend == "ledger":
        return ["*"]
    return [f"p{lane}" for lane in range(count)]


def _outbox_partition_key(settings: AppSettings, job_id: str, routing: Dict[str, Any]) -> str:
    if settings.storage_backend == "ledger":
        return job_id
    return f"p{routing['lane']}"


def _build_stores(settings: AppSettings):
    if settings.storage_backend == "memory":
        return (
            MemoryJobsStore(),
            MemoryStepsStore(),
            MemoryOutboxStore(),
            MemoryIdempotencyStore(),
            MemoryJobIndexStore(),
            MemoryTenantInflightStore(),
        )

    if settings.storage_backend not in {"table", "ledger"}:
        raise RuntimeError(f"Unsupported storage backend: {settings.storage_backend}")

    if not settings.table_connection_string:
        raise RuntimeError("LUCIUS_TABLE_CONNECTION is required for table storage")

    from azure.data.tables import TableServiceClient

    service_client = TableServiceClient.from_connection_string(settings.table_connection_string)
    if settings.storage_backend == "ledger":
        ledger_table = service_client.get_table_client(settings.ledger_table)
        return (
            TableLedgerJobsStore(ledger_table),
            TableLedgerStepsStore(ledger_table),
            TableLedgerOutboxStore(ledger_table),
            TableIdempotencyStore(service_client, settings.idempotency_table),
            TableJobIndexStore(service_client, settings.job_index_table),
            TableTenantInflightStore(service_client, settings.inflight_table),
        )
    return (
        TableJobsStore(service_client, settings.jobs_table),
        TableStepsStore(service_client, settings.steps_table),
        TableOutboxStore(service_client, settings.outbox_table),
        TableIdempotencyStore(service_client, settings.idempotency_table),
        TableJobIndexStore(service_client, settings.job_index_table),
        TableTenantInflightStore(service_client, settings.inflight_table),
    )


def _find_step(steps: list[Step], step_id: str) -> Step | None:
    return next((step for step in steps if step.step_id == step_id), None)


def create_app() -> FastAPI:
    @asynccontextmanager
    async def lifespan(app: FastAPI):
        if settings.outbox_retry_enabled:
            app.state.outbox_retry_task = asyncio.create_task(_retry_loop())
        try:
            yield
        finally:
            task = getattr(app.state, "outbox_retry_task", None)
            if task:
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass

    app = FastAPI(title="LUCIUS Orchestrator", lifespan=lifespan)
    logger = get_logger("lucius_orchestrator")

    settings = AppSettings.from_env()
    validator = SchemaValidator()
    registry = ProtocolRegistry()
    jobs_store, steps_store, outbox_store, idempotency_store, job_index_store, inflight_store = _build_stores(settings)
    publisher: Publisher | None = None
    if settings.service_bus_connection:
        publisher = AsyncServiceBusPublisher(
            settings.service_bus_connection,
            settings.outbox_publish_timeout_seconds,
        )
    app.state.jobs_store = jobs_store
    app.state.steps_store = steps_store
    app.state.outbox_store = outbox_store
    app.state.idempotency_store = idempotency_store
    app.state.job_index_store = job_index_store
    app.state.inflight_store = inflight_store
    app.state.outbox_publisher = publisher
    app.state.registry = registry
    app.state.validator = validator

    async def _publish_entry(entry: OutboxEntry) -> bool:
        if publisher is None:
            return False
        try:
            log_event(
                logger,
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
            await publisher.publish(entry)
        except Exception as exc:
            log_event(
                logger,
                "outbox.publish.failed",
                outbox_id=entry.outbox_id,
                job_id=entry.job_id,
                step_id=entry.step_id,
                tenant_id=entry.tenant_id,
                error=str(exc),
            )
            return False
        return True

    def _mark_publish_success(entry: OutboxEntry, job: Job | None, step: Step | None) -> None:
        marked_entry = entry
        if entry.etag is None:
            pending = outbox_store.list_pending(entry.tenant_bucket, settings.outbox_retry_batch_size)
            marked_entry = next((item for item in pending if item.outbox_id == entry.outbox_id), entry)
        outbox_store.mark_sent(marked_entry.outbox_id, marked_entry.tenant_bucket, marked_entry.etag or "")
        log_event(
            logger,
            "outbox.sent",
            outbox_id=marked_entry.outbox_id,
            job_id=marked_entry.job_id,
            step_id=marked_entry.step_id,
            tenant_id=marked_entry.tenant_id,
            topic=marked_entry.topic,
            partition=marked_entry.partition,
        )
        now = _now_iso()
        if step and step.state in {"DISPATCHING", "PENDING"}:
            if step.etag is None:
                step = _find_step(steps_store.get_steps(step.job_id), step.step_id) or step
            step.state = "INITIATED"
            step.updated_at = now
            steps_store.update_step(step, step.etag or "")
        if job and job.state in {"QUEUED", "DISPATCHING"}:
            if job.etag is None:
                index = job_index_store.get(job.job_id)
                if index:
                    job = jobs_store.get_job(job.job_id, job.tenant_id, index.tenant_bucket) or job
            job.state = "IN_PROGRESS"
            job.updated_at = now
            jobs_store.update_job(job, job.etag or "")

    def _should_retry(entry: OutboxEntry, now: datetime) -> bool:
        if entry.next_attempt_at is None:
            return True
        try:
            ready_at = datetime.fromisoformat(entry.next_attempt_at)
        except ValueError:
            return True
        return now >= ready_at

    def _schedule_retry(entry: OutboxEntry) -> OutboxEntry:
        base_entry = entry
        if entry.etag is None:
            pending = outbox_store.list_pending(entry.tenant_bucket, settings.outbox_retry_batch_size)
            base_entry = next((item for item in pending if item.outbox_id == entry.outbox_id), entry)
        updated = OutboxEntry(
            outbox_id=entry.outbox_id,
            tenant_id=entry.tenant_id,
            tenant_bucket=entry.tenant_bucket,
            job_id=entry.job_id,
            step_id=entry.step_id,
            attempt_no=entry.attempt_no,
            lease_id=entry.lease_id,
            state="PENDING",
            topic=entry.topic,
            partition=entry.partition,
            payload=entry.payload,
            resolved_mode=entry.resolved_mode,
            lane=entry.lane,
            routing_key_used=entry.routing_key_used,
            decision_source=entry.decision_source,
            decision_reason=entry.decision_reason,
            created_at=entry.created_at,
            next_attempt_at=_next_attempt_at(settings.outbox_retry_delay_seconds),
            sent_at=entry.sent_at,
            updated_at=_now_iso(),
            etag=base_entry.etag,
        )
        return outbox_store.update_outbox(updated, base_entry.etag or "")

    def _refresh_attempt(entry: OutboxEntry, step: Step) -> tuple[OutboxEntry, Step]:
        lease_id = uuid4().hex
        attempt_no = (step.attempt_no or 0) + 1
        now = _now_iso()
        step.attempt_no = attempt_no
        step.lease_id = lease_id
        step.lease_expires_at = _lease_expires_at()
        step.state = "DISPATCHING"
        step.updated_at = now
        updated_step = steps_store.update_step(step, step.etag or "")

        payload = dict(entry.payload)
        payload["attempt_no"] = attempt_no
        payload["lease_id"] = lease_id

        refreshed = OutboxEntry(
            outbox_id=entry.outbox_id,
            tenant_id=entry.tenant_id,
            tenant_bucket=entry.tenant_bucket,
            job_id=entry.job_id,
            step_id=entry.step_id,
            attempt_no=attempt_no,
            lease_id=lease_id,
            state="PENDING",
            topic=entry.topic,
            partition=entry.partition,
            payload=payload,
            resolved_mode=entry.resolved_mode,
            lane=entry.lane,
            routing_key_used=entry.routing_key_used,
            decision_source=entry.decision_source,
            decision_reason=entry.decision_reason,
            created_at=entry.created_at,
            next_attempt_at=_now_iso(),
            sent_at=None,
            updated_at=now,
            etag=entry.etag,
        )
        updated_entry = outbox_store.update_outbox(refreshed, entry.etag or "")
        return updated_entry, updated_step

    def _mark_retry_exhausted(entry: OutboxEntry, job: Job, step: Step) -> None:
        now = _now_iso()
        step.state = "FAILED_FINAL"
        step.last_error_code = "RETRY_LIMIT"
        step.last_error_message = "outbox retry limit exceeded"
        step.completed_at = now
        step.updated_at = now
        steps_store.update_step(step, step.etag or "")

        job.state = "FAILED_FINAL"
        job.error_code = step.last_error_code
        job.error_message = step.last_error_message
        job.completed_at = now
        job.updated_at = now
        jobs_store.update_job(job, job.etag or "")
        inflight_store.release(job.tenant_id)

        exhausted = OutboxEntry(
            outbox_id=entry.outbox_id,
            tenant_id=entry.tenant_id,
            tenant_bucket=entry.tenant_bucket,
            job_id=entry.job_id,
            step_id=entry.step_id,
            attempt_no=entry.attempt_no,
            lease_id=entry.lease_id,
            state="FAILED_FINAL",
            topic=entry.topic,
            partition=entry.partition,
            payload=entry.payload,
            resolved_mode=entry.resolved_mode,
            lane=entry.lane,
            routing_key_used=entry.routing_key_used,
            decision_source=entry.decision_source,
            decision_reason=entry.decision_reason,
            created_at=entry.created_at,
            next_attempt_at=None,
            sent_at=entry.sent_at,
            updated_at=now,
            etag=entry.etag,
        )
        outbox_store.update_outbox(exhausted, entry.etag or "")

    async def _process_pending_outbox(partitions: Iterable[str], limit: int) -> None:
        if publisher is None:
            return
        now = datetime.now(timezone.utc)
        for partition in partitions:
            entries = outbox_store.list_pending(partition, limit)
            for entry in entries:
                if not _should_retry(entry, now):
                    continue
                index = job_index_store.get(entry.job_id)
                if index is None:
                    continue
                job = jobs_store.get_job(entry.job_id, entry.tenant_id, index.tenant_bucket)
                step = _find_step(steps_store.get_steps(entry.job_id), entry.step_id)
                if job is None or step is None:
                    continue
                if step.attempt_no >= settings.outbox_max_attempts:
                    _mark_retry_exhausted(entry, job, step)
                    continue
                try:
                    updated_entry, updated_step = _refresh_attempt(entry, step)
                except Exception as exc:
                    log_event(
                        logger,
                        "outbox.retry.claim_failed",
                        outbox_id=entry.outbox_id,
                        job_id=entry.job_id,
                        step_id=entry.step_id,
                        error=str(exc),
                    )
                    continue
                if await _publish_entry(updated_entry):
                    _mark_publish_success(updated_entry, job, updated_step)
                else:
                    _schedule_retry(updated_entry)

    async def _retry_pending_outbox() -> None:
        await _process_pending_outbox(_outbox_partitions(settings), settings.outbox_retry_batch_size)

    async def _publish_outbox_background(entry: OutboxEntry, tenant_id: str) -> None:
        try:
            if await _publish_entry(entry):
                index = job_index_store.get(entry.job_id)
                if index is None:
                    return
                job = jobs_store.get_job(entry.job_id, tenant_id, index.tenant_bucket)
                step = _find_step(steps_store.get_steps(entry.job_id), entry.step_id)
                _mark_publish_success(entry, job, step)
            else:
                _schedule_retry(entry)
        except Exception as exc:
            log_event(
                logger,
                "outbox.publish.background_failed",
                outbox_id=entry.outbox_id,
                job_id=entry.job_id,
                step_id=entry.step_id,
                tenant_id=tenant_id,
                error=str(exc),
            )

    async def _retry_loop() -> None:
        while True:
            try:
                await _retry_pending_outbox()
            except Exception as exc:
                log_event(logger, "outbox.retry.failed", error=str(exc))
            await asyncio.sleep(settings.outbox_retry_interval_seconds)

    @app.post("/v1/commands")
    async def create_command(envelope: Dict[str, Any]):
        log_event(
            logger,
            "command.received",
            tenant_id=envelope.get("tenant_id"),
            request_type=envelope.get("request_type"),
            mode=envelope.get("mode"),
            doc_id=envelope.get("doc_id"),
        )
        try:
            validator.validate_envelope(envelope)
        except Exception as exc:
            if jsonschema and isinstance(exc, jsonschema.ValidationError):
                raise HTTPException(status_code=422, detail=str(exc)) from exc
            raise

        request_type = envelope["request_type"]
        protocol = registry.resolve(request_type)
        if protocol is None:
            raise HTTPException(status_code=400, detail="unsupported request_type")

        idempotency_payload = {
            "tenant_id": envelope["tenant_id"],
            "request_type": envelope["request_type"],
            "doc_id": envelope.get("doc_id"),
            "input_ref": envelope["input_ref"],
            "output_ref": envelope["output_ref"],
            "payload": envelope["payload"],
            "schema_version": envelope["schema_version"],
        }
        digest = idempotency_hash(idempotency_payload)
        existing_job_id = jobs_store.find_by_idempotency(envelope["tenant_id"], digest)
        if existing_job_id:
            return JSONResponse(status_code=202, content={"jobId": existing_job_id})

        payload = envelope["payload"]
        step_schema_map = {step.step_id: step.payload_schema_ref for step in protocol.steps}
        try:
            validator.validate_protocol_payloads(payload, step_schema_map)
        except Exception as exc:
            if jsonschema and isinstance(exc, jsonschema.ValidationError):
                raise HTTPException(status_code=422, detail=str(exc)) from exc
            raise

        tenant_id = envelope["tenant_id"]
        if not inflight_store.try_acquire(tenant_id, settings.max_inflight_per_tenant):
            log_event(
                logger,
                "rate_limit.rejected",
                tenant_id=tenant_id,
                limit=settings.max_inflight_per_tenant,
            )
            raise HTTPException(status_code=429, detail="tenant inflight limit reached")

        try:
            routing = _routing_decision(envelope)
            log_event(
                logger,
                "routing.resolved",
                tenant_id=envelope["tenant_id"],
                resolved_mode=routing["resolved_mode"],
                lane=routing["lane"],
                routing_key_used=routing["routing_key_used"],
            )

            job_id = uuid4().hex
            created_at = _now_iso()
            tenant_bucket = _tenant_bucket(envelope["tenant_id"], created_at)
            lease_id = uuid4().hex
            lease_expires_at = _lease_expires_at()
            job = Job(
                job_id=job_id,
                tenant_id=envelope["tenant_id"],
                tenant_bucket=tenant_bucket,
                request_type=request_type,
                doc_id=envelope.get("doc_id"),
                protocol_id=protocol.protocol_id,
                mode=routing["resolved_mode"],
                state="QUEUED",
                idempotency_key=envelope.get("idempotency_key"),
                idempotency_hash=digest,
                current_step_id=protocol.steps[0].step_id,
                current_step_index=0,
                attempts_total=0,
                created_at=created_at,
                updated_at=created_at,
                completed_at=None,
                error_code=None,
                error_message=None,
                correlation_id=envelope.get("correlation_id"),
                traceparent=envelope.get("traceparent"),
            )
            job = jobs_store.create_job(job)

            steps = []
            for index, step in enumerate(protocol.steps):
                if isinstance(payload, dict) and "steps" in payload:
                    step_payload = payload["steps"].get(step.step_id, {})
                else:
                    step_payload = payload
                if index == 0:
                    step_state = "DISPATCHING"
                    attempt_no = 1
                    step_lease_id = lease_id
                    step_lease_expires_at = lease_expires_at
                else:
                    step_state = "PENDING"
                    attempt_no = 0
                    step_lease_id = ""
                    step_lease_expires_at = ""
                steps.append(
                    Step(
                        job_id=job_id,
                        step_index=index,
                        step_id=step.step_id,
                        step_type=step.step_type,
                        service=step.service,
                        state=step_state,
                        attempt_no=attempt_no,
                        lease_id=step_lease_id,
                        lease_expires_at=step_lease_expires_at,
                        input_ref=envelope["input_ref"],
                        workspace_ref=envelope.get("workspace_ref", {}),
                        output_ref=envelope["output_ref"],
                        payload=step_payload,
                        resolved_mode=routing["resolved_mode"],
                        lane=routing["lane"],
                        routing_key_used=routing["routing_key_used"],
                        decision_source=routing["decision_source"],
                        decision_reason=routing["decision_reason"],
                        last_error_code=None,
                        last_error_message=None,
                        created_at=created_at,
                        updated_at=created_at,
                        completed_at=None,
                    )
                )
            steps_store.create_steps(job_id, steps)

            outbox_partition = _outbox_partition_key(settings, job_id, routing)
            outbox = OutboxEntry(
                outbox_id=uuid4().hex,
                tenant_id=envelope["tenant_id"],
                tenant_bucket=outbox_partition,
                job_id=job_id,
                step_id=protocol.steps[0].step_id,
                attempt_no=1,
                lease_id=lease_id,
                state="PENDING",
                topic=f"global-bus-p{routing['lane']}",
                partition=str(routing["lane"]),
                payload={
                    "jobId": job_id,
                    "tenant_id": envelope["tenant_id"],
                    "stepId": protocol.steps[0].step_id,
                    "protocol_id": protocol.protocol_id,
                    "step_type": protocol.steps[0].step_type,
                    "attempt_no": 1,
                    "lease_id": lease_id,
                    "input_ref": envelope["input_ref"],
                    "workspace_ref": envelope.get("workspace_ref", {}),
                    "output_ref": envelope["output_ref"],
                    "payload": steps[0].payload,
                    "callback_urls": envelope.get("callback_urls", {}),
                    "mode": routing["resolved_mode"],
                    "lane": routing["lane"],
                    "routing_key_used": routing["routing_key_used"],
                },
                resolved_mode=routing["resolved_mode"],
                lane=routing["lane"],
                routing_key_used=routing["routing_key_used"],
                decision_source=routing["decision_source"],
                decision_reason=routing["decision_reason"],
                created_at=created_at,
                next_attempt_at=_now_iso(),
                sent_at=None,
                updated_at=created_at,
            )
            outbox_entry = outbox_store.create_outbox(outbox)
            log_event(
                logger,
                "outbox.created",
                job_id=job_id,
                step_id=protocol.steps[0].step_id,
                tenant_id=envelope["tenant_id"],
                topic=outbox.topic,
                partition=outbox.partition,
                attempt_no=outbox.attempt_no,
                lease_id=outbox.lease_id,
            )

            job.state = "DISPATCHING"
            job.updated_at = _now_iso()
            job = jobs_store.update_job(job, job.etag or "")

            idempotency_store.put(
                JobIdempotency(
                    tenant_id=envelope["tenant_id"],
                    idempotency_hash=digest,
                    job_id=job_id,
                    created_at=created_at,
                )
            )
            job_index_store.put(
                JobIndex(
                    job_id=job_id,
                    tenant_id=envelope["tenant_id"],
                    tenant_bucket=tenant_bucket,
                    created_at=created_at,
                )
            )

            if publisher is None:
                log_event(
                    logger,
                    "outbox.publish.skipped",
                    outbox_id=outbox_entry.outbox_id,
                    job_id=job_id,
                    tenant_id=envelope["tenant_id"],
                    reason="missing_service_bus",
                )
                _schedule_retry(outbox_entry)
            else:
                asyncio.create_task(_publish_outbox_background(outbox_entry, envelope["tenant_id"]))

            return JSONResponse(status_code=202, content={"jobId": job_id})
        except Exception:
            inflight_store.release(tenant_id)
            raise

    @app.post("/v1/callbacks/ack")
    async def ack_callback(payload: Dict[str, Any]):
        log_event(
            logger,
            "callback.ack.received",
            job_id=payload.get("jobId"),
            step_id=payload.get("stepId"),
            tenant_id=payload.get("tenant_id"),
            attempt_no=payload.get("attempt_no"),
            lease_id=payload.get("lease_id"),
        )
        job_id = payload.get("jobId")
        step_id = payload.get("stepId")
        tenant_id = payload.get("tenant_id")
        attempt_no = payload.get("attempt_no")
        lease_id = payload.get("lease_id")
        if not all([job_id, step_id, tenant_id, attempt_no, lease_id]):
            raise HTTPException(status_code=400, detail="missing required fields")

        index = job_index_store.get(job_id)
        if index is None or index.tenant_id != tenant_id:
            raise HTTPException(status_code=404, detail="job not found")
        job = jobs_store.get_job(job_id, tenant_id, index.tenant_bucket)
        if job is None:
            raise HTTPException(status_code=404, detail="job not found")

        step = _find_step(steps_store.get_steps(job_id), step_id)
        if step is None:
            raise HTTPException(status_code=404, detail="step not found")
        if step.attempt_no != attempt_no or step.lease_id != lease_id:
            raise HTTPException(status_code=409, detail="attempt/lease mismatch")
        if step.state not in {"INITIATED", "PROCESSING", "IN_PROGRESS"}:
            raise HTTPException(status_code=409, detail="invalid step state")

        if step.state == "INITIATED":
            step.state = "PROCESSING"
            step.updated_at = _now_iso()
            steps_store.update_step(step, step.etag or "")

        if job.state in {"QUEUED", "DISPATCHING"}:
            job.state = "IN_PROGRESS"
            job.updated_at = _now_iso()
            jobs_store.update_job(job, job.etag or "")

        log_event(
            logger,
            "callback.ack.accepted",
            job_id=job_id,
            step_id=step_id,
            tenant_id=tenant_id,
            attempt_no=attempt_no,
        )
        return {"status": "ok"}

    @app.post("/v1/callbacks/result")
    async def result_callback(payload: Dict[str, Any]):
        log_event(
            logger,
            "callback.result.received",
            job_id=payload.get("jobId"),
            step_id=payload.get("stepId"),
            tenant_id=payload.get("tenant_id"),
            attempt_no=payload.get("attempt_no"),
            lease_id=payload.get("lease_id"),
            status=payload.get("status"),
        )
        job_id = payload.get("jobId")
        step_id = payload.get("stepId")
        tenant_id = payload.get("tenant_id")
        attempt_no = payload.get("attempt_no")
        lease_id = payload.get("lease_id")
        status = payload.get("status")
        failure_class = payload.get("failure_class")
        error = payload.get("error") or {}
        if not all([job_id, step_id, tenant_id, attempt_no, lease_id, status]):
            raise HTTPException(status_code=400, detail="missing required fields")

        index = job_index_store.get(job_id)
        if index is None or index.tenant_id != tenant_id:
            raise HTTPException(status_code=404, detail="job not found")
        job = jobs_store.get_job(job_id, tenant_id, index.tenant_bucket)
        if job is None:
            raise HTTPException(status_code=404, detail="job not found")

        steps = steps_store.get_steps(job_id)
        step = _find_step(steps, step_id)
        if step is None:
            raise HTTPException(status_code=404, detail="step not found")
        if step.attempt_no != attempt_no or step.lease_id != lease_id:
            raise HTTPException(status_code=409, detail="attempt/lease mismatch")
        if step.state not in {"INITIATED", "PROCESSING", "IN_PROGRESS"}:
            raise HTTPException(status_code=409, detail="invalid step state")

        now = _now_iso()
        step.updated_at = now
        if status == "SUCCEEDED":
            step.state = "SUCCEEDED"
            step.completed_at = now
        elif status == "FAILED" and failure_class == "RETRYABLE":
            step.state = "FAILED_RETRY"
        else:
            step.state = "FAILED_FINAL"
        step.last_error_code = error.get("code")
        step.last_error_message = error.get("message")
        steps_store.update_step(step, step.etag or "")

        if step.state == "SUCCEEDED" and step.step_index == len(steps) - 1:
            job.state = "SUCCEEDED"
            job.updated_at = now
            job.completed_at = now
            jobs_store.update_job(job, job.etag or "")
            inflight_store.release(job.tenant_id)
        elif step.state == "SUCCEEDED":
            next_step = steps[step.step_index + 1]
            lease_id = uuid4().hex
            next_step.attempt_no = 1
            next_step.lease_id = lease_id
            next_step.lease_expires_at = _lease_expires_at()
            next_step.state = "DISPATCHING"
            next_step.updated_at = now
            next_step = steps_store.update_step(next_step, next_step.etag or "")

            job.current_step_index = next_step.step_index
            job.current_step_id = next_step.step_id
            job.updated_at = now
            jobs_store.update_job(job, job.etag or "")

            outbox_partition = _outbox_partition_key(settings, job.job_id, {"lane": next_step.lane})
            outbox = OutboxEntry(
                outbox_id=uuid4().hex,
                tenant_id=job.tenant_id,
                tenant_bucket=outbox_partition,
                job_id=job.job_id,
                step_id=next_step.step_id,
                attempt_no=1,
                lease_id=lease_id,
                state="PENDING",
                topic=f"global-bus-p{next_step.lane}",
                partition=str(next_step.lane),
                payload={
                    "jobId": job.job_id,
                    "tenant_id": job.tenant_id,
                    "stepId": next_step.step_id,
                    "protocol_id": job.protocol_id,
                    "step_type": next_step.step_type,
                    "attempt_no": 1,
                    "lease_id": lease_id,
                    "input_ref": next_step.input_ref,
                    "workspace_ref": next_step.workspace_ref,
                    "output_ref": next_step.output_ref,
                    "payload": next_step.payload,
                    "callback_urls": {},
                    "mode": next_step.resolved_mode,
                    "lane": next_step.lane,
                    "routing_key_used": next_step.routing_key_used,
                },
                resolved_mode=next_step.resolved_mode,
                lane=next_step.lane,
                routing_key_used=next_step.routing_key_used,
                decision_source=next_step.decision_source,
                decision_reason=next_step.decision_reason,
                created_at=now,
                next_attempt_at=now,
                sent_at=None,
                updated_at=now,
            )
            outbox_entry = outbox_store.create_outbox(outbox)
            if publisher is None:
                _schedule_retry(outbox_entry)
            elif await _publish_entry(outbox_entry):
                _mark_publish_success(outbox_entry, job, next_step)
            else:
                _schedule_retry(outbox_entry)
        elif step.state == "FAILED_FINAL":
            job.state = "FAILED_FINAL"
            job.updated_at = now
            job.completed_at = now
            job.error_code = step.last_error_code
            job.error_message = step.last_error_message
            jobs_store.update_job(job, job.etag or "")
            inflight_store.release(job.tenant_id)

        log_event(
            logger,
            "callback.result.accepted",
            job_id=job_id,
            step_id=step_id,
            tenant_id=tenant_id,
            status=step.state,
        )
        return {"status": "ok"}

    @app.get("/v1/jobs/{job_id}")
    async def get_job(job_id: str, tenant_id: str, tenant_bucket: str | None = None):
        if tenant_bucket is None:
            index = job_index_store.get(job_id)
            if index is None or index.tenant_id != tenant_id:
                raise HTTPException(status_code=404, detail="job not found")
            tenant_bucket = index.tenant_bucket
        job = jobs_store.get_job(job_id, tenant_id, tenant_bucket)
        if job is None:
            raise HTTPException(status_code=404, detail="job not found")
        return job

    @app.get("/v1/jobs/{job_id}/steps")
    async def get_steps(job_id: str):
        return steps_store.get_steps(job_id)

    @app.post("/v1/jobs/{job_id}:cancel")
    async def cancel_job(job_id: str, tenant_id: str, tenant_bucket: str | None = None):
        if tenant_bucket is None:
            index = job_index_store.get(job_id)
            if index is None or index.tenant_id != tenant_id:
                raise HTTPException(status_code=404, detail="job not found")
            tenant_bucket = index.tenant_bucket
        job = jobs_store.get_job(job_id, tenant_id, tenant_bucket)
        if job is None:
            raise HTTPException(status_code=404, detail="job not found")
        if job.state in {"SUCCEEDED", "FAILED_FINAL", "CANCELLED"}:
            return job
        job.state = "CANCELLING"
        job.updated_at = _now_iso()
        return jobs_store.update_job(job, job.etag or "")

    if settings.admin_enabled:
        def _require_admin(key: str | None) -> None:
            if settings.admin_api_key and key != settings.admin_api_key:
                raise HTTPException(status_code=401, detail="unauthorized")

        @app.post("/v1/admin/outbox/retry")
        async def admin_outbox_retry(
            partition_key: str | None = None,
            limit: int = 100,
            x_admin_key: str | None = Header(default=None, alias="X-Admin-Key"),
        ):
            _require_admin(x_admin_key)
            if publisher is None:
                raise HTTPException(status_code=500, detail="missing service bus connection")

            partitions = [partition_key] if partition_key else list(_outbox_partitions(settings))
            await _process_pending_outbox(partitions, limit)
            return {"status": "ok"}

    return app
