from datetime import datetime, timedelta, timezone
from typing import Any, Dict
from uuid import uuid4
import zlib

from fastapi import FastAPI, Header, HTTPException
from fastapi.responses import JSONResponse

from lucius_orchestrator.config.protocols import ProtocolRegistry
from lucius_orchestrator.config.settings import AppSettings
from lucius_orchestrator.ledger.memory_store import (
    MemoryJobIndexStore,
    MemoryIdempotencyStore,
    MemoryJobsStore,
    MemoryOutboxStore,
    MemoryStepsStore,
    MemoryTenantInflightStore,
)
from lucius_orchestrator.ledger.models import Job, JobIdempotency, JobIndex, OutboxEntry, Step
from lucius_orchestrator.ledger.table_storage import (
    TableIdempotencyStore,
    TableJobIndexStore,
    TableJobsStore,
    TableOutboxStore,
    TableStepsStore,
    TableTenantInflightStore,
)
from lucius_orchestrator.validation.idempotency import idempotency_hash
from lucius_orchestrator.validation.validator import SchemaValidator
from lucius_dispatcher.app.service import ReconciliationConfig, ReconciliationService
from lucius_dispatcher.app.stores import build_stores as build_recon_stores
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

    if settings.storage_backend != "table":
        raise RuntimeError(f"Unsupported storage backend: {settings.storage_backend}")

    if not settings.table_connection_string:
        raise RuntimeError("LUCIUS_TABLE_CONNECTION is required for table storage")

    from azure.data.tables import TableServiceClient

    service_client = TableServiceClient.from_connection_string(settings.table_connection_string)
    return (
        TableJobsStore(service_client, settings.jobs_table),
        TableStepsStore(service_client, settings.steps_table),
        TableOutboxStore(service_client, settings.outbox_table),
        TableIdempotencyStore(service_client, settings.idempotency_table),
        TableJobIndexStore(service_client, settings.job_index_table),
        TableTenantInflightStore(service_client, settings.inflight_table),
    )


def _build_lucius_dispatcher(settings: AppSettings):
    jobs_store, steps_store, outbox_store, _, _, _ = build_recon_stores(settings)
    return ReconciliationService(jobs_store, steps_store, outbox_store, ReconciliationConfig())


def _find_step(steps: list[Step], step_id: str) -> Step | None:
    return next((step for step in steps if step.step_id == step_id), None)


def create_app() -> FastAPI:
    app = FastAPI(title="LUCIUS Orchestrator")
    logger = get_logger("lucius_orchestrator")

    settings = AppSettings.from_env()
    validator = SchemaValidator()
    registry = ProtocolRegistry()
    jobs_store, steps_store, outbox_store, idempotency_store, job_index_store, inflight_store = _build_stores(settings)
    reconciliation = _build_lucius_dispatcher(settings)
    app.state.jobs_store = jobs_store
    app.state.steps_store = steps_store
    app.state.outbox_store = outbox_store
    app.state.idempotency_store = idempotency_store
    app.state.job_index_store = job_index_store
    app.state.inflight_store = inflight_store
    app.state.registry = registry
    app.state.validator = validator

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

            outbox = OutboxEntry(
                outbox_id=uuid4().hex,
                tenant_id=envelope["tenant_id"],
                tenant_bucket=tenant_bucket,
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
                sent_at=None,
                updated_at=created_at,
            )
            outbox_store.create_outbox(outbox)
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
        if step.state != "AWAITING_ACK":
            raise HTTPException(status_code=409, detail="invalid step state")

        step.state = "IN_PROGRESS"
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
        if step.state != "IN_PROGRESS":
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

        @app.post("/v1/admin/reconcile/dispatch")
        async def admin_dispatch(
            partition_key: str,
            limit: int = 100,
            x_admin_key: str | None = Header(default=None, alias="X-Admin-Key"),
        ):
            _require_admin(x_admin_key)
            reconciliation._config.outbox_batch_size = limit
            if settings.admin_publish_enabled:
                from lucius_dispatcher.app.publisher import ServiceBusPublisher
                if not settings.service_bus_connection:
                    raise HTTPException(status_code=500, detail="missing service bus connection")
                publisher = ServiceBusPublisher(settings.service_bus_connection)
            else:
                from lucius_dispatcher.app.publisher import NoopPublisher
                publisher = NoopPublisher()
            reconciliation.dispatch_partition(partition_key, publisher)
            return {"status": "ok"}

        @app.post("/v1/admin/reconcile/sweep")
        async def admin_sweep(
            job_id: str,
            tenant_id: str,
            tenant_bucket: str,
            x_admin_key: str | None = Header(default=None, alias="X-Admin-Key"),
        ):
            _require_admin(x_admin_key)
            reconciliation.sweep_job(job_id, tenant_id, tenant_bucket)
            return {"status": "ok"}

    return app
