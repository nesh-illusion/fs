from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from typing import Any, Dict
from uuid import uuid4
import zlib

from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse

from config.protocols import ProtocolRegistry
from config.settings import AppSettings
from ledger.memory_store import (
    MemoryJobIndexStore,
    MemoryIdempotencyStore,
    MemoryJobsStore,
    MemoryStepsStore,
    MemoryTenantInflightStore,
)
from ledger.models import Job, JobIdempotency, JobIndex, Step
from ledger.table_storage import (
    TableIdempotencyStore,
    TableJobIndexStore,
    TableJobsStore,
    TableLedgerJobsStore,
    TableLedgerStepsStore,
    TableStepsStore,
    TableTenantInflightStore,
)
from validation.idempotency import idempotency_hash
from validation.validator import SchemaValidator
from shared.logging import get_logger, log_event

from temporalio.client import Client as TemporalClient
from temporalio.exceptions import WorkflowAlreadyStartedError

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


def _build_stores(settings: AppSettings):
    if settings.storage_backend == "memory":
        return (
            MemoryJobsStore(),
            MemoryStepsStore(),
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
            TableIdempotencyStore(service_client, settings.idempotency_table),
            TableJobIndexStore(service_client, settings.job_index_table),
            TableTenantInflightStore(service_client, settings.inflight_table),
        )
    return (
        TableJobsStore(service_client, settings.jobs_table),
        TableStepsStore(service_client, settings.steps_table),
        TableIdempotencyStore(service_client, settings.idempotency_table),
        TableJobIndexStore(service_client, settings.job_index_table),
        TableTenantInflightStore(service_client, settings.inflight_table),
    )


def _find_step(steps: list[Step], step_id: str) -> Step | None:
    return next((step for step in steps if step.step_id == step_id), None)


def create_app() -> FastAPI:
    @asynccontextmanager
    async def lifespan(app: FastAPI):
        if settings.temporal_enabled:
            app.state.temporal_client = await TemporalClient.connect(
                settings.temporal_address,
                namespace=settings.temporal_namespace,
            )
        try:
            yield
        finally:
            temporal_client = getattr(app.state, "temporal_client", None)
            if temporal_client is not None:
                await temporal_client.close()

    app = FastAPI(title="LUCIUS Orchestrator", lifespan=lifespan)
    logger = get_logger("lucius_orchestrator")

    settings = AppSettings.from_env()
    validator = SchemaValidator()
    registry = ProtocolRegistry()
    jobs_store, steps_store, idempotency_store, job_index_store, inflight_store = _build_stores(settings)
    app.state.jobs_store = jobs_store
    app.state.steps_store = steps_store
    app.state.idempotency_store = idempotency_store
    app.state.job_index_store = job_index_store
    app.state.inflight_store = inflight_store
    app.state.temporal_client = None
    app.state.registry = registry
    app.state.validator = validator

    @app.post("/v1/orchestrate")
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
                final_output=None,
            )
            job = jobs_store.create_job(job)

            steps = []
            for index, step in enumerate(protocol.steps):
                if isinstance(payload, dict) and "steps" in payload:
                    step_payload = payload["steps"].get(step.step_id, {})
                else:
                    step_payload = payload
                steps.append(
                    Step(
                        job_id=job_id,
                        step_index=index,
                        step_id=step.step_id,
                        step_type=step.step_type,
                        service=step.service,
                        state="PENDING",
                        attempt_no=0,
                        lease_id="",
                        lease_expires_at="",
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

            workflow_input = {
                "job_id": job_id,
                "tenant_id": envelope["tenant_id"],
                "protocol_id": protocol.protocol_id,
                "steps": [
                    {
                        "step_id": step.step_id,
                        "step_type": step.step_type,
                        "service": step.service,
                        "payload": step.payload,
                        "input_ref": step.input_ref,
                        "workspace_ref": step.workspace_ref,
                        "output_ref": step.output_ref,
                    }
                    for step in steps
                ],
                "lane": routing["lane"],
                "routing_key_used": routing["routing_key_used"],
                "mode": routing["resolved_mode"],
                "correlation_id": envelope.get("correlation_id"),
                "traceparent": envelope.get("traceparent"),
            }
            if settings.temporal_enabled:
                temporal_client = app.state.temporal_client
                if temporal_client is None:
                    raise HTTPException(status_code=500, detail="temporal client not available")
                try:
                    await temporal_client.start_workflow(
                        "LuciusWorkflow",
                        workflow_input,
                        id=job_id,
                        task_queue=settings.temporal_task_queue,
                        execution_timeout=timedelta(
                            seconds=settings.temporal_workflow_execution_timeout_seconds
                        ),
                    )
                except WorkflowAlreadyStartedError:
                    pass
            else:
                log_event(logger, "temporal.disabled", job_id=job_id)

            return JSONResponse(status_code=202, content={"jobId": job_id})
        except Exception:
            inflight_store.release(tenant_id)
            raise

    @app.post("/v1/internal/steps/attempt")
    async def record_attempt(payload: Dict[str, Any]):
        job_id = payload.get("jobId")
        step_id = payload.get("stepId")
        tenant_id = payload.get("tenant_id")
        attempt_no = payload.get("attempt_no")
        lease_id = payload.get("lease_id")
        lease_expires_at = payload.get("lease_expires_at") or _lease_expires_at()
        if not all([job_id, step_id, tenant_id, attempt_no, lease_id]):
            raise HTTPException(status_code=400, detail="missing required fields")

        index = job_index_store.get(job_id)
        if index is None or index.tenant_id != tenant_id:
            raise HTTPException(status_code=404, detail="job not found")
        job = jobs_store.get_job(job_id, tenant_id, index.tenant_bucket)
        if job is None:
            raise HTTPException(status_code=404, detail="job not found")
        if job.state in {"SUCCEEDED", "FAILED_FINAL", "CANCELLED"}:
            raise HTTPException(status_code=409, detail="job is in terminal state")

        steps = steps_store.get_steps(job_id)
        step = _find_step(steps, step_id)
        if step is None:
            raise HTTPException(status_code=404, detail="step not found")
        if step.state in {"SUCCEEDED", "FAILED_FINAL"}:
            raise HTTPException(status_code=409, detail="step is in terminal state")
        if step.attempt_no == attempt_no and step.lease_id == lease_id:
            return {"status": "ok"}

        now = _now_iso()
        step.attempt_no = attempt_no
        step.lease_id = lease_id
        step.lease_expires_at = lease_expires_at
        step.state = "INITIATED"
        step.updated_at = now
        steps_store.update_step(step, step.etag or "")

        job.current_step_id = step.step_id
        job.current_step_index = step.step_index
        job.attempts_total = (job.attempts_total or 0) + 1
        job.updated_at = now
        jobs_store.update_job(job, job.etag or "")
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
        steps = steps_store.get_steps(job_id)
        return {"job": job, "steps": steps}

    @app.get("/v1/jobs/{job_id}/steps")
    async def get_steps(job_id: str, step_id: str | None = None):
        steps = steps_store.get_steps(job_id)
        if step_id is None:
            return steps
        step = _find_step(steps, step_id)
        if step is None:
            raise HTTPException(status_code=404, detail="step not found")
        return step

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

    @app.post("/v1/jobs/{job_id}:pause")
    async def pause_job(job_id: str, tenant_id: str, tenant_bucket: str | None = None):
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
        if job.state != "PAUSED":
            job.state = "PAUSED"
            job.updated_at = _now_iso()
            job = jobs_store.update_job(job, job.etag or "")
        if settings.temporal_enabled:
            temporal_client = app.state.temporal_client
            if temporal_client is None:
                raise HTTPException(status_code=500, detail="temporal client not available")
            await temporal_client.signal_workflow(job_id, "pause", {})
        return job

    @app.post("/v1/jobs/{job_id}:resume")
    async def resume_job(job_id: str, tenant_id: str, tenant_bucket: str | None = None):
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
        if job.state == "PAUSED":
            job.state = "IN_PROGRESS"
            job.updated_at = _now_iso()
            job = jobs_store.update_job(job, job.etag or "")
        if settings.temporal_enabled:
            temporal_client = app.state.temporal_client
            if temporal_client is None:
                raise HTTPException(status_code=500, detail="temporal client not available")
            await temporal_client.signal_workflow(job_id, "resume", {})
        return job

    return app
