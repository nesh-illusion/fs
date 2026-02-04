import asyncio
import json
import logging
import os
from datetime import datetime, timezone
from typing import Any, Dict, Optional

from fastapi import FastAPI, HTTPException
from temporalio.client import Client as TemporalClient

from config.settings import AppSettings
from ledger.table_storage import TableLedgerJobsStore, TableLedgerStepsStore, TableTenantInflightStore
from ledger.models import Step


logger = logging.getLogger("lucius.invoker")
logging.basicConfig(level=logging.INFO)


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _find_step(steps: list[Step], step_id: str) -> Optional[Step]:
    for step in steps:
        if step.step_id == step_id:
            return step
    return None


def _normalize_final_output(output_ref: Any) -> Optional[str]:
    if output_ref is None:
        return None
    if isinstance(output_ref, str):
        return output_ref
    if isinstance(output_ref, dict):
        uri = output_ref.get("uri")
        if isinstance(uri, str):
            return uri
    return json.dumps(output_ref)


def _build_stores(settings: AppSettings):
    if not settings.table_connection_string:
        raise RuntimeError("LUCIUS_TABLE_CONNECTION not set")
    try:
        from azure.data.tables import TableServiceClient
    except ImportError as exc:  # pragma: no cover - optional dependency
        raise RuntimeError("azure-data-tables dependency is not installed") from exc

    service_client = TableServiceClient.from_connection_string(settings.table_connection_string)
    ledger_table = service_client.get_table_client(settings.ledger_table)
    jobs_store = TableLedgerJobsStore(ledger_table)
    steps_store = TableLedgerStepsStore(ledger_table)
    inflight_store = TableTenantInflightStore(service_client, settings.inflight_table)
    return jobs_store, steps_store, inflight_store


async def _publish_to_bus(connection: str, topic: str, payload: Dict[str, Any]) -> None:
    try:
        from azure.servicebus.aio import ServiceBusClient
        from azure.servicebus import ServiceBusMessage
    except ImportError as exc:  # pragma: no cover - optional dependency
        raise RuntimeError("azure-servicebus dependency is not installed") from exc

    message = ServiceBusMessage(json.dumps(payload))
    message.message_id = (
        f"{payload.get('jobId')}:{payload.get('stepId')}:{payload.get('attempt_no')}"
    )
    async with ServiceBusClient.from_connection_string(connection) as client:
        sender = client.get_topic_sender(topic_name=topic)
        async with sender:
            await sender.send_messages(message)


async def _handle_ack(app: FastAPI, payload: Dict[str, Any]) -> None:
    job_id = payload.get("jobId")
    step_id = payload.get("stepId")
    tenant_id = payload.get("tenant_id")
    attempt_no = payload.get("attempt_no")
    lease_id = payload.get("lease_id")
    if not all([job_id, step_id, tenant_id, attempt_no, lease_id]):
        logger.warning("ack.missing_fields", extra={"payload": payload})
        return

    jobs_store = app.state.jobs_store
    steps_store = app.state.steps_store

    job = jobs_store.get_job(job_id, tenant_id)
    if job is None or job.tenant_id != tenant_id:
        logger.warning("ack.job_not_found", extra={"job_id": job_id, "tenant_id": tenant_id})
        return

    steps = steps_store.get_steps(job_id)
    step = _find_step(steps, step_id)
    if step is None:
        logger.warning("ack.step_not_found", extra={"job_id": job_id, "step_id": step_id})
        return

    if step.state in {"SUCCEEDED", "FAILED_FINAL", "CANCELLED"}:
        logger.info("ack.terminal_step", extra={"job_id": job_id, "step_id": step_id})
        return
    if step.attempt_no != attempt_no or step.lease_id != lease_id:
        logger.info(
            "ack.stale_attempt",
            extra={
                "job_id": job_id,
                "step_id": step_id,
                "attempt_no": attempt_no,
                "lease_id": lease_id,
            },
        )
        return
    if step.state not in {"INITIATED", "PROCESSING", "IN_PROGRESS"}:
        logger.info(
            "ack.invalid_state",
            extra={"job_id": job_id, "step_id": step_id, "state": step.state},
        )
        return

    if step.state == "INITIATED":
        step.state = "PROCESSING"
        step.updated_at = _now_iso()
        steps_store.update_step(step, step.etag or "")

    if job.state in {"QUEUED"}:
        job.state = "IN_PROGRESS"
        job.updated_at = _now_iso()
        jobs_store.update_job(job, job.etag or "")

    temporal_client = app.state.temporal_client
    if temporal_client is not None:
        await temporal_client.signal_workflow(
            job_id,
            "step_ack",
            {
                "jobId": job_id,
                "stepId": step_id,
                "attempt_no": attempt_no,
                "lease_id": lease_id,
            },
        )


async def _handle_result(app: FastAPI, payload: Dict[str, Any]) -> None:
    job_id = payload.get("jobId")
    step_id = payload.get("stepId")
    tenant_id = payload.get("tenant_id")
    attempt_no = payload.get("attempt_no")
    lease_id = payload.get("lease_id")
    status = payload.get("status")
    error = payload.get("error") or {}
    output_ref = payload.get("output_ref")
    if not all([job_id, step_id, tenant_id, attempt_no, lease_id, status]):
        logger.warning("result.missing_fields", extra={"payload": payload})
        return

    jobs_store = app.state.jobs_store
    steps_store = app.state.steps_store
    inflight_store = app.state.inflight_store

    job = jobs_store.get_job(job_id, tenant_id)
    if job is None or job.tenant_id != tenant_id:
        logger.warning("result.job_not_found", extra={"job_id": job_id, "tenant_id": tenant_id})
        return

    steps = steps_store.get_steps(job_id)
    step = _find_step(steps, step_id)
    if step is None:
        logger.warning("result.step_not_found", extra={"job_id": job_id, "step_id": step_id})
        return

    if step.state in {"SUCCEEDED", "FAILED_FINAL", "CANCELLED"}:
        logger.info("result.terminal_step", extra={"job_id": job_id, "step_id": step_id})
        return
    if step.attempt_no != attempt_no or step.lease_id != lease_id:
        logger.info(
            "result.stale_attempt",
            extra={
                "job_id": job_id,
                "step_id": step_id,
                "attempt_no": attempt_no,
                "lease_id": lease_id,
            },
        )
        return
    if step.state not in {"INITIATED", "PROCESSING", "IN_PROGRESS"}:
        logger.info(
            "result.invalid_state",
            extra={"job_id": job_id, "step_id": step_id, "state": step.state},
        )
        return

    now = _now_iso()
    step.updated_at = now
    if status == "SUCCEEDED":
        step.state = "SUCCEEDED"
        step.completed_at = now
    elif status == "FAILED_RETRY":
        step.state = "FAILED_RETRY"
    else:
        step.state = "FAILED_FINAL"
        step.completed_at = now
    step.last_error_code = error.get("code")
    step.last_error_message = error.get("message")
    steps_store.update_step(step, step.etag or "")

    is_last_step = step.step_index == len(steps) - 1
    if step.state == "SUCCEEDED" and is_last_step:
        job.state = "SUCCEEDED"
        job.updated_at = now
        job.completed_at = now
        job.final_output = _normalize_final_output(output_ref)
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

    temporal_status = step.state
    temporal_client = app.state.temporal_client
    if temporal_client is not None:
        await temporal_client.signal_workflow(
            job_id,
            "step_result",
            {
                "jobId": job_id,
                "stepId": step_id,
                "status": temporal_status,
                "attempt_no": attempt_no,
                "lease_id": lease_id,
                "error": error,
            },
        )


async def _handle_reply(app: FastAPI, payload: Dict[str, Any]) -> None:
    msg_type = payload.get("type")
    if msg_type == "ACK":
        await _handle_ack(app, payload)
    elif msg_type == "RESULT":
        await _handle_result(app, payload)
    else:
        logger.warning("reply.unknown_type", extra={"payload": payload})


async def _consume_replies(app: FastAPI, lane: int) -> None:
    connection = app.state.servicebus_connection
    reply_prefix = app.state.reply_prefix
    subscription = app.state.reply_subscription
    topic = f"{reply_prefix}{lane}"

    try:
        from azure.servicebus.aio import ServiceBusClient
    except ImportError as exc:  # pragma: no cover - optional dependency
        raise RuntimeError("azure-servicebus dependency is not installed") from exc

    async with ServiceBusClient.from_connection_string(connection) as client:
        receiver = client.get_subscription_receiver(
            topic_name=topic,
            subscription_name=subscription,
        )
        async with receiver:
            while not app.state.shutdown_event.is_set():
                messages = await receiver.receive_messages(max_message_count=10, max_wait_time=5)
                for message in messages:
                    try:
                        body_bytes = b"".join(b for b in message.body)
                        payload = json.loads(body_bytes.decode("utf-8"))
                        await _handle_reply(app, payload)
                        await receiver.complete_message(message)
                    except Exception as exc:
                        logger.exception("reply.handle_failed", exc_info=exc)
                        await receiver.abandon_message(message)


def create_app() -> FastAPI:
    app = FastAPI()
    settings = AppSettings.from_env()

    jobs_store, steps_store, inflight_store = _build_stores(settings)
    app.state.jobs_store = jobs_store
    app.state.steps_store = steps_store
    app.state.inflight_store = inflight_store

    app.state.servicebus_connection = os.getenv("LUCIUS_SERVICEBUS_CONNECTION")
    app.state.reply_prefix = os.getenv("LUCIUS_SERVICEBUS_REPLY_PREFIX", "global-bus-replies-p")
    app.state.reply_subscription = os.getenv("LUCIUS_SERVICEBUS_REPLY_SUBSCRIPTION", "lucius-invoker")
    app.state.reply_lanes = int(os.getenv("LUCIUS_SERVICEBUS_LANES", "16"))
    app.state.shutdown_event = asyncio.Event()
    app.state.temporal_client = None
    app.state.reply_tasks = []

    @app.on_event("startup")
    async def _startup() -> None:
        if not app.state.servicebus_connection:
            raise RuntimeError("LUCIUS_SERVICEBUS_CONNECTION not set")
        app.state.temporal_client = await TemporalClient.connect(
            settings.temporal_address,
            namespace=settings.temporal_namespace,
        )
        for lane in range(app.state.reply_lanes):
            app.state.reply_tasks.append(asyncio.create_task(_consume_replies(app, lane)))

    @app.on_event("shutdown")
    async def _shutdown() -> None:
        app.state.shutdown_event.set()
        for task in app.state.reply_tasks:
            task.cancel()
        if app.state.temporal_client is not None:
            await app.state.temporal_client.close()

    @app.post("/v1/internal/steps/attempt")
    async def record_attempt(payload: Dict[str, Any]):
        job_id = payload.get("jobId")
        step_id = payload.get("stepId")
        tenant_id = payload.get("tenant_id")
        attempt_no = payload.get("attempt_no")
        lease_id = payload.get("lease_id")
        lease_expires_at = payload.get("lease_expires_at") or _now_iso()
        if not all([job_id, step_id, tenant_id, attempt_no, lease_id]):
            raise HTTPException(status_code=400, detail="missing required fields")

        job = jobs_store.get_job(job_id, tenant_id)
        if job is None or job.tenant_id != tenant_id:
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

    @app.post("/v1/internal/publish")
    async def publish(payload: Dict[str, Any]):
        lane = payload.get("lane")
        directive = payload.get("directive")
        if lane is None or directive is None:
            raise HTTPException(status_code=400, detail="missing required fields")
        topic = f"global-bus-p{lane}"
        await _publish_to_bus(app.state.servicebus_connection, topic, directive)
        return {"status": "ok"}

    return app


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("invoker.app:create_app", host="0.0.0.0", port=8001, factory=True)
