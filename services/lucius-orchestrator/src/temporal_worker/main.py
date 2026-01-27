import asyncio
import os
from dataclasses import dataclass
from datetime import timedelta
from typing import Any, Dict, Optional

import httpx
from temporalio import activity, workflow
from temporalio.client import Client
from temporalio.worker import Worker


INVOKER_URL = os.getenv("LUCIUS_INVOKER_URL", "http://lucius-invoker:8001").rstrip("/")
TEMPORAL_ADDRESS = os.getenv("LUCIUS_TEMPORAL_ADDRESS", "localhost:7233")
TEMPORAL_NAMESPACE = os.getenv("LUCIUS_TEMPORAL_NAMESPACE", "default")
TEMPORAL_TASK_QUEUE = os.getenv("LUCIUS_TEMPORAL_TASK_QUEUE", "lucius")

MAX_ATTEMPTS = int(os.getenv("LUCIUS_MAX_ATTEMPTS", "3"))
LEASE_MINUTES = int(os.getenv("LUCIUS_LEASE_MINUTES", "15"))
STEP_RESULT_TIMEOUT_SECONDS = int(os.getenv("LUCIUS_STEP_RESULT_TIMEOUT_SECONDS", "900"))


@dataclass
class StepResult:
    status: str
    step_id: str
    attempt_no: int
    lease_id: str
    error: Optional[Dict[str, Any]] = None


@activity.defn
async def record_attempt_lease(payload: Dict[str, Any]) -> None:
    url = f"{INVOKER_URL}/v1/internal/steps/attempt"
    async with httpx.AsyncClient(timeout=10.0) as client:
        response = await client.post(url, json=payload)
        if response.status_code >= 300:
            raise RuntimeError(f"record_attempt_lease failed: {response.status_code} {response.text}")


@activity.defn
async def publish_to_bus(payload: Dict[str, Any]) -> None:
    url = f"{INVOKER_URL}/v1/internal/publish"
    async with httpx.AsyncClient(timeout=10.0) as client:
        response = await client.post(url, json=payload)
        if response.status_code >= 300:
            raise RuntimeError(f"publish_to_bus failed: {response.status_code} {response.text}")


@workflow.defn
class LuciusWorkflow:
    def __init__(self) -> None:
        self._current_step_id: Optional[str] = None
        self._current_attempt_no: Optional[int] = None
        self._current_lease_id: Optional[str] = None
        self._pending_result: Optional[StepResult] = None
        self._paused: bool = False

    @workflow.run
    async def run(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        job_id = payload["job_id"]
        tenant_id = payload["tenant_id"]
        protocol_id = payload["protocol_id"]
        steps = payload["steps"]
        lane = payload["lane"]
        routing_key_used = payload["routing_key_used"]
        mode = payload["mode"]
        correlation_id = payload.get("correlation_id")
        traceparent = payload.get("traceparent")

        for step in steps:
            attempt_no = 0
            while True:
                if self._paused:
                    await workflow.wait_condition(lambda: not self._paused)
                attempt_no += 1
                lease_id = workflow.uuid4().hex
                lease_expires_at = (workflow.now() + timedelta(minutes=LEASE_MINUTES)).isoformat()

                await workflow.execute_activity(
                    record_attempt_lease,
                    {
                        "jobId": job_id,
                        "stepId": step["step_id"],
                        "tenant_id": tenant_id,
                        "attempt_no": attempt_no,
                        "lease_id": lease_id,
                        "lease_expires_at": lease_expires_at,
                    },
                    start_to_close_timeout=timedelta(seconds=15),
                )

                directive = {
                    "jobId": job_id,
                    "tenant_id": tenant_id,
                    "stepId": step["step_id"],
                    "protocol_id": protocol_id,
                    "step_type": step["step_type"],
                    "attempt_no": attempt_no,
                    "lease_id": lease_id,
                    "input_ref": step["input_ref"],
                    "workspace_ref": step["workspace_ref"],
                    "output_ref": step["output_ref"],
                    "payload": step["payload"],
                    "mode": mode,
                    "lane": lane,
                    "routing_key_used": routing_key_used,
                    "correlation_id": correlation_id,
                    "traceparent": traceparent,
                }

                await workflow.execute_activity(
                    publish_to_bus,
                    {"lane": lane, "directive": directive},
                    start_to_close_timeout=timedelta(seconds=30),
                )

                self._current_step_id = step["step_id"]
                self._current_attempt_no = attempt_no
                self._current_lease_id = lease_id
                self._pending_result = None

                try:
                    await workflow.wait_condition(
                        lambda: self._pending_result is not None,
                        timeout=timedelta(seconds=STEP_RESULT_TIMEOUT_SECONDS),
                    )
                except TimeoutError as exc:
                    if attempt_no >= MAX_ATTEMPTS:
                        raise RuntimeError("step timeout exceeded") from exc
                    continue

                result = self._pending_result
                if result is None:
                    continue
                if result.status == "SUCCEEDED":
                    break
                if result.status == "FAILED_RETRY" and attempt_no < MAX_ATTEMPTS:
                    continue
                raise RuntimeError(f"step failed: {result.status}")

        return {"status": "SUCCEEDED"}

    @workflow.signal
    def step_ack(self, payload: Dict[str, Any]) -> None:
        return None

    @workflow.signal
    def step_result(self, payload: Dict[str, Any]) -> None:
        if (
            payload.get("stepId") != self._current_step_id
            or payload.get("attempt_no") != self._current_attempt_no
            or payload.get("lease_id") != self._current_lease_id
        ):
            return
        self._pending_result = StepResult(
            status=payload.get("status", ""),
            step_id=payload.get("stepId", ""),
            attempt_no=payload.get("attempt_no", 0),
            lease_id=payload.get("lease_id", ""),
            error=payload.get("error"),
        )

    @workflow.signal
    def pause(self, payload: Dict[str, Any]) -> None:
        self._paused = True

    @workflow.signal
    def resume(self, payload: Dict[str, Any]) -> None:
        self._paused = False


async def _run_worker() -> None:
    client = await Client.connect(TEMPORAL_ADDRESS, namespace=TEMPORAL_NAMESPACE)
    worker = Worker(
        client,
        task_queue=TEMPORAL_TASK_QUEUE,
        workflows=[LuciusWorkflow],
        activities=[record_attempt_lease, publish_to_bus],
    )
    await worker.run()


def main() -> None:
    asyncio.run(_run_worker())


if __name__ == "__main__":
    main()
