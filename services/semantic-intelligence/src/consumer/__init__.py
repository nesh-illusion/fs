import json
from typing import Any, Dict

from clients.ack_client import send_ack
from models.directive_model import Directive, parse_directive
from models.error_model import ErrorDetail, FailureClass
from idempotency.store import build_idempotency_store
from clients.result_client import send_result
from shared.logging import get_logger, log_event


LOGGER = get_logger("sis_service")


def execute(directive: Directive) -> Dict[str, Any]:
    return {"status": "ok"}


def handle_message(payload: Dict[str, Any]) -> None:
    directive = parse_directive(payload)
    log_event(
        LOGGER,
        "directive.received",
        job_id=directive.jobId,
        step_id=directive.stepId,
        tenant_id=directive.tenant_id,
        attempt_no=directive.attempt_no,
        lease_id=directive.lease_id,
    )
    idempotency = build_idempotency_store()
    if idempotency.seen(directive):
        log_event(
            LOGGER,
            "directive.duplicate",
            job_id=directive.jobId,
            step_id=directive.stepId,
            tenant_id=directive.tenant_id,
            attempt_no=directive.attempt_no,
        )
        return
    idempotency.mark_seen(directive)

    log_event(
        LOGGER,
        "directive.ack",
        job_id=directive.jobId,
        step_id=directive.stepId,
        tenant_id=directive.tenant_id,
    )
    send_ack(directive)

    try:
        result = execute(directive)
        log_event(
            LOGGER,
            "directive.result",
            job_id=directive.jobId,
            step_id=directive.stepId,
            tenant_id=directive.tenant_id,
            status="SUCCEEDED",
        )
        send_result(directive, status="SUCCEEDED", output_ref=result)
    except Exception as exc:
        error = ErrorDetail(code="EXECUTION_ERROR", message=str(exc))
        log_event(
            LOGGER,
            "directive.result",
            job_id=directive.jobId,
            step_id=directive.stepId,
            tenant_id=directive.tenant_id,
            status="FAILED",
            error=str(exc),
        )
        send_result(
            directive,
            status="FAILED",
            failure_class=FailureClass.RETRYABLE,
            error=error,
        )
