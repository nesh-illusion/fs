import json
from typing import Any, Dict

from .ack_client import send_ack
from .directive_model import Directive, parse_directive
from .error_model import ErrorDetail, FailureClass
from .idempotency_store import build_idempotency_store
from .result_client import send_result


def execute(directive: Directive) -> Dict[str, Any]:
    return {"status": "ok"}


def handle_message(payload: Dict[str, Any]) -> None:
    directive = parse_directive(payload)
    idempotency = build_idempotency_store()
    if idempotency.seen(directive):
        return
    idempotency.mark_seen(directive)

    send_ack(directive)

    try:
        result = execute(directive)
        send_result(directive, status="SUCCEEDED", output_ref=result)
    except Exception as exc:
        error = ErrorDetail(code="EXECUTION_ERROR", message=str(exc))
        send_result(
            directive,
            status="FAILED",
            failure_class=FailureClass.RETRYABLE,
            error=error,
        )
