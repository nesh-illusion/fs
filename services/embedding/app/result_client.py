from datetime import datetime, timezone

import requests

from .directive_model import Directive
from .error_model import ErrorDetail


def send_result(
    directive: Directive,
    status: str,
    failure_class: str | None = None,
    error: ErrorDetail | None = None,
    output_ref: dict | str | None = None,
) -> None:
    payload = {
        "jobId": directive.jobId,
        "stepId": directive.stepId,
        "tenant_id": directive.tenant_id,
        "attempt_no": directive.attempt_no,
        "lease_id": directive.lease_id,
        "status": status,
        "failure_class": failure_class,
        "error": None if error is None else {"code": error.code, "message": error.message},
        "output_ref": output_ref,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }
    url = directive.callback_urls.get("result")
    if not url:
        return
    requests.post(url, json=payload, timeout=10)
