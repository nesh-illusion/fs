from datetime import datetime, timezone

import requests

from .directive_model import Directive


def send_ack(directive: Directive) -> None:
    payload = {
        "jobId": directive.jobId,
        "stepId": directive.stepId,
        "tenant_id": directive.tenant_id,
        "attempt_no": directive.attempt_no,
        "lease_id": directive.lease_id,
        "status": "ACKED",
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }
    url = directive.callback_urls.get("ack")
    if not url:
        return
    requests.post(url, json=payload, timeout=5)
