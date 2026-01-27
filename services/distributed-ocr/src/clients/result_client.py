import json
import os
from datetime import datetime, timezone

from azure.servicebus import ServiceBusClient, ServiceBusMessage

from models.directive_model import Directive
from models.error_model import ErrorDetail


def send_result(
    directive: Directive,
    status: str,
    failure_class: str | None = None,
    error: ErrorDetail | None = None,
    output_ref: dict | str | None = None,
) -> None:
    payload = {
        "type": "RESULT",
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
    connection = os.getenv("SERVICEBUS_CONNECTION")
    if not connection:
        raise RuntimeError("SERVICEBUS_CONNECTION not set")
    reply_prefix = os.getenv("SERVICEBUS_REPLY_PREFIX", "global-bus-replies-p")
    topic = f"{reply_prefix}{directive.lane}"
    message = ServiceBusMessage(json.dumps(payload))
    message.message_id = (
        f"{directive.jobId}:{directive.stepId}:{directive.attempt_no}:{directive.lease_id}:RESULT"
    )
    with ServiceBusClient.from_connection_string(connection) as client:
        sender = client.get_topic_sender(topic_name=topic)
        with sender:
            sender.send_messages(message)
