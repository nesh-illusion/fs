from dataclasses import dataclass
from typing import Any, Dict


@dataclass
class Directive:
    jobId: str
    tenant_id: str
    stepId: str
    protocol_id: str
    step_type: str
    attempt_no: int
    lease_id: str
    input_ref: Any
    workspace_ref: Any
    output_ref: Any
    payload: Dict[str, Any]
    callback_urls: Dict[str, str]


def parse_directive(payload: Dict[str, Any]) -> Directive:
    return Directive(
        jobId=payload["jobId"],
        tenant_id=payload["tenant_id"],
        stepId=payload["stepId"],
        protocol_id=payload["protocol_id"],
        step_type=payload["step_type"],
        attempt_no=payload["attempt_no"],
        lease_id=payload["lease_id"],
        input_ref=payload["input_ref"],
        workspace_ref=payload.get("workspace_ref"),
        output_ref=payload["output_ref"],
        payload=payload.get("payload", {}),
        callback_urls=payload.get("callback_urls", {}),
    )
