from dataclasses import dataclass
from typing import Any, Dict, List, Optional


@dataclass
class Job:
    job_id: str
    tenant_id: str
    tenant_bucket: str
    request_type: str
    doc_id: str | None
    protocol_id: str
    mode: str
    state: str
    idempotency_key: Optional[str]
    idempotency_hash: str
    current_step_id: Optional[str]
    current_step_index: Optional[int]
    attempts_total: int
    created_at: str
    updated_at: str
    completed_at: Optional[str]
    error_code: Optional[str]
    error_message: Optional[str]
    correlation_id: Optional[str]
    traceparent: Optional[str]
    etag: Optional[str] = None


@dataclass
class Step:
    job_id: str
    step_index: int
    step_id: str
    step_type: str
    service: str
    state: str
    attempt_no: int
    lease_id: str
    lease_expires_at: str
    input_ref: Any
    workspace_ref: Any
    output_ref: Any
    payload: Dict[str, Any]
    resolved_mode: str
    lane: int
    routing_key_used: str
    decision_source: str
    decision_reason: str
    last_error_code: Optional[str]
    last_error_message: Optional[str]
    created_at: str
    updated_at: str
    completed_at: Optional[str]
    etag: Optional[str] = None


@dataclass
class OutboxEntry:
    outbox_id: str
    tenant_id: str
    tenant_bucket: str
    job_id: str
    step_id: str
    attempt_no: int
    lease_id: str
    state: str
    topic: str
    partition: str
    payload: Dict[str, Any]
    resolved_mode: str
    lane: int
    routing_key_used: str
    decision_source: str
    decision_reason: str
    created_at: str
    sent_at: Optional[str]
    updated_at: str
    etag: Optional[str] = None


@dataclass
class JobIdempotency:
    tenant_id: str
    idempotency_hash: str
    job_id: str
    created_at: str


@dataclass
class JobIndex:
    job_id: str
    tenant_id: str
    tenant_bucket: str
    created_at: str
