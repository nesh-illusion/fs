# Phase 2 Implementation Plan (API + Orchestrator)

## Scope
- Persistence interfaces for Jobs/Steps/Outbox (+ JobIdempotency).
- Schema validator for envelope + step payload.
- API endpoints: create, query, cancel.

## Persistence interfaces
Define a minimal storage abstraction layer (Table Storage today; mockable for local).

### Interfaces
- `JobsStore`:
  - `create_job(job) -> jobId` (ETag on create)
  - `get_job(jobId) -> job`
  - `update_job(job, etag) -> job`
  - `find_by_idempotency(tenant_id, hash) -> jobId | None`
- `StepsStore`:
  - `create_steps(jobId, steps[])`
  - `get_steps(jobId) -> list`
  - `get_active_step(jobId) -> step`
  - `update_step(step, etag) -> step`
- `OutboxStore`:
  - `create_outbox(entry)`
  - `list_pending(partition_key, limit)`
  - `mark_sent(outbox_id, etag)`
- `IdempotencyStore`:
  - `put(tenant_id, hash, jobId)`
  - `get(tenant_id, hash) -> jobId | None`

### ETag usage
- All updates must pass ETag from last read.
- Reject updates to terminal states.

## Validator wiring
1) Load JSON schemas from `docs/contracts` and `docs/schemas/steps`.
2) Validate request envelope against `command-envelope.v1.schema.json`.
3) Resolve `request_type -> protocol_id` from App Config (or local registry).
4) Validate each step payload against its `payload_schema_ref`.

## API endpoints
- `POST /v1/commands`:
  - Validate envelope and payload.
  - Idempotency check -> return existing `jobId` if present.
  - Create job row + step rows.
  - Create outbox for step 0.
  - Return `202 { jobId }`.
- `GET /v1/jobs/{jobId}`:
  - Return job state.
- `GET /v1/jobs/{jobId}/steps`:
  - Return step list.
- `POST /v1/jobs/{jobId}:cancel`:
  - Set job -> `CANCELLING`.
  - Do not start new steps after cancel.

## Next deliverables
- Implement storage adapters (Table + mock).
- Add unit tests for validator and idempotency.
