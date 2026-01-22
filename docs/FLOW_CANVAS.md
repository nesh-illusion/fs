# Lucius Orchestrator Flow Canvas

## Entry: POST /v1/commands
1) Validate envelope + payloads (schema + protocol step schemas).
2) Resolve protocol by `request_type`.
3) Idempotency hash + lookup; return existing `jobId` if found.
4) Rate-limit per tenant (`inflight`).
5) Compute routing decision:
   - Normalize tenant/doc_id, choose lane with CRC32 mod 16.
6) Create Job + Steps + Outbox entry.
7) Publish to Service Bus (async).
8) On publish success (publish → update flow):
   - Outbox → `SENT`
   - Step → `INITIATED` (step row updated in DB)
   - Job → `IN_PROGRESS`
9) On publish failure:
   - Outbox remains `PENDING`
   - Retry loop handles later.

## Callbacks
### POST /v1/callbacks/ack
- Validates `jobId`, `stepId`, `attempt_no`, `lease_id`.
- Moves Step → `PROCESSING` if currently `INITIATED`.
- Ensures Job → `IN_PROGRESS` if still `QUEUED`/`DISPATCHING`.

### POST /v1/callbacks/result
- Validates `jobId`, `stepId`, `attempt_no`, `lease_id`.
- Updates Step state:
  - `SUCCEEDED`
  - `FAILED_RETRY` (retryable)
  - `FAILED_FINAL` (terminal)
- If last step succeeded → Job `SUCCEEDED`.
- If step failed final → Job `FAILED_FINAL`.
- If step succeeded and not last:
  - Orchestrator creates next step outbox.
  - Orchestrator publishes next step (publish → update flow).
  - Next step → `INITIATED` (step row updated in DB), Job stays `IN_PROGRESS`.
  - If next step publish fails, outbox stays `PENDING` and retry loop handles it.

## Retry Loop (Outbox)
- Runs every `LUCIUS_OUTBOX_RETRY_INTERVAL`.
- Only picks `PENDING` entries where `now >= next_attempt_at`.
- Increments `attempt_no`, refreshes `lease_id`, updates step + outbox in DB (update → publish → update flow).
- Publishes, then marks `SENT`.
- If publish fails → `PENDING`, set `next_attempt_at = now + retry_delay`.
- If `attempt_no >= max_attempts`:
  - Step → `FAILED_FINAL`
  - Job → `FAILED_FINAL`
  - Outbox → `FAILED_FINAL`
  - Next steps are not initiated; job is terminal.

## Who advances steps?
- Platform services execute the current step and call `ACK` and `RESULT`.
- The orchestrator advances to the next step on `RESULT` success.

## Partition + Routing
- Lane = CRC32(normalized routing_key) % 16
- `routing_key_used`:
  - `tenant_id` for DEFAULT
  - `tenant_id + doc_id` for BURST
- Lane stored on each Step + Outbox payload.

## Storage Backends
- `memory`: in-process, ephemeral.
- `table`: separate tables for job/steps/outbox/idempotency/index/inflight.
- `ledger`: single table for job+steps+outbox, PartitionKey=`job_id`.

## Key API Endpoints
- `POST /v1/commands`
- `POST /v1/callbacks/ack`
- `POST /v1/callbacks/result`
- `GET /v1/jobs/{job_id}`
- `GET /v1/jobs/{job_id}/steps`
- `POST /v1/jobs/{job_id}:cancel`
- `POST /v1/admin/outbox/retry`
