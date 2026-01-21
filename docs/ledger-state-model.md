# LUCIUS Ledger + State Model (Phase 1)

## Data model (Azure Table Storage)
All tables use optimistic concurrency with ETags. Terminal states are immutable.

### Jobs table
- PartitionKey: `tenant_id#YYYYMM` (time bucket for throughput)
- RowKey: `jobId`
- Fields:
  - `tenant_id`, `request_type`, `protocol_id`, `mode`
  - `doc_id` (optional)
  - `state` (job state enum)
  - `idempotency_key`, `idempotency_hash`
  - `current_step_id`, `current_step_index`
  - `attempts_total` (all steps)
  - `created_at`, `updated_at`, `completed_at`
  - `error_code`, `error_message` (final failure)
  - `correlation_id`, `traceparent`

### Steps table
- PartitionKey: `jobId`
- RowKey: `step_index#step_id`
- Fields:
  - `step_id`, `step_type`, `service`
  - `state` (step state enum)
  - `attempt_no`, `lease_id`, `lease_expires_at`
  - `input_ref`, `workspace_ref`, `output_ref`
  - `payload` (opaque JSON)
  - `resolved_mode`, `lane`, `routing_key_used`
  - `decision_source`, `decision_reason`
  - `last_error_code`, `last_error_message`
  - `created_at`, `updated_at`, `completed_at`

### Outbox table
- PartitionKey: `tenant_id#YYYYMM`
- RowKey: `outbox_id`
- Fields:
  - `jobId`, `stepId`, `attempt_no`, `lease_id`
  - `state` (`PENDING`, `SENT`, `FAILED_FINAL`)
  - `topic`, `partition`
  - `payload` (directive JSON)
  - `resolved_mode`, `lane`, `routing_key_used`
  - `decision_source`, `decision_reason`
  - `created_at`, `sent_at`, `updated_at`

### Events table (optional)
- PartitionKey: `jobId`
- RowKey: `timestamp#event_id`
- Fields:
  - `event_type`, `payload`, `created_at`

## State transition rules

### Concurrency (ETag) rules
- All state transitions must include the current entity ETag.
- Jobs: transition only if `current_step_id` matches the intended step.
- Steps: transition only if `attempt_no` and `lease_id` match the active attempt.
- Outbox: mark `SENT` only if `state=PENDING` with current ETag.

### Job states
`QUEUED` → `DISPATCHING` → `IN_PROGRESS` → `SUCCEEDED | FAILED_FINAL | CANCELLED`
`CANCELLING` can be entered from any non-terminal state.

Rules:
- Only one step may be `IN_PROGRESS` per job.
- Job terminal states are immutable.
- Job transitions require the current Jobs ETag and a matching active step.

#### Job transition table
| From state | To state | Trigger | Preconditions |
| --- | --- | --- | --- |
| `QUEUED` | `DISPATCHING` | First step outbox created | Job ETag; `current_step_id` matches step 0 |
| `DISPATCHING` | `IN_PROGRESS` | Step 0 ACK received | Job ETag; step 0 attempt/lease match |
| `IN_PROGRESS` | `IN_PROGRESS` | Step N succeeded, step N+1 activated | Job ETag; step N terminal; step N+1 created |
| `IN_PROGRESS` | `SUCCEEDED` | Final step succeeded | Job ETag; final step terminal |
| `IN_PROGRESS` | `FAILED_FINAL` | Step failed final | Job ETag; step terminal failed |
| `QUEUED`/`DISPATCHING`/`IN_PROGRESS` | `CANCELLING` | Cancel request | Job ETag; not terminal |
| `CANCELLING` | `CANCELLED` | Active step terminal | Job ETag; active step terminal |

### Step states
`PENDING` → `DISPATCHING` → `AWAITING_ACK` → `IN_PROGRESS` → `SUCCEEDED | FAILED_FINAL | CANCELLED`
`FAILED_RETRY` is transient and must transition to `DISPATCHING` with a new `attempt_no` and `lease_id`.

Rules:
- Step terminal states are immutable.
- A callback is accepted only if `(attempt_no, lease_id)` match the current step row.
- Only the active step (lowest index not terminal) can be moved to `IN_PROGRESS`.

#### Step transition table
| From state | To state | Trigger | Preconditions |
| --- | --- | --- | --- |
| `PENDING` | `DISPATCHING` | Outbox row created | Step ETag; set `attempt_no`, `lease_id`, `lease_expires_at` |
| `DISPATCHING` | `AWAITING_ACK` | Outbox `SENT` | Step ETag; attempt/lease match |
| `AWAITING_ACK` | `IN_PROGRESS` | ACK callback | Step ETag; attempt/lease match |
| `IN_PROGRESS` | `SUCCEEDED` | RESULT success | Step ETag; attempt/lease match |
| `IN_PROGRESS` | `FAILED_FINAL` | RESULT non-retryable | Step ETag; attempt/lease match |
| `IN_PROGRESS` | `FAILED_RETRY` | RESULT retryable | Step ETag; attempt/lease match; attempts < max |
| `AWAITING_ACK` | `FAILED_RETRY` | ACK timeout | Step ETag; attempts < max |
| `FAILED_RETRY` | `DISPATCHING` | Retry dispatch | Step ETag; increment `attempt_no` and new `lease_id` |
| Any non-terminal | `CANCELLED` | Cancel + active step terminal | Step ETag; job is `CANCELLING` |

### Outbox states
`PENDING` → `SENT` → `FAILED_FINAL`

Rules:
- Outbox rows are immutable after `SENT` or `FAILED_FINAL`.
- `SENT` must only be written after successful publish.

#### Outbox transition table
| From state | To state | Trigger | Preconditions |
| --- | --- | --- | --- |
| `PENDING` | `SENT` | Publish succeeded | Outbox ETag; directive published |
| `PENDING` | `FAILED_FINAL` | Publish failed final | Outbox ETag; attempts >= max |

## Idempotency strategy
- Accept `idempotency_key` if provided; otherwise derive a hash:
  - `hash = SHA256(canonical_json)`
- Store both `idempotency_key` and `idempotency_hash` on the Job.
- On duplicate, return existing `jobId` and do not create new steps/outbox rows.

### Canonical JSON for idempotency
- Fields included: `tenant_id`, `request_type`, `input_ref`, `output_ref`, `payload`, `schema_version`.
- Canonicalization rules:
  - Sort object keys lexicographically.
  - Preserve array order.
  - Serialize numbers without trailing zeros.
  - Use UTF-8 without BOM.
  - Omit fields with null values.

## Timeouts and retries
- ACK timeout: 30s → move to `FAILED_RETRY` and re-dispatch (max 3 attempts).
- IN_PROGRESS lease: 15m → move to `FAILED_RETRY` unless non-retryable failure.
- Dispatch retry backoff: 30s, 2m, 10m (max 3).
- ACK retry backoff: 1m, 5m, 15m (max 3).
