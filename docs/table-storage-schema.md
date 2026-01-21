# LUCIUS Table Storage Schema (Phase 1)

## Goals
- Enable fast reads by jobId and tenant/time bucket.
- Support idempotency lookup by `(tenant_id, idempotency_hash)`.
- Keep partitions balanced while supporting hot tenants.

## Table: Jobs
PartitionKey: `tenant_id#YYYYMM`  
RowKey: `jobId`

Secondary index (for idempotency):
- Table: `JobIdempotency`
  - PartitionKey: `tenant_id`
  - RowKey: `idempotency_hash`
  - Fields: `jobId`, `created_at`
  - Default naming: `job-idempotency`

Secondary index (for job lookup):
- Table: `JobIndex`
  - PartitionKey: `jobId`
  - RowKey: `jobId`
  - Fields: `tenant_id`, `tenant_bucket`, `created_at`
  - Default naming: `job-index`

Common queries:
- `GET job by jobId`: scan within tenant bucket (or store `tenant_bucket` on request)
- `GET job by jobId (index)`: direct read from `JobIndex` to find `tenant_bucket`
- `GET latest jobs for tenant`: query by PartitionKey
- `idempotency lookup`: `JobIdempotency` table direct read

## Table: Steps
PartitionKey: `jobId`  
RowKey: `step_index#step_id`

Common queries:
- `GET steps by jobId`: partition scan
- `GET active step`: filter by state in partition

## Table: Outbox
PartitionKey: `tenant_id#YYYYMM`  
RowKey: `outbox_id`

Common queries:
- `GET pending outbox`: query PartitionKey with `state=PENDING`
- `GET by job/step`: filter by jobId + stepId

## Table: Events (optional)
PartitionKey: `jobId`  
RowKey: `timestamp#event_id`

Common queries:
- `GET audit trail`: partition scan by jobId

## Partitioning notes
- Default: use `tenant_id#YYYYMM` without buckets.
- If hot tenant or high throughput, add hash suffix to Jobs/Outbox PartitionKey:
  - `tenant_id#YYYYMM#bucket` where `bucket = CRC32(jobId) % 4`
- Keep Steps partitioned strictly by jobId to ensure atomic step scans.

When to enable buckets:
- Sustained write latency or throttling on Jobs/Outbox for a single tenant.
- A single tenant consistently dominates overall traffic volume.
