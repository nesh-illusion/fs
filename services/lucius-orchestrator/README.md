# Lucius Orchestrator

FastAPI service that validates command requests, creates jobs/steps, writes outbox directives, publishes to Service Bus, and exposes admin outbox retry endpoints.

## Components
- `api/app.py`: HTTP entrypoint, routes, orchestration wiring.
- `config/`: settings and protocol registry.
  - `settings.py`: env-driven configuration.
  - `protocols.py`: resolves request_type to protocol and steps.
- `ledger/`: job/step/outbox models and storage backends.
  - `models.py`: Job, Step, OutboxEntry.
  - `memory_store.py`: in-memory stores (dev).
  - `table_storage.py`: Azure Table Storage implementation.
- `validation/`: schema and idempotency validation.
  - `validator.py`: validates envelope and step payloads.
  - `idempotency.py`: idempotency hash logic.

## Flow
1) Receive `POST /v1/commands`.
2) Validate envelope and step payloads against JSON schema.
3) Resolve protocol and build Job + Steps.
4) Persist Job/Steps + Idempotency + JobIndex.
5) Write Outbox directive to the chosen topic/partition.
6) Return `202` with `jobId`.
7) Publish to Service Bus and mark outbox sent + step initiated + job in progress.
8) Admin endpoints can retry pending outbox entries when enabled.

## Configuration
Required/optional environment variables:
- `LUCIUS_STORAGE_BACKEND=memory|table|ledger`
- `LUCIUS_TABLE_CONNECTION` (required if `table`)
- `LUCIUS_JOBS_TABLE`, `LUCIUS_STEPS_TABLE`, `LUCIUS_OUTBOX_TABLE`
- `LUCIUS_LEDGER_TABLE` (required if `ledger`)
- `LUCIUS_IDEMPOTENCY_TABLE`, `LUCIUS_JOB_INDEX_TABLE`
- `LUCIUS_ADMIN_ENABLED=true|false`
- `LUCIUS_ADMIN_API_KEY`
- `LUCIUS_SERVICEBUS_CONNECTION` (optional, for publishing)
- `LUCIUS_OUTBOX_PUBLISH_TIMEOUT` (seconds, default 2.0)
- `LUCIUS_OUTBOX_RETRY_ENABLED=true|false`
- `LUCIUS_OUTBOX_RETRY_INTERVAL` (seconds, default 5.0)
- `LUCIUS_OUTBOX_RETRY_BATCH_SIZE`
- `LUCIUS_OUTBOX_RETRY_DELAY` (seconds, default 30.0)
- `LUCIUS_OUTBOX_MAX_ATTEMPTS` (default 3)

Memory mode uses in-process stores and does not persist state between restarts.

Ledger mode stores Job, Step, and Outbox entities in a single table:
- PartitionKey: `job_id`
- RowKey: `JOB` for jobs, `STEP#<index>#<step_id>` for steps, `OUTBOX#<outbox_id>` for outbox entries

## Local Run
Example (module path assumes `PYTHONPATH=services/lucius-orchestrator/src`):
```
uvicorn api.app:create_app --factory --host 0.0.0.0 --port 8000
```

## Tests
If pytest cannot find a writable temp directory, set `TMPDIR`:
```
TMPDIR=/var/folders/pv/bj3hzlgn76zdqhhgcnwbpk8m0000gn/T \
PYTHONDONTWRITEBYTECODE=1 \
PYTHONPATH=services/lucius-orchestrator/src:services/distributed-ocr:services/vector-ingestion:services/semantic-intelligence:services \
pytest -q -p no:cacheprovider services/lucius-orchestrator/tests/api \
  services/lucius-orchestrator/tests/validation services/lucius-orchestrator/tests/integration
```

## Docker
Build and run:
```
docker build -t lucius/lucius-orchestrator:local -f services/lucius-orchestrator/Dockerfile .
docker run --rm -p 8000:8000 -e LUCIUS_STORAGE_BACKEND=memory lucius/lucius-orchestrator:local
```
