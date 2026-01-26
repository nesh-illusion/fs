# Lucius Orchestrator

FastAPI service that validates command requests, creates jobs/steps, starts Temporal workflows, and exposes callback/read APIs backed by the ledger read-model.

## Components
- `api/app.py`: HTTP entrypoint, routes, orchestration wiring.
- `config/`: settings and protocol registry.
  - `settings.py`: env-driven configuration.
  - `protocols.py`: resolves request_type to protocol and steps.
- `ledger/`: job/step/outbox models and storage backends (outbox retained for legacy/backfill).
  - `models.py`: Job, Step, OutboxEntry.
- `temporal_worker/`: Temporal worker workflow + activities (run as separate process).
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
5) Start Temporal workflow with job + step inputs (workflow_id = jobId).
6) Return `202` with `jobId`.
7) Temporal worker records attempt/lease, publishes directive, waits for result signals.
8) Orchestrator updates ledger on ACK/RESULT callbacks and signals Temporal.

## Configuration
Required/optional environment variables:
- `LUCIUS_STORAGE_BACKEND=memory|table|ledger`
- `LUCIUS_TABLE_CONNECTION` (required if `table`)
- `LUCIUS_JOBS_TABLE`, `LUCIUS_STEPS_TABLE`
- `LUCIUS_LEDGER_TABLE` (required if `ledger`)
- `LUCIUS_IDEMPOTENCY_TABLE`, `LUCIUS_JOB_INDEX_TABLE`
- `LUCIUS_TEMPORAL_ENABLED=true|false`
- `LUCIUS_TEMPORAL_ADDRESS`
- `LUCIUS_TEMPORAL_NAMESPACE`
- `LUCIUS_TEMPORAL_TASK_QUEUE`
- `LUCIUS_TEMPORAL_WORKFLOW_EXECUTION_TIMEOUT` (seconds, default 3600)

Temporal worker process:
```
python -m temporal_worker.main
```

Memory mode uses in-process stores and does not persist state between restarts.

Ledger mode stores Job, Step, and Outbox entities in a single table:
- PartitionKey: `job_id`
- RowKey: `JOB` for jobs, `STEP#<index>#<step_id>` for steps

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
