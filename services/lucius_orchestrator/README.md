# Lucius Orchestrator

FastAPI service that validates command requests, creates jobs/steps, writes outbox directives, and exposes admin reconciliation endpoints.

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
7) Admin endpoints can dispatch/sweep reconciliation when enabled.

## Configuration
Required/optional environment variables:
- `LUCIUS_STORAGE_BACKEND=memory|table`
- `LUCIUS_TABLE_CONNECTION` (required if `table`)
- `LUCIUS_JOBS_TABLE`, `LUCIUS_STEPS_TABLE`, `LUCIUS_OUTBOX_TABLE`
- `LUCIUS_IDEMPOTENCY_TABLE`, `LUCIUS_JOB_INDEX_TABLE`
- `LUCIUS_ADMIN_ENABLED=true|false`
- `LUCIUS_ADMIN_API_KEY`
- `LUCIUS_ADMIN_PUBLISH=true|false`
- `LUCIUS_SERVICEBUS_CONNECTION` (optional, for publishing)

Memory mode uses in-process stores and does not persist state between restarts.

## Local Run
Example (module path assumes `PYTHONPATH=services`):
```
uvicorn lucius_orchestrator.api.app:create_app --factory --host 0.0.0.0 --port 8000
```

## Docker
Build and run:
```
docker build -t lucius/lucius_orchestrator:local -f services/lucius_orchestrator/Dockerfile .
docker run --rm -p 8000:8000 -e LUCIUS_STORAGE_BACKEND=memory lucius/lucius_orchestrator:local
```
