# Lucius Orchestrator Ops Canvas

## Docker: Build + Run
```bash
docker compose build
docker compose up -d
docker compose logs -f lucius-orchestrator
```

Stop:
```bash
docker compose down
```

## API: Single-step request (OCR)
```bash
curl -s -X POST http://localhost:8000/v1/commands \
  -H "Content-Type: application/json" \
  -d '{
    "tenant_id": "t1",
    "request_type": "OCR",
    "input_ref": "in",
    "output_ref": "out",
    "payload": {},
    "schema_version": "v1",
    "callback_urls": {
      "ack": "http://localhost:8000/v1/callbacks/ack",
      "result": "http://localhost:8000/v1/callbacks/result"
    }
  }'
```

## API: Multi-step request (OCR->EMBEDDING)
```bash
curl -s -X POST http://localhost:8000/v1/commands \
  -H "Content-Type: application/json" \
  -d '{
    "tenant_id": "t1",
    "request_type": "OCR->EMBEDDING",
    "input_ref": "in",
    "output_ref": "out",
    "payload": {
      "steps": {
        "ocr": {"output_format": "TEXT"},
        "embedding": {"model": "text-embedding-3-large"}
      }
    },
    "schema_version": "v1",
    "callback_urls": {
      "ack": "http://localhost:8000/v1/callbacks/ack",
      "result": "http://localhost:8000/v1/callbacks/result"
    }
  }'
```

## API: Read job + steps (memory, table, or ledger)
```bash
curl -s "http://localhost:8000/v1/jobs/<jobId>?tenant_id=t1"
curl -s "http://localhost:8000/v1/jobs/<jobId>/steps"
curl -s "http://localhost:8000/v1/jobs/<jobId>/steps?step_id=<stepId>"
```
`GET /v1/jobs/<jobId>` returns job state plus a collated list of steps.

## Partition lane visibility
1) Logs:
```bash
docker compose logs -f lucius-orchestrator
```
Look for `routing.resolved` with `lane` and `routing_key_used`.

2) Steps API:
```bash
curl -s "http://localhost:8000/v1/jobs/<jobId>/steps"
```
Check `lane` on the step payload.

## Retry + next-step behavior
- Temporal workflow handles retries and sequencing.
- The workflow records `attempt_no`/`lease_id` before publish, waits for results, and advances to the next step on `SUCCEEDED`.

## Temporal: plug-and-play config
Required env:
- `LUCIUS_TEMPORAL_ADDRESS`
- `LUCIUS_TEMPORAL_NAMESPACE`
- `LUCIUS_TEMPORAL_TASK_QUEUE`

Optional env:
- `LUCIUS_TEMPORAL_ENABLED` (default true)
- `LUCIUS_TEMPORAL_WORKFLOW_EXECUTION_TIMEOUT` (seconds, default 3600)

Where to set:
1) Shell:
```bash
export LUCIUS_TEMPORAL_ADDRESS="localhost:7233"
export LUCIUS_TEMPORAL_NAMESPACE="default"
export LUCIUS_TEMPORAL_TASK_QUEUE="lucius"
```
2) docker-compose:
```yaml
services:
  lucius-orchestrator:
    environment:
      LUCIUS_TEMPORAL_ADDRESS: "temporal:7233"
      LUCIUS_TEMPORAL_NAMESPACE: "default"
      LUCIUS_TEMPORAL_TASK_QUEUE: "lucius"
  lucius-temporal-worker:
    environment:
      LUCIUS_TEMPORAL_ADDRESS: "temporal:7233"
      LUCIUS_TEMPORAL_NAMESPACE: "default"
      LUCIUS_TEMPORAL_TASK_QUEUE: "lucius"
```

## Service Bus: plug-and-play config
Required env:
- `LUCIUS_SERVICEBUS_CONNECTION` (Temporal worker)

Optional env:
- `LUCIUS_MAX_ATTEMPTS` (Temporal workflow, default 3)
- `LUCIUS_STEP_RESULT_TIMEOUT_SECONDS` (Temporal workflow, default 900)
- `LUCIUS_LEASE_MINUTES` (Temporal workflow, default 15)

Where to set:
1) Shell:
```bash
export LUCIUS_SERVICEBUS_CONNECTION="Endpoint=sb://...;SharedAccessKeyName=...;SharedAccessKey=..."
```
2) docker-compose:
```yaml
services:
  lucius-temporal-worker:
    environment:
      LUCIUS_SERVICEBUS_CONNECTION: "Endpoint=sb://...;SharedAccessKeyName=...;SharedAccessKey=..."
```
3) docker run:
```bash
docker run --rm \
  -e LUCIUS_SERVICEBUS_CONNECTION="Endpoint=sb://...;SharedAccessKeyName=...;SharedAccessKey=..." \
  -e LUCIUS_TEMPORAL_ADDRESS="temporal:7233" \
  -e LUCIUS_TEMPORAL_NAMESPACE="default" \
  -e LUCIUS_TEMPORAL_TASK_QUEUE="lucius" \
  lucius/lucius-orchestrator:local python -m temporal_worker.main
```

## Platform consumers: Service Bus config
Required env (each consumer service):
- `SERVICEBUS_CONNECTION`
- `SERVICEBUS_TOPIC`
- `SERVICEBUS_SUBSCRIPTION`

Notes:
- `SERVICEBUS_TOPIC` should match the lane topic(s) used by the orchestrator (`global-bus-p0` ... `global-bus-p15`).
- `SERVICEBUS_SUBSCRIPTION` is the consumer group name (per service).

Where to set:
1) Shell:
```bash
export SERVICEBUS_CONNECTION="Endpoint=sb://...;SharedAccessKeyName=...;SharedAccessKey=..."
export SERVICEBUS_TOPIC="global-bus-p0"
export SERVICEBUS_SUBSCRIPTION="distributed-ocr"
```
2) docker-compose:
```yaml
services:
  distributed-ocr:
    environment:
      SERVICEBUS_CONNECTION: "Endpoint=sb://...;SharedAccessKeyName=...;SharedAccessKey=..."
      SERVICEBUS_TOPIC: "global-bus-p0"
      SERVICEBUS_SUBSCRIPTION: "distributed-ocr"
```

## Azure Table Storage: plug-and-play config
Backend selection:
- `LUCIUS_STORAGE_BACKEND=memory` (default, non-persistent)
- `LUCIUS_STORAGE_BACKEND=table` (separate tables)
- `LUCIUS_STORAGE_BACKEND=ledger` (single-table ledger)

Required env:
- `LUCIUS_TABLE_CONNECTION` (required for table/ledger)

Optional env:
- `LUCIUS_MAX_INFLIGHT_PER_TENANT` (default 100)

Table names (table backend):
- `LUCIUS_JOBS_TABLE`, `LUCIUS_STEPS_TABLE`
- `LUCIUS_IDEMPOTENCY_TABLE`, `LUCIUS_JOB_INDEX_TABLE`, `LUCIUS_INFLIGHT_TABLE`

Table name (ledger backend):
- `LUCIUS_LEDGER_TABLE`

Where to set:
1) Shell:
```bash
export LUCIUS_STORAGE_BACKEND=table
export LUCIUS_TABLE_CONNECTION="DefaultEndpointsProtocol=...;AccountName=...;AccountKey=...;EndpointSuffix=core.windows.net"
```
2) docker-compose:
```yaml
services:
  lucius-orchestrator:
    environment:
      LUCIUS_STORAGE_BACKEND: "table"
      LUCIUS_TABLE_CONNECTION: "DefaultEndpointsProtocol=...;AccountName=...;AccountKey=...;EndpointSuffix=core.windows.net"
```

## Azure App Configuration: plug-and-play config
Status: not wired yet. To enable, we need to add an App Config loader and merge it into `AppSettings` before reading env.

Proposed env keys:
- `LUCIUS_APPCONFIG_CONNECTION`
- Optional: `LUCIUS_APPCONFIG_LABEL`, `LUCIUS_APPCONFIG_PREFIX`

Where to set (once wired):
1) Shell:
```bash
export LUCIUS_APPCONFIG_CONNECTION="Endpoint=https://...;Id=...;Secret=..."
```
2) docker-compose:
```yaml
services:
  lucius-orchestrator:
    environment:
      LUCIUS_APPCONFIG_CONNECTION: "Endpoint=https://...;Id=...;Secret=..."
```
