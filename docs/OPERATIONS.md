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

## API: Read job + steps (in-memory or table)
```bash
curl -s "http://localhost:8000/v1/jobs/<jobId>?tenant_id=t1"
curl -s "http://localhost:8000/v1/jobs/<jobId>/steps"
```

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
- Outbox retry increments `attempt_no` and refreshes `lease_id` before republishing.
- On a `SUCCEEDED` result for a non-final step, the orchestrator creates/publishes the next step.

## Service Bus: plug-and-play config
Required env:
- `LUCIUS_SERVICEBUS_CONNECTION`

Optional env:
- `LUCIUS_OUTBOX_PUBLISH_TIMEOUT` (seconds, default 2.0)
- `LUCIUS_OUTBOX_RETRY_DELAY` (seconds, default 30.0)
- `LUCIUS_OUTBOX_MAX_ATTEMPTS` (default 3)

Where to set:
1) Shell:
```bash
export LUCIUS_SERVICEBUS_CONNECTION="Endpoint=sb://...;SharedAccessKeyName=...;SharedAccessKey=..."
```
2) docker-compose:
```yaml
services:
  lucius-orchestrator:
    environment:
      LUCIUS_SERVICEBUS_CONNECTION: "Endpoint=sb://...;SharedAccessKeyName=...;SharedAccessKey=..."
```
3) docker run:
```bash
docker run --rm -e LUCIUS_SERVICEBUS_CONNECTION="Endpoint=sb://...;SharedAccessKeyName=...;SharedAccessKey=..." lucius/lucius-orchestrator:local
```

## Azure Table Storage: plug-and-play config
Backend selection:
- `LUCIUS_STORAGE_BACKEND=table` (separate tables)
- `LUCIUS_STORAGE_BACKEND=ledger` (single-table ledger)

Required env:
- `LUCIUS_TABLE_CONNECTION`

Table names (table backend):
- `LUCIUS_JOBS_TABLE`, `LUCIUS_STEPS_TABLE`, `LUCIUS_OUTBOX_TABLE`
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
