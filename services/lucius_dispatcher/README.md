# Lucius Dispatcher

Standalone deployable for the LUCIUS dispatcher + sweepers (historically named reconciliation). It publishes outbox directives and performs retry/cleanup sweeps.

## Components
- `app/runner.py`: CLI entrypoint for dispatch/sweep/loop.
- `app/service.py`: dispatcher/sweeper orchestration and config.
- `app/dispatcher.py`: loads outbox entries and publishes directives.
- `app/publisher.py`: Service Bus publisher or noop in memory mode.
- `app/sweepers.py`: ACK/lease/drift sweeps and retry transitions.
- `app/stores.py`: builds memory/table stores using orchestrator ledger interfaces.

## Flow
1) Load settings and build stores.
2) Dispatch reads PENDING outbox entries and publishes directives.
3) Update outbox/step state for dispatched items.
4) Sweepers handle retries for missing ACKs, expired leases, and drifted states.
5) Loop mode repeats dispatch on a partition at a fixed interval.

## Configuration
Environment variables:
- `LUCIUS_STORAGE_BACKEND=memory|table`
- `LUCIUS_TABLE_CONNECTION` (required if `table`)
- `LUCIUS_JOBS_TABLE`, `LUCIUS_STEPS_TABLE`, `LUCIUS_OUTBOX_TABLE`
- `LUCIUS_SERVICEBUS_CONNECTION` (optional, for publishing)

Memory mode uses in-process stores and a noop publisher.

## Commands
- `lucius-reconciliation dispatch --partition <tenant#YYYYMM>`
- `lucius-reconciliation sweep --job-id <id> --tenant <tenant> --bucket <tenant#YYYYMM>`
- `lucius-reconciliation loop --partition <tenant#YYYYMM> --interval 10`

## Local Run
Example (module path assumes `PYTHONPATH=services`):
```
lucius-reconciliation loop --partition t1#202401 --interval 5
```

## Tests
If pytest cannot find a writable temp directory, set `TMPDIR`:
```
TMPDIR=/var/folders/pv/bj3hzlgn76zdqhhgcnwbpk8m0000gn/T \
PYTHONDONTWRITEBYTECODE=1 \
PYTHONPATH=services/ocr:services:tests \
pytest -q -p no:cacheprovider tests/lucius_dispatcher/unit
```

## Docker
Build and run:
```
docker build -t lucius/lucius_dispatcher:local -f services/lucius_dispatcher/Dockerfile .
docker run --rm -e LUCIUS_STORAGE_BACKEND=memory lucius/lucius_dispatcher:local \
  lucius-reconciliation loop --partition t1#202401 --interval 5
```

TODO: Replace fixed shard ranges with HA lease-based partition ownership (Table Storage) to enable failover.
