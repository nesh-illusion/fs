# LUCIUS Orchestrator Roadmap

## Status at a glance
- Current phase: Phase 0 complete; Phase 1 next
- Last updated: 2026-01-16

## Phase 0 — Alignment + Decisions (done)
Exit criteria:
- Decisions locked (lease_id format, timeouts, failure_class rules)
- Contract schema v1 drafted
- Protocol registry format v1 drafted

Work completed:
- Decisions:
  - lease_id: UUIDv4
  - ACK timeout 30s; IN_PROGRESS lease 15m
  - dispatch retry backoff 30s/2m/10m; ACK retry backoff 1m/5m/15m
  - failure_class rules locked
- Contract schemas:
  - docs/contracts/command-envelope.v1.schema.json
  - docs/contracts/directive.v1.schema.json
  - docs/contracts/ack.v1.schema.json
  - docs/contracts/result.v1.schema.json
- Protocol registry:
  - docs/protocols/protocol-registry.v1.schema.json
  - docs/protocols/protocol-registry.v1.example.json
- Step payload schemas:
  - docs/schemas/steps/ocr.v1.json
  - docs/schemas/steps/embedding.v1.json
  - docs/schemas/steps/sis.v1.json

## Phase 1 — Ledger + State Machine (done)
Exit criteria:
- Data model defined for Jobs, Steps, Outbox, Events
- State transition rules documented with ETag constraints
- Idempotency strategy for job creation documented

Work completed:
- docs/ledger-state-model.md
- docs/table-storage-schema.md

## Phase 2 — API + Orchestrator (done)
Exit criteria:
- FastAPI endpoints implemented
- Validator enforces envelope + payload schemas
- Orchestrator creates jobs/steps + outbox

Work completed:
- services/lucius-orchestrator/src/api/app.py
- services/lucius-orchestrator/src/ledger/memory_store.py
- services/lucius-orchestrator/src/ledger/table_storage.py
- services/lucius-orchestrator/src/config/protocols.py
- services/lucius-orchestrator/src/config/settings.py
- docs/phase2-implementation.md
- services/lucius-orchestrator/tests/validation/test_job_index.py
- services/lucius-orchestrator/tests/api/test_job_lookup.py
- services/lucius-orchestrator/tests/api/test_command_errors.py
  - In progress: services/lucius-orchestrator/tests/validation/test_idempotency.py, services/lucius-orchestrator/tests/validation/test_schema_validator.py

## Phase 3 — Reconciliation Service (done)
Exit criteria:
- Dispatcher publishes outbox to Service Bus
- Sweeper loops implemented with retry logic

Planned work:
- Dispatcher loop reads Outbox PENDING entries and marks SENT
- ACK sweeper moves AWAITING_ACK -> FAILED_RETRY
- Lease sweeper moves IN_PROGRESS -> FAILED_RETRY or FAILED_FINAL
- Drift sweeper fixes stuck DISPATCHING/OUTBOX

## Phase 4 — Bus + Platform Skeleton (done)
Exit criteria:
- Service Bus topic/partition conventions defined
- Platform skeleton with ACK/RESULT callbacks ready

Planned work:
- Define bus conventions (topics/partitions)
- Provide platform skeleton template
  - In progress: docs/phase4-platform-skeleton.md
  - In progress: services/distributed-ocr/src/*, services/vector-ingestion/src/*, services/semantic-intelligence/src/*
  - In progress: services/distributed-ocr/pyproject.toml, services/vector-ingestion/pyproject.toml, services/semantic-intelligence/pyproject.toml
  - In progress: services/distributed-ocr/src/consumer/servicebus_consumer.py, services/vector-ingestion/src/consumer/servicebus_consumer.py, services/semantic-intelligence/src/consumer/servicebus_consumer.py
  - In progress: services/distributed-ocr/src/idempotency/store.py, services/vector-ingestion/src/idempotency/store.py, services/semantic-intelligence/src/idempotency/store.py
  - In progress: retry/backoff + DLQ handling in platform Service Bus consumers
  - In progress: services/distributed-ocr/tests/unit/test_idempotency_store.py, services/distributed-ocr/tests/unit/test_consumer.py
  - In progress: ACK/RESULT timestamp + service bus message parsing fixes

Work completed:
- docs/phase4-platform-skeleton.md
- services/distributed-ocr/*
- services/vector-ingestion/*
- services/semantic-intelligence/*

## Phase 5 — Observability + Guardrails
Exit criteria:
- Metrics, logs, traces defined and emitted
- Backpressure gates defined and enforced

## Phase 6 — Hardening + Rollout
Exit criteria:
- Integration tests cover cancellation, retry, drift repair
- Runbook and canary plan complete

## Change log
- 2026-01-16: Created Phase 0 artifacts (schemas, protocol registry, step payload schemas)
