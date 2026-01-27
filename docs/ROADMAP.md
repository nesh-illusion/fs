# LUCIUS Orchestrator Roadmap

## Status at a glance
- Current phase: Phase 5 next (Observability + Guardrails)
- Last updated: 2026-01-26

## Phase 0 — Alignment + Decisions (done)
Exit criteria:
- Decisions locked (lease_id format, failure_class rules)
- Contract schema v1 drafted
- Protocol registry format v1 drafted

Work completed:
- Decisions:
  - lease_id: UUIDv4
  - failure_class rules locked
- Contract schemas:
  - services/lucius-orchestrator/src/contracts/command-envelope.v1.schema.json
  - services/lucius-orchestrator/src/contracts/directive.v1.schema.json
- Protocol registry:
  - services/lucius-orchestrator/src/protocols/protocol-registry.v1.schema.json
  - services/lucius-orchestrator/src/protocols/protocol-registry.v1.example.json
- Step payload schemas:
  - services/lucius-orchestrator/src/schemas/steps/ocr.v1.json
  - services/lucius-orchestrator/src/schemas/steps/embedding.v1.json
  - services/lucius-orchestrator/src/schemas/steps/sis.v1.json

## Phase 1 — Ledger + State Machine (done)
Exit criteria:
- Data model defined for Jobs and Steps
- State transition rules documented with ETag constraints
- Idempotency strategy for job creation documented

Work completed:
- docs/ledger-state-model.md
- docs/table-storage-schema.md

## Phase 2 — API + Orchestrator (done)
Exit criteria:
- FastAPI endpoints implemented
- Validator enforces envelope + payload schemas
- Orchestrator creates jobs/steps + starts Temporal workflow

Work completed:
- services/lucius-orchestrator/src/api/app.py
- services/lucius-orchestrator/src/ledger/memory_store.py
- services/lucius-orchestrator/src/ledger/table_storage.py
- services/lucius-orchestrator/src/config/protocols.py
- services/lucius-orchestrator/src/config/settings.py
- docs/LUCIUS_TEMPORAL_WORKFLOW_SPEC.md
- services/lucius-orchestrator/src/temporal_worker/main.py
- services/lucius-orchestrator/tests/validation/test_job_index.py
- services/lucius-orchestrator/tests/api/test_job_lookup.py
- services/lucius-orchestrator/tests/api/test_command_errors.py
- services/lucius-orchestrator/tests/validation/test_idempotency.py
- services/lucius-orchestrator/tests/validation/test_schema_validator.py

## Phase 3 — Temporal Orchestration (done)
Exit criteria:
- Temporal activities publish to Service Bus
- Workflow handles retries, timeouts, and sequencing

Work completed:
- services/lucius-orchestrator/src/temporal_worker/main.py
- docker-compose.yml (Temporal server + worker)

## Phase 4 — Bus + Platform Skeleton (done)
Exit criteria:
- Service Bus topic/partition conventions defined
- Platform skeleton with ACK/RESULT callbacks ready

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
- 2026-01-26: Migrated orchestration to Temporal; removed outbox retry flow and consolidated specs
