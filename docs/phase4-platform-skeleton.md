# Phase 4: Platform Skeleton + Bus Conventions

## Bus conventions
- Topics: `global-bus-p0` ... `global-bus-p15`
- Partition: `crc32(tenant_id) % 16`
- Message body: directive JSON schema v1

## Platform skeleton
Location: `services/lucius-orchestrator/platform_skeleton/`
- `directive_model.py`: directive parsing
- `consumer.py`: main handler + execute hook
- `idempotency_store.py`: idempotency gating
- `ack_client.py`: ACK callback
- `result_client.py`: RESULT callback
- `workspace_io.py`: blob workspace helpers (placeholder)
- `error_model.py`: error types
- `health.py`: health check stub

## Next steps
- Replace MemoryIdempotencyStore with a durable store (Table/Blob/Redis).
- Implement real Service Bus consumer.
- Implement `workspace_io` against Blob Storage.
