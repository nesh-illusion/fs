# Phase 4: Platform Skeleton + Bus Conventions

## Bus conventions
- Topics: `global-bus-p0` ... `global-bus-p15`
- Partition: `crc32(tenant_id) % 16`
- Message body: directive JSON schema v1

## Platform skeleton
Location: service-specific skeletons in:
- `services/distributed-ocr/src/consumer/`
- `services/vector-ingestion/src/consumer/`
- `services/semantic-intelligence/src/consumer/`

Common pieces in each service:
- `models/directive_model.py`: directive parsing
- `consumer/__init__.py`: main handler + execute hook
- `idempotency/store.py`: idempotency gating
- `clients/ack_client.py`: ACK callback
- `clients/result_client.py`: RESULT callback
- `models/error_model.py`: error types

## Next steps
- Replace MemoryIdempotencyStore with a durable store (Table/Blob/Redis).
- Implement real Service Bus consumer.
- Implement workspace storage helpers against Blob Storage.
