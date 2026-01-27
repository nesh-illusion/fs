# Platform Service Skeleton

## Purpose
This skeleton provides a consistent contract for consuming directives, sending ACK/RESULT bus replies, and enforcing idempotency.

Environment:
- `SERVICEBUS_REPLY_PREFIX` (default `global-bus-replies-p`)

## Required components
- models/directive_model.py
- consumer/__init__.py
- idempotency/store.py
- clients/ack_client.py
- clients/result_client.py
- clients/workspace_io.py
- models/error_model.py
- health/__init__.py

## Execution flow
1) Consume directive
2) Deduplicate via (jobId, stepId, attempt_no, lease_id)
3) Send ACK callback
4) Execute step
5) Write artifacts
6) Send RESULT callback

## Step implementation
Replace the `execute()` function in `consumer.py`.
