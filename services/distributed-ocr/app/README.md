# Platform Service Skeleton

## Purpose
This skeleton provides a consistent contract for consuming directives, sending ACK/RESULT callbacks, and enforcing idempotency.

## Required components
- directive_model.py
- consumer.py
- idempotency_store.py
- ack_client.py
- result_client.py
- workspace_io.py
- error_model.py
- health.py

## Execution flow
1) Consume directive
2) Deduplicate via (jobId, stepId, attempt_no, lease_id)
3) Send ACK callback
4) Execute step
5) Write artifacts
6) Send RESULT callback

## Step implementation
Replace the `execute()` function in `consumer.py`.
